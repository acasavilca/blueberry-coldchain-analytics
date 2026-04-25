import json
from pathlib import Path
from datetime import datetime, timedelta, timezone

import pandas as pd
from google.cloud import storage

from simulator import (
    retrieve_satellite_data,
    prepare_forcing_arrays,
    get_tr,
    create_numba_dicts,
    build_door_and_mass_schedules,
    run_simulation_chunk,
    build_batch_event_tables,
    build_telemetry_table,
    expand_minute_timestamps_to_internal,
)

from config import (
    LOCATION,
    RETRIEVAL_CONFIG,
    FORCING_CONFIG,
    SCHEDULER_CONFIG,
    SIMULATION_CONFIG,
    TELEMETRY_CONFIG,
    RUNTIME_CONFIG,
    WEATHER_DATASET_DTYPES,
)

from respiration_data import RESPIRATION_DB

# ----------------------------------
# CONFIG
# ----------------------------------
STATE_PATH = Path(RUNTIME_CONFIG["state_path"])
LOCAL_OUT = Path(RUNTIME_CONFIG["local_out_dir"])
# BUCKET_NAME = RUNTIME_CONFIG["bucket_name"]
CHUNK_HOURS = RUNTIME_CONFIG["chunk_hours"]
BACKFILL_MODE = RUNTIME_CONFIG["backfill_mode"]

FRUIT_TYPE = TELEMETRY_CONFIG["fruit_type"]
# SEED = SIMULATION_CONFIG["seed"]

def load_state():
    if STATE_PATH.exists():
        with open(STATE_PATH, "r") as f:
            state = json.load(f)
    else:
        state = {
            "next_start": "2023-01-01T00:00:00",
            "next_batch_id": 1,
            "active_batches": [],
        }
    return state

# def load_state_kestra():
#     # Attempt to get 'simulation_state' from Kestra's KV store
#     try:
#         state = Kestra.get_kv("simulation_state")
#     except:
#         # Default state if it's the first time running
#         state = {
#             "next_start": "2023-01-01T00:00:00",
#             "next_batch_id": 1,
#             "active_batches": [],
#         }
#     return state

def save_state(state):
    with open(STATE_PATH, "w") as f:
        json.dump(state, f, indent=2)


def deserialize_active_batches(active_batches_json):
    batches = []
    for b in active_batches_json:
        b2 = b.copy()
        for key in ["arrival_ts", "first_dispatch_ts", "final_dispatch_ts"]:
            val = b2.get(key)
            if val is None:
                b2[key] = pd.NaT
            elif val == "NaT":
                b2[key] = pd.NaT
            else:
                b2[key] = pd.Timestamp(val)
        batches.append(b2)
    return batches


def serialize_active_batches(active_batches):
    out = []
    for b in active_batches:
        b2 = b.copy()
        for key in ["arrival_ts", "first_dispatch_ts", "final_dispatch_ts"]:
            val = b2.get(key)
            if pd.isna(val):
                b2[key] = "NaT"
            else:
                b2[key] = pd.Timestamp(val).isoformat()
        out.append(b2)
    return out


def upload_file(local_path: Path, bucket_name: str, object_name: str, project_id):
    client = storage.Client(project=project_id) # (project=RUNTIME_CONFIG["project_id"])
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(str(local_path))


def main():
    state = load_state()

    start_dt = pd.Timestamp(state["next_start"])
    end_dt = start_dt + pd.Timedelta(hours=CHUNK_HOURS)

    start_str = start_dt.strftime("%Y%m%d")
    end_str = start_dt.strftime("%Y%m%d")

    active_batches = deserialize_active_batches(state["active_batches"])
    next_batch_id = state["next_batch_id"]

    # ----------------------------------
    # 1. Retrieve forcing
    # ----------------------------------
    df = retrieve_satellite_data(
        start_dt=start_str,
        end_dt=end_str,
        latitude=LOCATION["latitude"],
        longitude=LOCATION["longitude"],
        dtypes=WEATHER_DATASET_DTYPES,
        **RETRIEVAL_CONFIG,
    )

    forcing, df_resampled, timestep = prepare_forcing_arrays(
        df,
        **FORCING_CONFIG,
    )

    # ----------------------------------
    # 2. Build schedules
    # ----------------------------------
    door_ext_schedule, door_int_schedule, fruit_mass_delta_kg, event_code, incoming_temperature_arr = build_door_and_mass_schedules(
        hour_arr=forcing["hour"],
        minute_arr=forcing["minute"],
        month_arr=forcing["month"],
        year_arr=forcing["year"],
        tunnel_exit_fruit_temp=SIMULATION_CONFIG["tunnel_exit_fruit_temp"],
        **SCHEDULER_CONFIG,
    )

    # ----------------------------------
    # 3. Run simulator
    # ----------------------------------
    T_lookup, R_lookup = get_tr(respiration_database=RESPIRATION_DB, fruit=FRUIT_TYPE)

    float64_dict, int8_dict = create_numba_dicts()

    sim_params = SIMULATION_CONFIG.copy()
    sim_params["T_lookup"] = T_lookup
    sim_params["R_lookup"] = R_lookup
    sim_params["float64_dict"] = float64_dict
    sim_params["int8_dict"] = int8_dict

    outputs_dict, calls_dict = run_simulation_chunk(
        T_ambient_arr=forcing["T_ambient"],
        RH_ambient_arr=forcing["RH_ambient"],
        P_arr=forcing["P"],
        GHI_arr=forcing["GHI"],
        WS_arr=forcing["WS"],
        T2MDEW_arr=forcing["T2MDEW"],
        T_ground_arr=forcing["TSOIL_28_100CM"],

        hour_arr=forcing["hour"],
        minute_arr=forcing["minute"],
        second_arr=forcing["second"],
        doy_arr=forcing["day_of_year"],

        door_ext_schedule=door_ext_schedule,
        door_int_schedule=door_int_schedule,
        fruit_mass_delta_kg=fruit_mass_delta_kg,
        incoming_temperature_arr=incoming_temperature_arr,

        timestep=timestep,

        **sim_params,
    )

    # convert typed dicts to normal dicts for pandas safety
    outputs_dict_py = {k: outputs_dict[k] for k in outputs_dict}
    calls_dict_py = {k: calls_dict[k] for k in calls_dict}

    # ----------------------------------
    # 4. Build telemetry
    # ----------------------------------
    telemetry_dt_arr = expand_minute_timestamps_to_internal(
        forcing["dt_arr"],
        int(SIMULATION_CONFIG["dt_internal"])
    )

    telemetry_df = build_telemetry_table(
        telemetry_dt_arr=telemetry_dt_arr,
        outputs_dict=outputs_dict_py,
        calls_dict=calls_dict_py,
        **TELEMETRY_CONFIG,
        # seed=SEED,
    )

    # ----------------------------------
    # 5. Build events / batches
    # ----------------------------------
    events_df, batches_df, active_batches, next_batch_id = build_batch_event_tables(
        dt_arr=forcing["dt_arr"],
        fruit_mass_delta_kg=fruit_mass_delta_kg,
        incoming_temperature_arr=incoming_temperature_arr,
        fruit_type=FRUIT_TYPE,
        active_batches=active_batches,
        next_batch_id=next_batch_id,
        # seed=SEED,
    )

    # ----------------------------------
    # 6. Save parquet locally
    # ----------------------------------
    day_str = start_dt.strftime("%Y-%m-%d")
    hour_str = start_dt.strftime("%H")

    telemetry_dir = LOCAL_OUT / "raw" / "telemetry" / f"date={day_str}"
    events_dir = LOCAL_OUT / "raw" / "events" / f"date={day_str}"
    batches_dir = LOCAL_OUT / "raw" / "batches" / f"date={day_str}"

    telemetry_dir.mkdir(parents=True, exist_ok=True)
    events_dir.mkdir(parents=True, exist_ok=True)
    batches_dir.mkdir(parents=True, exist_ok=True)

    telemetry_path = telemetry_dir / f"telemetry_{hour_str}.parquet"
    events_path = events_dir / f"events_{hour_str}.parquet"
    batches_path = batches_dir / f"batches_{hour_str}.parquet"

    telemetry_df.to_parquet(telemetry_path, index=False)
    events_df.to_parquet(events_path, index=False)
    batches_df.to_parquet(batches_path, index=False)

    # ----------------------------------
    # 7. Upload to GCS
    # ----------------------------------
    upload_file(
        telemetry_path,
        BUCKET_NAME,
        f"raw/telemetry/date={day_str}/{telemetry_path.name}",
    )
    upload_file(
        events_path,
        BUCKET_NAME,
        f"raw/events/date={day_str}/{events_path.name}",
    )
    upload_file(
        batches_path,
        BUCKET_NAME,
        f"raw/batches/date={day_str}/{batches_path.name}",
    )

    # ----------------------------------
    # 8. Update state
    # ----------------------------------
    state["next_start"] = end_dt.isoformat()
    state["next_batch_id"] = next_batch_id
    state["active_batches"] = serialize_active_batches(active_batches)

    save_state(state)

    print(f"Completed chunk {start_dt} -> {end_dt}")


if __name__ == "__main__":
    if BACKFILL_MODE:
        num_days = RUNTIME_CONFIG["backfill_days"]
        for _ in range(num_days):
            main()
    else:
        main()
