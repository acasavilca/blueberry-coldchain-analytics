#!/usr/bin/env python
# coding: utf-8

# In[1]:


import json
from pathlib import Path
from datetime import datetime, timedelta, timezone

import numpy as np
import os

import pickle

import pandas as pd
from google.cloud import storage

from numba import njit, types
from numba.typed import Dict

# DEL_20260609_simulator
from simulator import (
    retrieve_satellite_data,
    prepare_forcing_arrays,
    get_tr,
    run_simulation_chunk,
    build_batch_event_tables,
    build_telemetry_table,
    expand_minute_timestamps_to_internal,
    encode_coordinates,
    format_dates_for_data_retrieval,
)

# from DEL_20260609_config import (
from config import (
    LOCATION,
    RETRIEVAL_CONFIG,
    FORCING_CONFIG,
    SCHEDULER_CONFIG,
    SIMULATION_CONFIG,
    TELEMETRY_CONFIG,
    WEATHER_DATASET_DTYPES,
    FRUITS_CONFIG,
    LOGISTICS_CONFIG,
)

from utils import (
    NumpyEncoder,
    create_typed_dict,
    plain_dict_to_typed_dict,
    typed_dict_to_plain_dict,
    clean_rng_state,
    process_kestra_payload,
)

from respiration_data import RESPIRATION_DB

from simulation_state_defaults import INITIALIZATION_STATE, STATE_SCHEMA


# In[2]:


# Constants (module_level, never change)
REINITIALIZE = False
IS_TEST = False
FRUIT_TYPE = "blueberry"
START_DT = 2024_01_01_17_00_00
END_DT   = 2025_01_01_20_00_00
BASE_SEED = 42
CHUNK_SIZE_HOURS = 24
INPUT_TIME_GRAIN = "hourly"
KESTRA_PAYLOAD_FILE = "del_LAST_STATE.json"
WEATHER_DATA_FILE = "del_WEATHER_DATA.parquet"

# Derived constants
ACTIVE_FRUIT_CONFIG = FRUITS_CONFIG[FRUIT_TYPE]
SEED = int(BASE_SEED + ACTIVE_FRUIT_CONFIG["seed_offset"])
DT_INTERNAL = SIMULATION_CONFIG["dt_internal"]
LATITUDE = LOCATION["latitude"]
LONGITUDE = LOCATION["longitude"]
PLANT_ID = encode_coordinates(
    lat=LATITUDE,
    lon=LONGITUDE,
)
M_DOT_AIR_THEORETICAL = ACTIVE_FRUIT_CONFIG["m_dot_air_theoretical"]

# State loading
if os.path.exists(KESTRA_PAYLOAD_FILE) and not REINITIALIZE:
    with open(KESTRA_PAYLOAD_FILE, "r") as f:
        kestra_payload = json.load(f)
    raw_state, rng = process_kestra_payload(kestra_payload_dict=kestra_payload)
    simulation_state = {k: v for k, v in raw_state.items() if k != "last_epoch_timestamp"}
    last_epoch_timestamp = raw_state["last_epoch_timestamp"]
else:
    rng = np.random.default_rng(seed=SEED)
    raw_state = INITIALIZATION_STATE
    simulation_state = {k: v for k, v in raw_state.items() if k != "last_epoch_timestamp"}

is_first_run = bool(simulation_state["is_first_run"])

# Epoch window
if is_first_run:
    start_hour_epoch, end_hour_epoch = format_dates_for_data_retrieval(
        start_dt_str=str(START_DT),
        end_dt_str=str(END_DT),
        input_time_grain=INPUT_TIME_GRAIN,
        inclusive_end=True, # Leave in True (default) to do a successful interpolation later in the script
    )
else:
    start_hour_epoch = last_epoch_timestamp + DT_INTERNAL
    end_hour_epoch = start_hour_epoch + CHUNK_SIZE_HOURS*3600

# Respiration tables lookup
T_lookup, R_lookup = get_tr(respiration_database=RESPIRATION_DB, fruit=FRUIT_TYPE)

# Typed dicts
(
    empty_yearly_weight_dict_typed,

    telemetry_array_dict_float64,
    telemetry_array_dict_int8,

    last_state_scalar_dict_float64, 
    last_state_array_dict_float64,
    last_state_scalar_dict_int64,
    last_state_array_dict_int64,
    last_state_scalar_dict_int8,
    last_state_array_dict_int8,
) = [
    create_typed_dict(
        key_type=key_type,
        value_type=value_type,
        value_structure=value_structure,
    ) for key_type, value_type, value_structure in (
        (types.int64,        types.float64, 'scalar'),

        (types.unicode_type, types.float64, 'array'),
        (types.unicode_type, types.int8,    'array'),

        (types.unicode_type, types.float64, 'scalar'),
        (types.unicode_type, types.float64, 'array'),
        (types.unicode_type, types.int64,   'scalar'),
        (types.unicode_type, types.int64,   'array'),
        (types.unicode_type, types.int8,    'scalar'),
        (types.unicode_type, types.int8,    'array'),
    )
]


# In[3]:


# Weather data
if not IS_TEST:
    df = retrieve_satellite_data(
        start_hour_epoch=start_hour_epoch,
        end_hour_epoch=end_hour_epoch,
        latitude=LATITUDE,
        longitude=LONGITUDE,
        dtypes=WEATHER_DATASET_DTYPES,
        frequency="hourly",
        is_backfill=True,
        rng=rng,
        **RETRIEVAL_CONFIG,
    )
    df.to_parquet(WEATHER_DATA_FILE)
else:
    df = pd.read_parquet(WEATHER_DATA_FILE)


# In[5]:


# Forcing arrays
(
    forcing,
    localtime_reference,
    df_resampled,
    timestep,
    days_since_comp_installation_arr,
) = prepare_forcing_arrays(df, **FORCING_CONFIG)


# In[6]:


# Simulation execution context
sim_ctx = {}

# 1. Merge all your other configuration and state dictionaries
sim_ctx.update(SIMULATION_CONFIG)
sim_ctx.update(simulation_state)
sim_ctx.update(SCHEDULER_CONFIG)
sim_ctx.update(LOGISTICS_CONFIG)

# 2. Add all the arrays from the forcing and localtime data
sim_ctx["T_ambient_arr"] = forcing["T_ambient"]
sim_ctx["RH_ambient_arr"] = forcing["RH_ambient"]
sim_ctx["P_arr"] = forcing["P"]
sim_ctx["GHI_arr"] = forcing["GHI"]
sim_ctx["WS_arr"] = forcing["WS"]
sim_ctx["T2MDEW_arr"] = forcing["T2MDEW"]
sim_ctx["T_ground_arr"] = forcing["TSOIL_54CM"]

sim_ctx["hour_local_arr"] = localtime_reference["hour"]
sim_ctx["minute_local_arr"] = localtime_reference["minute"]
sim_ctx["month_local_arr"] = localtime_reference["month"]
sim_ctx["year_local_arr"] = localtime_reference["year"]

# 3. Add individual execution scalars and generators
sim_ctx["start_epoch_timestamp"] = start_hour_epoch
sim_ctx["days_since_comp_installation_arr"] = days_since_comp_installation_arr
sim_ctx["timestep"] = timestep
sim_ctx["rng"] = rng

# 4. Add runtime lookups
sim_ctx["T_lookup"] = T_lookup
sim_ctx["R_lookup"] = R_lookup

# 5. Add the empty types dicts to catch output data
sim_ctx["telemetry_array_dict_float64"] = telemetry_array_dict_float64
sim_ctx["telemetry_array_dict_int8"] = telemetry_array_dict_int8
sim_ctx["last_state_scalar_dict_float64"] = last_state_scalar_dict_float64
sim_ctx["last_state_array_dict_float64"] = last_state_array_dict_float64
sim_ctx["last_state_array_dict_int64"] = last_state_array_dict_int64
sim_ctx["last_state_scalar_dict_int64"] = last_state_scalar_dict_int64
sim_ctx["last_state_scalar_dict_int8"] = last_state_scalar_dict_int8
sim_ctx["last_state_array_dict_int8"] = last_state_array_dict_int8

# 5. Add fruit-specific parameters
sim_ctx["tunnel_exit_fruit_temp_ref"] = ACTIVE_FRUIT_CONFIG["tunnel_exit_fruit_temp_ref"]
sim_ctx["Cp_fruit"] = ACTIVE_FRUIT_CONFIG["Cp_fruit"]
sim_ctx["k_p"] = ACTIVE_FRUIT_CONFIG["k_p"]
sim_ctx["setpoint"] = ACTIVE_FRUIT_CONFIG["setpoint"]
sim_ctx["target_rh"] = ACTIVE_FRUIT_CONFIG["target_rh"]
sim_ctx["m_max"] = ACTIVE_FRUIT_CONFIG["m_max"]
sim_ctx["Q_rated"] = ACTIVE_FRUIT_CONFIG["Q_rated"]
sim_ctx["TD_design"] = ACTIVE_FRUIT_CONFIG["TD_design"]
sim_ctx["k_zone_ref"] = ACTIVE_FRUIT_CONFIG["k_zone_ref"]
sim_ctx["base_quality_arr"] = np.array(ACTIVE_FRUIT_CONFIG["base_quality"], dtype=np.float64)
sim_ctx["monthly_weight_arr"] = np.array(ACTIVE_FRUIT_CONFIG["monthly_weight"], dtype=np.float64)
sim_ctx["yearly_weight_dict"] = plain_dict_to_typed_dict(
    plain_dict=ACTIVE_FRUIT_CONFIG["yearly_weight"],
    empty_typed_dict=empty_yearly_weight_dict_typed,
)


# In[8]:


# Run simulator
(
    telemetry_array_dict_float64,
    telemetry_array_dict_int8,
    last_state_scalar_dict_float64, 
    last_state_array_dict_float64, 
    last_state_array_dict_int64,
    last_state_scalar_dict_int64,
    last_state_scalar_dict_int8,
    last_state_array_dict_int8,
    arrivals_arr,
    dispatches_arr,
) = run_simulation_chunk(**sim_ctx)

# Convert typed dicts to plain Python dicts
(
    telemetry_float64,
    telemetry_int8,
    last_scalar_float64,
    last_array_float64,
    last_array_int64,
    last_scalar_int64,
    last_scalar_int8,
    last_array_int8,
) = [
    typed_dict_to_plain_dict(typed_dict) for typed_dict in (
        telemetry_array_dict_float64,
        telemetry_array_dict_int8,
        last_state_scalar_dict_float64, 
        last_state_array_dict_float64, 
        last_state_array_dict_int64,
        last_state_scalar_dict_int64,
        last_state_scalar_dict_int8,
        last_state_array_dict_int8,
    )
]

outputs_dict = telemetry_float64
calls_dict = telemetry_int8


# In[89]:


# Build telemetry table
telemetry_df = build_telemetry_table(
    plant_id=PLANT_ID,
    fruit_type=FRUIT_TYPE,
    outputs_dict=outputs_dict,
    calls_dict=calls_dict,
    m_dot_air_theoretical=M_DOT_AIR_THEORETICAL,
    rng=rng,
    **TELEMETRY_CONFIG,
)


# In[91]:


# Build events / batches
batches_df, events_df = build_batch_event_tables(
    plant_id=PLANT_ID,
    fruit_type=FRUIT_TYPE,
    arrivals_arr=arrivals_arr,
    dispatches_arr=dispatches_arr,
)


# In[94]:


# Persist state
last_state = {
    **last_scalar_float64,
    **last_array_float64,
    **last_array_int64,
    **last_scalar_int64,
    **last_scalar_int8,
    **last_array_int8,
}

type_metadata = {}
for parent_key, sub_dict in STATE_SCHEMA.items():
    for inner_key, inner_val in sub_dict.items():
        if inner_key in last_state:
            type_metadata[inner_key] = sub_dict[inner_key]

serializable_rng_state = clean_rng_state(rng.bit_generator.state)

kestra_payload = {
    "rng_state": serializable_rng_state,
    "data_states": last_state,   # Your actual variables
    "_types": type_metadata,      # The explicit type blueprint
}

with open(KESTRA_PAYLOAD_FILE, "w") as f:
    json.dump(kestra_payload, f, cls=NumpyEncoder)

