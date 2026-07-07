import numpy as np
from pytimeparse.timeparse import timeparse

# SEED = 42
# FRUIT_TYPE = "blueberry"
LATITUDE = -8.5771
LONGITUDE = -78.5661
RESAMPLE_RATE = "1min"
COMPRESSOR_INSTALLATION_DATE = '2021-01-01'
DT_INTERNAL = 5.0 # 10.0

LOCATION = {
    "latitude": LATITUDE,
    "longitude": LONGITUDE,
}

RETRIEVAL_CONFIG = {
    "measurements": "temperature_2m,relative_humidity_2m,wind_speed_10m,shortwave_radiation,dewpoint_2m,surface_pressure,soil_temperature_54cm",
    "timezone_param": None, # "auto",
}

FORCING_CONFIG = {
    "resample_rate": RESAMPLE_RATE,
    "compressor_installation_date": COMPRESSOR_INSTALLATION_DATE,
}

SCHEDULER_CONFIG = {
    "forcing_dt_sec": float(timeparse(RESAMPLE_RATE)),
    "min_ship_mass": 5000.0,
    "min_arrived_mass": 100.0,
    "min_dispatch_weight": 0.5,
    "max_inventory_kg": 100_000.0,
    "arrival_scale": 1100.0, # 275.0,
    "shipment_scale": 20_000.0, # 9500.0,
    "ext_duration_mean_min": 1.5,
    "int_duration_mean_min": 0.5,
    "duration_shape": 2.0,
    "max_active_batches": 500,
    "max_arrivals_per_run": 50_000,# 300,
    "max_dispatches_per_run": 100_000, # 3000,
    "max_batches_per_dispatch": 20,
    "num_fields_arrival": 5, # batch_id, timestamp, quality_grade, mass_kg, tunnel_exit_temp_c
    "num_fields_dispatch": 6, # batch_id, timestamp, quality_grade, mass_at_dispatch, mass_removed, mass_remaining
}

LOGISTICS_CONFIG = {
    "lambda_ext_bg_per_hour_arr": np.array([
        0.00, 0.00, 0.00, 0.00, 0.00, 0.02,
        0.05, 0.10, 0.20, 0.25, 0.20, 0.15,
        0.10, 0.10, 0.15, 0.20, 0.25, 0.20,
        0.10, 0.05, 0.02, 0.00, 0.00, 0.00
    ], dtype=float),
    "lambda_int_bg_per_hour_arr": np.array([
        0.02, 0.02, 0.02, 0.02, 0.02, 0.05,
        0.20, 0.50, 1.00, 1.20, 1.30, 1.20,
        1.00, 0.80, 1.00, 1.20, 1.30, 1.10,
        0.60, 0.20, 0.08, 0.05, 0.02, 0.02
    ], dtype=float),
    "lambda_arrival_per_hour_arr": np.array([
        0.00, 0.00, 0.00, 0.00, 0.00, 0.05,
        0.20, 0.60, 1.20, 1.50, 1.20, 0.80,
        0.60, 0.50, 0.80, 1.00, 1.20, 1.00,
        0.50, 0.20, 0.05, 0.00, 0.00, 0.00
    ], dtype=float),
    "lambda_shipment_per_hour_arr": np.array([
        0.00, 0.00, 0.00, 0.00, 0.00, 0.00,
        0.00, 0.00, 0.05, 0.10, 0.15, 0.20,
        0.40, 0.50, 0.80, 1.00, 1.20, 1.00,
        0.50, 0.20, 0.05, 0.00, 0.00, 0.00
    ], dtype=float),
}

SIMULATION_CONFIG = {
    # geometry / envelope primitives
    "L": 20.0,
    "W": 15.0,
    "H": 6.0,
    "alpha_roof": 0.6,
    "k_fans": .02, # .10
    "U_wall": 0.225,
    "U_floor": 0.75,
    # "T_ground": 19.0,
    # "Q_rated": 22_000.0,
    "f_structure": 0.05,
    # "k_wind_U": 0.02,
    "Cp_air": 1006.0,

    "degradation_factor": .92,

    "compressor_degradation_rate": 5e-6,

    # control
    "setpoint": 0.0,
    "deadband": .5, # 0.5,
    "tau_cool": 120.0, # 120.0, # 120.0, # 60.0,
    "tau_sensor": 90.0, # 90.0, # 90.0, # 60.0,

    "cooling_frac_floor": 0.05,

    # timestep
    "dt_internal": DT_INTERNAL,

    # fruit / product
    "h_respiration": 10.61,
    # "tunnel_exit_fruit_temp": -1.0,
    # "Cp_fruit": 3640.0,
    # chosen zone only; do not keep the whole k_by_zone dict
    # "k_zone": float(np.log(8) / (24.0 * 3600.0)),   # cold_storage

    # air exchange / free volume
    "n_ach_eff_per_sec": 2.0 / 86400, # 2.0 / 86400,        # per day

    # gas constants
    "R_const": 8.314,
    "M_CO2": 44.0,
    "M_air": 28.97,

    # coil / humidity control
    # "T_coil_ref": -2.0,
    "BF": 0.30, # 0.5, # 0.2,
    "tau_condense": 30.0, # 120.0, # 15.0,   # replace if you settled on another final value
    # "target_rh": .965, # 0.95,
    "rh_deadband": 0.02, # 0.02
    "tau_humid_frac": 180.0, # 180.0, # 180.0, # 60.0,
    "tau_humid_sensor": 300.0, # 300.0, # 10.0, # 10.0,
    # "m_max": 0.0008, # 0.003,
    "f_evap_humid": 0.9, # 0.95
    
    # transpiration
    # "k_p": 2.5e-10,

    "CO2_outdoor_ppm": 420.0,
    "O2_outdoor_pct": 20.95,

    "f_min": .05,
    "V_free_min": .05,
    "m_air_room_min": .05,
    "rho_load_bulk": 250.0,
    
    # Plant parameters
    "T_plant_a": 10.0, # Celsius
    "T_plant_b": 0.2,
    "dT_plant": 60.0, # sec
    "tau_plant": 7200.0, # hrs
    "RH_plant": 0.65,

    # Compressor parameters
    "a_cond": 12.0, # 8.0
    "b_cond": 0.4,
    "eta_ref": 0.55,

    # Door parameters
    "tau_door_ext": 15.0, # sec
    "tau_door_int": 15.0, # sec
    "W_door_ext": 3.5, # m
    "H_door_ext": 4.0, # m
    "W_door_int": 3.0, # m
    "H_door_int": 3.5, # m
    "td_coeff": 0.15,

    "k_door_ext": .2, # .2,
    "k_door_int": .2, # .2,

    # "m_dot_evap_air_kg_s": 5.0,

    "eps": 1e-5,

    "h_i_walls": 8.0,
    "h_i_roof": 6.0,
    
    # "seed": SEED,
}

TELEMETRY_CONFIG = {
    # "fruit_type": FRUIT_TYPE,
    # "dt_internal": DT_INTERNAL,
    "f_RH_noise": .5,
    # "seed": SEED,
}

RUNTIME_CONFIG = {
    "project_id": "fruit-packing-plant-simulator",
    # "bucket_name": "simulator_test_20260415",
    "chunk_hours": 24,
    "local_out_dir": "data_out",
    "state_path": "state.json",
    "backfill_mode": True, 
    "backfill_days": 365,
}

WEATHER_DATASET_DTYPES = {
    "DTYPES": {
        "T2M": "float64",
        "RH2M": "float64",
        "WS10M": "float64",
        "ALLSKY_SFC_SW_DWN": "float64",
        "T2MDEW": "float64",
        "PS": "float64",
        "TSOIL_54CM": "float64",
        "latitude": "float64",
        "longitude": "float64",
        "location_id": "string",
    },
    "DATETIME_COLS": [
        "datetime",
    ],
}

EVENTS_DTYPES = {
    "DTYPES": {
        "plant_id": "string",
        "event_type": "string",
        "batch_id": "Int64",
        "mass_kg": "float64",
        "fruit_type": "string",
        "quality_grade": "string",
        "tunnel_exit_temp_c": "float64",
        "timezone_id": "string",
    },
    "DATETIME_COLS": [
        "timestamp",
        "loaded_at",
    ],
}

BATCHES_DTYPES = {
    "DTYPES": {
        "plant_id": "string",
        "batch_id": "Int64",
        "fruit_type": "string",
        "mass_kg_initial": "float64",
        "mass_kg_remaining": "float64",
        "tunnel_exit_temp_c": "float64",
        "quality_grade": "string",
        "timezone_id": "string",
    },
    "DATETIME_COLS": [
        "arrival_ts",
        "first_dispatch_ts",
        "final_dispatch_ts",
        "loaded_at",
    ],
}

TELEMETRY_DTYPES = {
    "DTYPES": {
        "plant_id": "string",
        "temp_room_c": "float64",
        "temp_pulp_c": "float64",
        "rh_room_pct": "float64",
        "co2_ppm": "float64",
        "o2_pct": "float64",
        "power_compressor_kw": "float64",
        "temp_evap_inlet_c": "float64",
        "temp_evap_outlet_c": "float64",
        "rh_evap_inlet_pct": "float64",
        "rh_evap_outlet_pct": "float64",
        "evap_fan_speed_pct": "float64",
        "temp_coil_suction_c": "float64",
        "fruit_mass_stored_kg": "float64",
        "comp_modulation_pct": "float64",
        "door_int_open": "Int64",
        "door_ext_open": "Int64",
        "compressor_on": "Int64",
        "humidifier_on": "Int64",
        "fruit_type": "string",
        "timezone_id": "string",
    },
    "DATETIME_COLS": [
        "datetime",
        "loaded_at",
    ],
}

MISSING_TSOIL_54CM = {
        2022: 27.70745528136833,
        2021: 27.803116438356163,
}

FRUITS_CONFIG = {
    "blueberry": {
        "seed_offset": 0,
        "tunnel_exit_fruit_temp_ref": -1.0,
        "max_dwell_days": 15.0,
        "target_rh": 0.925,
        "m_max": 0.0006, # 0.0008
        "setpoint": 0.0,
        "Cp_fruit": 3640.0,
        "k_zone_ref": 9.6e-5,
        "k_p": 2.5e-10,
        "monthly_weight": [0.22, 0.18, 0.13, 0.10, 0.12, 0.25,
                           0.50, 0.85, 1.15, 1.40, 1.30, 0.80],
        "yearly_weight": {
        # Blueberry - normalized to 2022 baseline
            2022: 1.00,
            2023: 1.04,   # +30% in 2022/23 season, but El Niño hit mid-2023
            2024: 0.60,   # -43% due to El Niño
            2025: 1.18,   # record +57% recovery
            2026: 1.17,   # ~flat, slight dip
        },
        # [Grade A (Export), Grade B (Domestic), Grade C (Processing)]
        # Blueberries are highly perishable; high sorting standards keep export high.
        "base_quality": [0.75, 0.18, 0.07],
        "Q_rated": 22_000.0, # W # 22_000.0
        "TD_design": 4.0, # 3.0 2.0
    },
    "avocado": {
        "seed_offset": 333,
        "tunnel_exit_fruit_temp_ref": 6.0,  # pre-cooled to ~6°C before cold storage
        "max_dwell_days": 21.0,
        "target_rh": 0.925, # 0.925
        "m_max": 0.0005, # 0.0008
        "setpoint": 5.5,
        "Cp_fruit": 3010.0,  # from USDA specific heat tables
        "k_zone_ref": 6.4e-5,
        "k_p": 4.0e-10,  # avocados transpire more due to higher oil content
        "monthly_weight": [0.18, 0.22, 0.38, 0.65, 1.20, 1.40,
                           1.40, 1.20, 0.60, 0.30, 0.15, 0.10],
        # peaks May-Aug per ProHass/USDA data: 74% of yearly exports
        # Avocado - normalized to 2022 baseline
        "yearly_weight": {
            2022: 1.00,
            2023: 1.40,   # record year
            2024: 1.25,   # -10% from 2023
            2025: 1.73,   # record +38%
            2026: 1.83,   # +6% projected
        },
        # [Grade A (Export/Hass Category 1), Grade B (Category 2), Grade C (Oil/Guacamole)]
        # Avocados have rigid sizing/cosmetic rules; more fruit drops to category 2 natively.
        "base_quality": [0.70, 0.22, 0.08],
        "Q_rated": 22_000.0, # W
        "TD_design": 4.0,
    },
}

for fruit in FRUITS_CONFIG.keys():
    Q_rated = FRUITS_CONFIG[fruit]["Q_rated"]
    TD_design = FRUITS_CONFIG[fruit]["TD_design"]

    UA_coil_theoretical = Q_rated/TD_design

    Cp_air = SIMULATION_CONFIG["Cp_air"]
    BF = SIMULATION_CONFIG["BF"]

    FRUITS_CONFIG[fruit]["m_dot_air_theoretical"] = UA_coil_theoretical / (Cp_air * (1 - BF))

