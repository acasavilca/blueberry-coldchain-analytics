import numpy as np

SEED = 42
FRUIT_TYPE = "blueberry"
LATITUDE = -8.5771
LONGITUDE = -78.5661
RESAMPLE_RATE = "1min"
DT_INTERNAL = 1.0 # 10.0

LOCATION = {
    "latitude": str(LATITUDE),
    "longitude": str(LONGITUDE),
}

RETRIEVAL_CONFIG = {
    "measurements": "temperature_2m,relative_humidity_2m,wind_speed_10m,shortwave_radiation,dewpoint_2m,surface_pressure,soil_temperature_54cm",
    "timezone": "auto",
}

FORCING_CONFIG = {
    "resample_rate": RESAMPLE_RATE,
}

SCHEDULER_CONFIG = {
    "forcing_dt_sec": 60.0,
    "min_ship_mass": 5000.0,
    "max_inventory_kg": 100_000.0,
    "arrival_scale": 1100.0, # 275.0,
    "shipment_scale": 20_000.0, # 9500.0,
    # "seed": SEED,
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
    "Q_rated": 22_000.0,
    "f_structure": 0.05,
    "k_wind_U": 0.02,
    "Cp_air": 1006.0,

    # control
    "setpoint": 0.0,
    "deadband": .5, # 0.5,
    "tau_cool": 120.0, # 120.0, # 120.0, # 60.0,
    "tau_sensor": 90.0, # 90.0, # 90.0, # 60.0,

    # timestep
    "dt_internal": DT_INTERNAL,

    # fruit / product
    "h_respiration": 10.61,
    # "tunnel_exit_fruit_temp": -1.0,
    # "Cp_fruit": 3640.0,
    # chosen zone only; do not keep the whole k_by_zone dict
    "k_zone": float(np.log(8) / (24.0 * 3600.0)),   # cold_storage

    # air exchange / free volume
    "n_ach_eff_per_sec": 2.0 / 86400, # 2.0 / 86400,        # per day

    # gas constants
    "R_const": 8.314,
    "M_CO2": 44.0,
    "M_air": 28.97,

    # coil / humidity control
    "T_coil_ref": -2.0,
    "BF": 0.20, # 0.5, # 0.2,
    "tau_condense": 30.0, # 120.0, # 15.0,   # replace if you settled on another final value
    # "target_rh": .965, # 0.95,
    "rh_deadband": 0.02,
    "tau_humid_frac": 180.0, # 180.0, # 180.0, # 60.0,
    "tau_humid_sensor": 300.0, # 300.0, # 10.0, # 10.0,
    "m_max": 0.0008, # 0.003,
    "f_evap_humid": 0.95, # 0.4,
    
    # transpiration
    # "k_p": 2.5e-10,

    "CO2_outdoor_ppm": 420,
    "O2_outdoor_pct": 20.95,
    "cooling_frac_init": 0.0,

    "f_min": .05,
    "V_free_min": .05,
    "m_air_room_min": .05,
    "rho_load_bulk": 250.0,

    "cooling_call_init": False,
    "cooling_frac_init": 0.0,

    "humidifier_call_init": False,
    "humidifier_frac_init": 0.0,

    "condense_frac_init": 0.0,

    "fruit_mass_kg_init": 25000.0,     # your current value
    "total_water_loss_kg_init": 0.0,
    "eta_init": 0.55,
    
    # Plant parameters
    "T_plant_a": 10.0, # Celsius
    "T_plant_b": 0.2,
    "dT_plant": 60.0, # sec
    "tau_plant": 7200.0, # hrs
    "RH_plant": 0.65,

    # Door parameters
    "tau_door_ext": 10.0, # sec
    "tau_door_int": 15.0, # sec
    "W_door_ext": 3.5, # m
    "H_door_ext": 4.0, # m
    "W_door_int": 3.0, # m
    "H_door_int": 3.5, # m
    "td_coeff": 0.15,

    "k_door_ext": .3, # .2,
    "k_door_int": .2, # .2,

    "m_dot_evap_air_kg_s": 5.0,

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
    },
    "DATETIME_COLS": [
        "datetime",
    ],
}
MISSING_TSOIL_54CM = {
        "2022": 27.70745528136833,
        "2021": 27.803116438356163,
}

FRUIT_CONFIGS = {
    "blueberry": {
        "tunnel_exit_fruit_temp": -1.0,
        "target_rh": 0.965,
        "setpoint": 0.0,
        "Cp_fruit": 3640.0,
        "k_p": 2.5e-10,
        "T_coil_ref": -2.0,
        "monthly_weight": [0.15, 0.10, 0.08, 0.08, 0.12, 0.25,
                           0.50, 0.85, 1.15, 1.40, 1.30, 0.80],
        "yearly_weight": {
        # Blueberry - normalized to 2022 baseline
            2022: 1.00,
            2023: 1.04,   # +30% in 2022/23 season, but El Niño hit mid-2023
            2024: 0.60,   # -43% due to El Niño
            2025: 1.18,   # record +57% recovery
            2026: 1.17,   # ~flat, slight dip
        },
    },
    "avocado": {
        "tunnel_exit_fruit_temp": 6.0,  # pre-cooled to ~6°C before cold storage
        "target_rh": 0.92,
        "setpoint": 5.0,
        "Cp_fruit": 3010.0,  # from USDA specific heat tables
        "k_p": 4.0e-10,  # avocados transpire more due to higher oil content
        "T_coil_ref": 3.0,
        "monthly_weight": [0.10, 0.15, 0.30, 0.60, 1.20, 1.40,
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
    },
}
