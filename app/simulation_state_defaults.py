import numpy as np

INITIALIZATION_STATE = {
    # Check if this is the first run
    "is_first_run": np.int8(1),
    # Global step
    "global_step_init": np.int64(0),
    # Controllers
    "cooling_call_init": np.int8(0),
    "humidifier_call_init": np.int8(0),
    "cooling_frac_init": 0.0,
    "humidifier_frac_init": 0.0,
    "condense_frac_init": 0.0,
    # Temperature
    "T_room_init": 0.0,
    "T_sensor_init": 0.0,
    "T_plant_init": 0.0,
    "T_plant_target_init": 0.0,
    "T_pulp_init": 0.0,
    # Gases
    "CO2_ppm_init": 0.0,
    "O2_pct_init": 0.0,
    # Humidity parameters
    "W_room_init": 0.0,
    "P_w_room_init": 0.0,
    "RH_room_init": 0.0,
    "RH_room_sensor_init": 0.0,
    "W_coil_sat_init": 0.0,
    # Logistics Scalars
    "door_ext_open_fraction_init": 0.0,
    "door_int_open_fraction_init": 0.0,
    "ext_open_until_init": 0.0,
    "int_open_until_init": 0.0,
    # Logistics Tracking 
    "n_active_init": np.int64(0),
    "next_batch_id_init": np.int64(0),
    "front_idx_init": np.int64(0),
    "back_idx_init": np.int64(0),
    # Logistics Arrays
    "batch_masses_init": np.array([], dtype=np.float64),
    "batch_incoming_temps_init": np.array([], dtype=np.float64),
    "batch_quality_grades_init": np.array([], dtype=np.int8),
    "batch_ids_init": np.array([], dtype=np.int64),
    "active_batch_mask_init": np.array([], dtype=np.int8),
    # Fruit mass
    "fruit_mass_kg_init": 0.0,
    "total_water_loss_kg_init": 0.0,
    "last_epoch_timestamp": 0.0,
}

STATE_SCHEMA = {
    "scalar_int8": {
        "is_first_run": "int8",
        "cooling_call_init": "int8",
        "humidifier_call_init": "int8",
    },
    "scalar_float64": {
        "cooling_frac_init": "float64",
        "humidifier_frac_init": "float64",
        "condense_frac_init": "float64",
        "T_room_init": "float64",
        "T_sensor_init": "float64",
        "T_plant_init": "float64",
        "T_plant_target_init": "float64",
        "T_pulp_init": "float64",
        "CO2_ppm_init": "float64",
        "O2_pct_init": "float64",
        "W_room_init": "float64",
        "P_w_room_init": "float64",
        "RH_room_init": "float64",
        "RH_room_sensor_init": "float64",
        "W_coil_sat_init": "float64",
        "door_ext_open_fraction_init": "float64",
        "door_int_open_fraction_init": "float64",
        "ext_open_until_init": "float64",
        "int_open_until_init": "float64",
        "fruit_mass_kg_init": "float64",
        "total_water_loss_kg_init": "float64",
        "last_epoch_timestamp": "float64",
    },
    "scalar_int64": {
        "global_step_init": "int64",
        "n_active_init": "int64",
        "next_batch_id_init": "int64",
        "front_idx_init": "int64",
        "back_idx_init": "int64",
    },
    "array_float64": {
        "batch_masses_init": "float64",
        "batch_incoming_temps_init": "float64",
    },
    "array_int64": {
        "batch_ids_init": "int64",
    },
    "array_int8": {
        "batch_quality_grades_init": "int8",
        "active_batch_mask_init": "int8",
    },
}
