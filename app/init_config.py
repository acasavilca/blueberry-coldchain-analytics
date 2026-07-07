import numpy as np

initialization_state = {
    # Check if this is the first run
    "is_first_run": np.int8(1),

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
    
    # Logistics Tracking (Pointers & Counters)
    "n_active_init": np.int64(0),
    "next_batch_id_init": np.int64(0),
    "front_idx_init": np.int64(0),
    "back_idx_init": np.int64(0),

    # Logistics Arrays (Initialized empty since first run overrides them anyway)
    "batch_masses_init": np.array([], dtype=np.float64),
    "batch_incoming_temps_init": np.array([], dtype=np.float64),
    "batch_quality_grades_init": np.array([], dtype=np.int8),
    "batch_ids_init": np.array([], dtype=np.int64),

    # Fruit mass
    "fruit_mass_kg_init": 0.0,
    "total_water_loss_kg_init": 0.0,

    "last_epoch_timestamp": 0.0,
}

