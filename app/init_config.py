import numpy as np
last_state = {
    # check if this is the first run
    "is_first_run": np.int8(1),

    # controllers
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

    # Logistics
    "door_ext_open_fraction_init": 0.0,
    "door_int_open_fraction_init": 0.0,

    # Fruit mass
    "fruit_mass_kg_init": 0.0,
    "total_water_loss_kg_init": 0.0,
}
