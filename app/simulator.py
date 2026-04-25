from random import seed
import sys
from tracemalloc import start
import requests
from numba import njit, types
from numba.typed import Dict
import pandas as pd
import numpy as np
from pathlib import Path
import json
from pytimeparse.timeparse import timeparse
import base64
import struct
from config import MISSING_TSOIL_54CM, FRUIT_CONFIGS
from respiration_data import *

ELEVATION_CACHE_PATH = Path("elevation_cache.json")

# Fruit-dependent parameters
# TUNNEL_EXIT_FRUIT_TEMP = FRUIT_CONFIGS[FRUIT_TYPE]["tunnel_exit_fruit_temp"]
# TARGET_RH = FRUIT_CONFIGS[FRUIT_TYPE]["target_rh"]
# SETPOINT = FRUIT_CONFIGS[FRUIT_TYPE]["setpoint"]
# CP_FRUIT = FRUIT_CONFIGS[FRUIT_TYPE]["Cp_fruit"]
    

def encode_coordinates(lat: float, lon: float) -> str:
    """Converts lat/lon floats into a short URL-safe string key."""
    # '!dd' packs two 64-bit double precision floats (big-endian)
    byte_data = struct.pack('!dd', lat, lon)
    
    # Encode to base64 and remove the trailing '=' padding for a cleaner string
    encoded_str = base64.urlsafe_b64encode(byte_data).decode('utf-8').rstrip('=')
    return encoded_str

def decode_coordinates(key: str) -> tuple[float, float]:
    """Converts the string key back into a (latitude, longitude) tuple."""
    # Restore the stripped padding for the base64 decoder
    padding = '=' * (4 - len(key) % 4)
    byte_data = base64.urlsafe_b64decode(key + padding)
    
    # Unpack the 16 bytes back into two floats
    lat, lon = struct.unpack('!dd', byte_data)
    return lat, lon

@njit
def manual_clipping(value, low, high):
    if value < low:
        return low
    elif value > high:
        return high
    else:
        return value

@njit
def least_squares_regression(x, y):
    # --- Manual Replacement for np.polyfit(x, y, 1) ---
    n = len(x)
    sum_x = np.sum(x)
    sum_y = np.sum(y)
    sum_xx = np.sum(x*x)
    sum_xy = np.sum(x*y)

    denominator = (n * sum_xx - sum_x**2)

    if denominator == 0:
        m, b = 0.0, 0.0  # Avoid division by zero for single points or identical X values
    else:
        m = (n * sum_xy - sum_x * sum_y) / denominator
        b = (sum_y - m * sum_x) / n
    return m, b

## Cache elevation
def _coord_key(lat: float, lon: float, ndigits: int = 4) -> str:
    return f"{round(float(lat), ndigits)},{round(float(lon), ndigits)}"

def fetch_elevation_from_api(latitude: float, longitude: float) -> float:
    url = "https://api.open-meteo.com/v1/elevation"
    params = {"latitude": latitude, "longitude": longitude}
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    return float(data["elevation"][0])


def get_cached_elevation(latitude: float, longitude: float) -> float:
    key = _coord_key(latitude, longitude)

    cache = {}
    if ELEVATION_CACHE_PATH.exists():
        with open(ELEVATION_CACHE_PATH, "r") as f:
            cache = json.load(f)

    if key in cache:
        return float(cache[key])

    elevation = fetch_elevation_from_api(latitude, longitude)
    cache[key] = elevation

    with open(ELEVATION_CACHE_PATH, "w") as f:
        json.dump(cache, f, indent=2)

    return elevation

###

def retrieve_satellite_data(
    # past_days,
    # forecast_days,
    start_dt,
    end_dt,
    latitude,
    longitude,
    dtypes,
    measurements,
    timezone,
    frequency="hourly",
    # is_historical=True,
):
    
    historical = "https://historical-forecast-api.open-meteo.com/v1/forecast"
    forecast ="https://api.open-meteo.com/v1/forecast"

    # start_dt, end_dt = pd.to_datetime(start_dt).strftime('%Y-%m-%d'), pd.to_datetime(end_dt).strftime('%Y-%m-%d')

    start_dt, end_dt = pd.to_datetime(start_dt), pd.to_datetime(end_dt)
    today = pd.Timestamp.today()

    days_since_start_sim = (today - start_dt).days
    days_since_end_sim = (today - end_dt).days

    if days_since_start_sim <= 60 and days_since_end_sim <= 60:
        base_url = forecast
    elif days_since_start_sim > 60 and days_since_end_sim > 60:
        base_url = historical
    else:
        base_url = historical
    
    if days_since_end_sim < 4 and base_url == historical:
        print("Please select an end date at least 4 days in the past to ensure historical data availability, or select more recent dates to use forecast data.")
        sys.exit("Error: Exiting program.")

    if start_dt < pd.Timestamp('2021-03-23') or end_dt < pd.Timestamp('2021-03-23'):
        print("Please select start and end dates from 2021-03-23 onward to ensure data availability, or select more recent dates to use forecast data.")
        sys.exit("Error: Exiting program.")

    print(f"Using {base_url} endpoint based on selected date range and current date.")

    start_dt, end_dt = start_dt.strftime('%Y-%m-%d'), end_dt.strftime('%Y-%m-%d')

    params = {
        "start_date": start_dt,
        "end_date": end_dt,
        "latitude": latitude,
        "longitude": longitude,
        frequency: measurements,
        "wind_speed_unit": "ms",
        "timezone": timezone,
        # "past_days": past_days,
        # "forecast_days": forecast_days,
    }

    # Execute the request
    response = requests.get(base_url, params=params)

    # Check for success and parse JSON
    if response.status_code == 200:
        data = response.json()
    else:
        print(f"Error {response.status_code}: {response.text}")
        exit(1)

    df = pd.DataFrame(data['hourly'])
    
    # Rename to match your simulator's expected NASA column names
    df = df.rename(columns={
        "time": "datetime",
        "temperature_2m": "T2M",
        "relative_humidity_2m": "RH2M",
        "dewpoint_2m": "T2MDEW",
        "shortwave_radiation": "ALLSKY_SFC_SW_DWN",
        "wind_speed_10m": "WS10M",
        "surface_pressure": "PS",
        "soil_temperature_54cm": "TSOIL_54CM",
    })

    year_str = str(pd.Timestamp(start_dt).year)
    fill_value = MISSING_TSOIL_54CM.get(year_str, 27.0)  # fallback to 27 if year not in dict
    rng = np.random.default_rng(42)
    noise = rng.normal(0, 0.1, size=df["TSOIL_54CM"].isna().sum())
    df.loc[df["TSOIL_54CM"].isna(), "TSOIL_54CM"] = fill_value + noise

    df['PS'] /= 10.0 # hPa to kPa

    datetime_cols = dtypes['DATETIME_COLS']
    df[datetime_cols] = df[datetime_cols].apply(
        pd.to_datetime
    ) # , format="%Y%m%d%H%M"

    dtypes = dtypes['DTYPES']

    return df.astype(dtypes)    

def prepare_forcing_arrays(df, resample_rate='1min'):
    df_resampled = (
        df.set_index('datetime')
          .resample(resample_rate)
          .interpolate(method='time')
          .reset_index()
    )
    dt_arr = df_resampled['datetime'].to_numpy()

    forcing = {
        # full datetime, for Python-side scheduling / labeling / storage
        'dt_arr': dt_arr,

        # Numba-friendly calendar fields
        'year':  dt_arr.astype('datetime64[Y]').astype(np.int64) + 1970,
        'month': (dt_arr.astype('datetime64[M]').astype(np.int64) % 12) + 1,
        'day':   (dt_arr.astype('datetime64[D]') - dt_arr.astype('datetime64[M]')).astype(np.int64) + 1,
        'hour':  (dt_arr.astype('datetime64[h]').astype(np.int64) % 24),
        'minute': (dt_arr.astype('datetime64[m]').astype(np.int64) % 60),
        'second': (dt_arr.astype('datetime64[s]').astype(np.int64) % 60),
        'day_of_year': (
            dt_arr.astype('datetime64[D]') - dt_arr.astype('datetime64[Y]')
        ).astype(np.int64) + 1,

        # weather
        'T_ambient': df_resampled['T2M'].to_numpy(np.float64),
        'RH_ambient': df_resampled['RH2M'].to_numpy(np.float64) / 100.0,
        'P': df_resampled['PS'].to_numpy(np.float64) * 1e3,
        'GHI': df_resampled['ALLSKY_SFC_SW_DWN'].to_numpy(np.float64),
        'WS': df_resampled['WS10M'].to_numpy(np.float64),
        'T2MDEW': df_resampled['T2MDEW'].to_numpy(np.float64),
        'TSOIL_54CM': df_resampled['TSOIL_54CM'].to_numpy(np.float64),
    }
    return forcing, df_resampled, float(timeparse(resample_rate))

def get_tr(respiration_database, fruit):
    df = (
        pd.DataFrame(respiration_database["data"])
        .T
        .rename_axis("fruit")
        .reset_index()
    )
    df.columns = ["fruit"] + [int(c) for c in df.columns[1:]]

    df_long = df.melt(
        id_vars="fruit",
        var_name="temperature_C",
        value_name="respiration"
    )

    sub = df_long[df_long["fruit"] == fruit]

    if sub.empty:
        raise ValueError(f"Fruit {fruit!r} not found in respiration DB")

    else:
        T = pd.to_numeric(sub["temperature_C"].values, errors="coerce")
        R = pd.to_numeric(sub["respiration"].values, errors="coerce")

        return np.asarray(T, dtype=float), np.asarray(R, dtype=float)

@njit
def get_t_ref(T, T_min, T_max):
    mid = (T_min + T_max) / 2

    # Initialize with the first element
    best_t = T[0]
    best_diff = abs(best_t - mid)

    for i in range(1, len(T)):
        t = T[i]
        diff = abs(t - mid)

        # This mimics the (diff, t) tuple comparison logic
        if diff < best_diff:
            best_diff = diff
            best_t = t
        elif diff == best_diff:
            if t < best_t:
                best_t = t

    return best_t

@njit
def get_fruit_resp_params(T_lookup, R_lookup):
    order = np.argsort(T_lookup)
    T = T_lookup[order]
    R = R_lookup[order]
    T_max = np.max(T)
    T_min = np.min(T)
    buffer = 0.5*np.median(np.diff(T)) if len(T) > 1 else 0.0
    T_lo = T_min - buffer
    T_hi = T_max + buffer

    T_ref = get_t_ref(T, T_min, T_max)
    x = T - T_ref
    y = np.log(R)

    m, b = least_squares_regression(x, y)

    Q_10 = np.exp(10*m)
    R_ref = np.exp(b)

    return T, R, T_min, T_max, T_lo, T_hi, T_ref, Q_10, R_ref

@njit
def p_sat_magnus(T):
    P_sat = 610.78 * np.exp(17.27 * T / (T + 237.3))
    return P_sat

@njit
def partial_pressure_from_w(W, P):
    P_w = (W*P)/(0.622 + W)
    return P_w

@njit
def w_from_partial_pressure(P_w, P):
    W = 0.622*P_w/(P - P_w)
    return W

@njit
def compressor_metrics(T_room, T_ambient, eta, Q_cooling):
    ## COP and compressor power
    T_evap_K = (T_room - 8) + 273.15      # evaporator 8°C below setpoint
    T_cond_K = (T_ambient + 12) + 273.15    # condenser 12°C above ambient
    COP_carnot = T_evap_K / (T_cond_K - T_evap_K)
    COP_actual = COP_carnot * eta
    COP_actual = max(COP_carnot * eta, 0.5)  # floor at 0.5, physically unreachable below this
    W_compressor_kw = Q_cooling / (COP_actual * 1000)
    Q_condenser_kw = W_compressor_kw + Q_cooling/1000
    return COP_actual, W_compressor_kw, Q_condenser_kw

@njit
def fruit_respiration_rate(k_p, fruit_mass_kg, P_sat_pulp, P_w_room):
    m_transp_rate = max(k_p * fruit_mass_kg * (P_sat_pulp - P_w_room), 0)
    return m_transp_rate

@njit
def get_r_fruit(T_pulp, T, R, T_min, T_max, T_lo, T_hi, T_ref, R_ref, Q_10):
    idx = np.searchsorted(T, T_pulp)

    if idx < len(T) and T[idx] == T_pulp:
        R_fruit = R[idx]
    elif idx > 0 and T[idx - 1] == T_pulp:
        R_fruit = R[idx - 1]
    elif T_pulp < T_min or T_pulp > T_max:
        T_pulp_eff = manual_clipping(T_pulp, T_lo, T_hi)
        R_fruit = R_ref * Q_10**((T_pulp_eff - T_ref) / 10.0)
    else:
        T_1, T_2 = T[idx - 1], T[idx]
        R_1, R_2 = R[idx - 1], R[idx]
        R_fruit = R_1 * (R_2 / R_1)**((T_pulp - T_1) / (T_2 - T_1))

    return R_fruit

@njit
def sigma_T_calc(cooling_on):
    return 0.08 if cooling_on else 0.15

def create_numba_dicts():
    float64_dict = Dict.empty(
        key_type=types.unicode_type,
        value_type=types.float64[:],
        )
    int8_dict = Dict.empty(
        key_type=types.unicode_type,
        value_type=types.int8[:],
        )
    return float64_dict, int8_dict

@njit
def calculate_door_infiltration_gosney(
    T_room,
    T_source,  # Pass T_ambient OR T_plant here
    P_Pa,
    WS2M,
    A_door,    # Pass A_door_ext OR A_door_int
    H_door,    # Pass H_door_ext OR H_door_int
    R_dry,
    is_outdoor_door,
    ):

    g = 9.81

    # 1. Absolute Temperatures
    T_room_k = T_room + 273.15
    T_source_k = T_source + 273.15

    # Density is based on the temperature gradient between the two specific zones
    T_cold = min(T_room_k, T_source_k)
    T_warm = max(T_room_k, T_source_k)

    if T_room_k == T_source_k:
        T_cold = T_room_k
        T_warm = T_source_k

    # 2. Densities
    rho_cold = P_Pa / (R_dry * T_cold)
    rho_warm = P_Pa / (R_dry * T_warm)

    rho_cold = max(rho_cold, 1e-9)
    rho_warm = max(rho_warm, 1e-9)

    # 3. Buoyancy (Gosney & Olama)
    density_term = (1 - rho_warm/rho_cold)**0.5
    interference_term = (2 / (1 + (rho_cold/rho_warm)**(1/3)))**1.5

    # Use the generic A_door and H_door passed to the function
    Q_buoyancy = 0.221 * A_door * np.sqrt(g * H_door) * density_term * interference_term

    # 4. Wind (Only for external doors)
    if is_outdoor_door:
        C_wind = 0.5
        v_wind_eff = C_wind * WS2M
        Q_wind = 0.3 * A_door * v_wind_eff
    else:
        Q_wind = 0.0

    # 5. Combined Flow (m3/s)
    Q_total = np.sqrt(Q_buoyancy**2 + Q_wind**2)

    return Q_total, P_Pa / (R_dry * T_source_k)  # rho_warm is the density of the air ENTERING the room

def sample_gamma_minutes(rng, mean_min: float, shape: float) -> float:
    scale = mean_min / shape
    return float(rng.gamma(shape, scale))

def build_door_and_mass_schedules(
    hour_arr: np.ndarray,
    minute_arr: np.ndarray,
    doy_arr: np.ndarray,
    month_arr: np.ndarray,
    year_arr: np.ndarray,
    # tunnel_exit_fruit_temp: float,
    max_inventory_kg: float,
    arrival_scale: float,
    shipment_scale: float,
    fruit_type: str,
    forcing_dt_sec: float = 60.0,
    min_ship_mass: float = 20.0,
    seed: int = 42,
):
    new_seed = abs(seed + int(year_arr[0]) * 10000 + int(month_arr[0]) * 100 + int(doy_arr[0]))

    tunnel_exit_fruit_temp = FRUIT_CONFIGS[fruit_type]["tunnel_exit_fruit_temp"]

    rng = np.random.default_rng(new_seed)
    n = len(hour_arr)
    dt_hr = forcing_dt_sec/3600.0

    door_ext_schedule = np.zeros(n, dtype=np.int8)
    door_int_schedule = np.zeros(n, dtype=np.int8)
    fruit_mass_delta_kg = np.zeros(n, dtype=np.float64)
    incoming_temperature_arr = np.full(n, np.nan, dtype=np.float64)

    # monthly_weight = np.array([
    #     0.70, 0.75, 0.80, 0.85, 0.90, 1.00,
    #     1.10, 1.25, 1.40, 1.50, 1.35, 1.00
    # ], dtype=float)

    # monthly_weight = np.array([
    #     0.15, 0.10, 0.08, 0.08, 0.12, 0.25,
    #     0.50, 0.85, 1.15, 1.40, 1.30, 0.80
    # ], dtype=float)

    # Background door openings not directly tied to product movement
    lambda_ext_bg_per_hour = np.array([
        0.00, 0.00, 0.00, 0.00, 0.00, 0.02,
        0.05, 0.10, 0.20, 0.25, 0.20, 0.15,
        0.10, 0.10, 0.15, 0.20, 0.25, 0.20,
        0.10, 0.05, 0.02, 0.00, 0.00, 0.00
    ], dtype=float)

    lambda_int_bg_per_hour = np.array([
        0.02, 0.02, 0.02, 0.02, 0.02, 0.05,
        0.20, 0.50, 1.00, 1.20, 1.30, 1.20,
        1.00, 0.80, 1.00, 1.20, 1.30, 1.10,
        0.60, 0.20, 0.08, 0.05, 0.02, 0.02
    ], dtype=float) # * 0.6

    # Logistics event rates
    # Internal arrivals into cold room from process side
    lambda_arrival_per_hour = np.array([
        0.00, 0.00, 0.00, 0.00, 0.00, 0.05,
        0.20, 0.60, 1.20, 1.50, 1.20, 0.80,
        0.60, 0.50, 0.80, 1.00, 1.20, 1.00,
        0.50, 0.20, 0.05, 0.00, 0.00, 0.00
    ], dtype=float)

    # lambda_arrival_per_hour = np.array([
    # 0.00, 0.00, 0.00, 0.00, 0.00, 0.20,
    # 0.80, 2.40, 4.80, 6.00, 4.80, 3.20,
    # 2.40, 2.00, 3.20, 4.00, 4.80, 4.00,
    # 2.00, 0.80, 0.20, 0.00, 0.00, 0.00
    # ], dtype=float)

    # External shipments out of cold room to trucks
    lambda_shipment_per_hour = np.array([
        0.00, 0.00, 0.00, 0.00, 0.00, 0.00,
        0.00, 0.00, 0.05, 0.10, 0.15, 0.20,
        0.40, 0.50, 0.80, 1.00, 1.20, 1.00,
        0.50, 0.20, 0.05, 0.00, 0.00, 0.00
    ], dtype=float)

    # Door open durations
    ext_duration_mean_min = 1.5
    int_duration_mean_min = 0.5
    duration_shape = 2.0

    ext_open_until = -1
    int_open_until = -1

    event_code = np.zeros(n, dtype=np.int8)

    inventory_kg = 0.0
    for i in range(n):
        hr = int(hour_arr[i])
        minute = int(minute_arr[i])

        # simple seasonality weights
        month_idx = int(month_arr[i]) - 1
        season_weight = FRUIT_CONFIGS[fruit_type]["monthly_weight"][month_idx]
        year = int(year_arr[i])
        yearly_w = FRUIT_CONFIGS[fruit_type]["yearly_weight"]
        year_weight = yearly_w.get(year, yearly_w[max(yearly_w.keys())])
        throughput_weight = season_weight * year_weight
        # year_offset = int(year_arr[i]) - 2023
        # trend_weight = 1.0 + 0.08 * year_offset
        # trend_weight = max(1.0 + 0.08 * year_offset, 0.1)
        # throughput_weight = season_weight * trend_weight

        # ---- 1) Generate logistics events first ----
        p_arrival = min(1.0, lambda_arrival_per_hour[hr] * dt_hr * throughput_weight)
        p_shipment = min(1.0, lambda_shipment_per_hour[hr] * dt_hr * throughput_weight)

        if inventory_kg >= max_inventory_kg:
            p_arrival = 0.0

        if inventory_kg <= min_ship_mass:
            p_shipment = 0.0

        u = rng.random()

        # Arrival into cold room through internal door
        if u < p_arrival:
            raw_mass = rng.gamma(shape=2.0, scale=arrival_scale*throughput_weight) #*throughput_weight)
            mass = min(raw_mass, max_inventory_kg - inventory_kg)

            if mass > 0:
                fruit_mass_delta_kg[i] += mass
                inventory_kg += mass
                event_code[i] = 1
                incoming_temperature_arr[i] = np.clip(
                    rng.normal(tunnel_exit_fruit_temp + 1, 0.4),
                    tunnel_exit_fruit_temp - 0.5,
                    tunnel_exit_fruit_temp + 2.0
                )

                dur_min = sample_gamma_minutes(rng, int_duration_mean_min, duration_shape)
                dur_steps = max(1, int(round((dur_min * 60.0) / forcing_dt_sec)))
                int_open_until = max(int_open_until, i + dur_steps)
        
        # Shipment out of cold room through external door
        elif u < p_arrival + p_shipment: #  and inventory_kg > 0.0:
            requested_mass = rng.gamma(shape=2.0, scale=shipment_scale *throughput_weight)
            mass = min(requested_mass, inventory_kg) # *throughput_weight)
            if mass > min_ship_mass or mass == inventory_kg:
                fruit_mass_delta_kg[i] -= mass
                inventory_kg -= mass
                event_code[i] = 2

                dur_min = sample_gamma_minutes(rng, ext_duration_mean_min, duration_shape)
                dur_steps = max(1, int(round((dur_min * 60.0) / forcing_dt_sec)))
                ext_open_until = max(ext_open_until, i + dur_steps)

        if (
            hr == 23 and minute == 50
            and inventory_kg > 0.0
            and event_code[i]==0
        ):
            mass = inventory_kg
            fruit_mass_delta_kg[i] -= mass
            inventory_kg = 0.0
            event_code[i] = 2

            dur_min = sample_gamma_minutes(rng, ext_duration_mean_min, duration_shape)
            dur_steps = max(1, int(round((dur_min * 60.0) / forcing_dt_sec)))
            ext_open_until = max(ext_open_until, i + dur_steps)

         # ---- 2) Add background door openings ----

        p_ext_bg = min(1.0, lambda_ext_bg_per_hour[hr] * dt_hr)
        if rng.random() < p_ext_bg:
            if rng.random() < 0.50:
                dur_min = sample_gamma_minutes(rng, ext_duration_mean_min, duration_shape)
                dur_steps = max(1, int(round((dur_min * 60.0) / forcing_dt_sec)))
                ext_open_until = max(ext_open_until, i + dur_steps)

        p_int_bg = min(1.0, lambda_int_bg_per_hour[hr] * dt_hr)
        if rng.random() < p_int_bg:
            if rng.random() < 0.70:
                dur_min = sample_gamma_minutes(rng, int_duration_mean_min, duration_shape)
                dur_steps = max(1, int(round((dur_min * 60.0) / forcing_dt_sec)))
                int_open_until = max(int_open_until, i + dur_steps)

        # ---- 3) Write schedules ----

        if i < ext_open_until:
            door_ext_schedule[i] = 1
        if i < int_open_until:
            door_int_schedule[i] = 1

    return door_ext_schedule, door_int_schedule, fruit_mass_delta_kg, event_code, incoming_temperature_arr

def build_batch_event_tables(
    dt_arr,
    fruit_mass_delta_kg,
    incoming_temperature_arr,
    plant_id,
    fruit_type="blueberry",
    active_batches=None,
    next_batch_id=1,
    seed=42,
):
    start = pd.Timestamp(dt_arr[0])
    new_seed = abs(seed + start.year * 10000 + start.month * 100 + start.day)
    rng = np.random.default_rng(new_seed)

    if active_batches is None:
        active_batches = []
    
    event_rows = []
    batch_rows = []

    n = len(dt_arr)

    for i in range(n):
        ts = dt_arr[i]
        delta = fruit_mass_delta_kg[i]

        # -----------------------------
        # ARRIVAL
        # -----------------------------
        if delta > 0:
            mass_in = float(delta)

            incoming_temperature = incoming_temperature_arr[i]
            # sampled_temp = np.clip(
            #     rng.normal(tunnel_exit_fruit_temp + 1.0, 0.4),
            #     tunnel_exit_fruit_temp - 0.5,
            #     tunnel_exit_fruit_temp + 2.0
            # )   # tunnel exit temp
            sampled_grade = rng.choice(["A", "B", "C"], p=[0.7, 0.2, 0.1])

            # # schedule dispatch same day
            # residence_hours = rng.uniform(4.0, 12.0)
            # dispatch_ts = ts + np.timedelta64(int(residence_hours * 60), "m")

            # day_end = ts.astype("datetime64[D]") + np.timedelta64(1, "D")
            # if dispatch_ts >= day_end:
            #     dispatch_ts = day_end - np.timedelta64(1, "m")

            batch = {
                "plant_id": plant_id,
                "batch_id": next_batch_id,
                "fruit_type": fruit_type,
                "arrival_ts": ts,
                # "scheduled_dispatch_ts": dispatch_ts,
                "mass_kg_initial": mass_in,
                "mass_kg_remaining": mass_in,
                "tunnel_exit_temp_c": incoming_temperature,
                "quality_grade": sampled_grade,
                "first_dispatch_ts": pd.NaT,
                "final_dispatch_ts": pd.NaT,
            }

            active_batches.append(batch)

            event_rows.append({
                "plant_id": plant_id,
                "timestamp": ts,
                "event_type": "arrival",
                "batch_id": next_batch_id,
                "mass_kg": mass_in,
                "fruit_type": fruit_type,
                "quality_grade": sampled_grade,
                "tunnel_exit_temp_c": incoming_temperature,
            })

            next_batch_id += 1

        # -----------------------------
        # DISPATCH (FIFO)
        # -----------------------------
        elif delta < 0:
            mass_to_remove = float(-delta)

            while mass_to_remove > 1e-9 and active_batches:
                b = active_batches[0]
                removable = min(b["mass_kg_remaining"], mass_to_remove)

                if pd.isna(b["first_dispatch_ts"]):
                    b["first_dispatch_ts"] = ts

                b["mass_kg_remaining"] -= removable
                mass_to_remove -= removable

                event_rows.append({
                    "plant_id": plant_id,
                    "timestamp": ts,
                    "event_type": "dispatch",
                    "batch_id": b["batch_id"],
                    "mass_kg": removable,
                    "fruit_type": b["fruit_type"],
                    "quality_grade": b["quality_grade"],
                    "tunnel_exit_temp_c": b["tunnel_exit_temp_c"],
                })

                if b["mass_kg_remaining"] <= 1e-9:
                    b["final_dispatch_ts"] = ts
                    batch_rows.append(b.copy())
                    active_batches.pop(0)

        # -----------------------------
        # END-OF-DAY FORCE CLEAR
        # -----------------------------
        # simple: detect last timestep of the day
        if i < n - 1:
            next_day = dt_arr[i + 1].astype("datetime64[D]")
            current_day = ts.astype("datetime64[D]")

            if next_day > current_day:
                # day ended → clear remaining batches
                while active_batches:
                    b = active_batches[0]

                    if pd.isna(b["first_dispatch_ts"]):
                        b["first_dispatch_ts"] = ts

                    event_rows.append({
                        "plant_id": plant_id,
                        "timestamp": ts,
                        "event_type": "dispatch_eod",
                        "batch_id": b["batch_id"],
                        "mass_kg": b["mass_kg_remaining"],
                        "fruit_type": b["fruit_type"],
                        "quality_grade": b["quality_grade"],
                        "tunnel_exit_temp_c": b["tunnel_exit_temp_c"],
                    })

                    b["final_dispatch_ts"] = ts
                    batch_rows.append(b.copy())
                    active_batches.pop(0)

                # for b in active_batches:
                #     b_copy = b.copy()
                #     b_copy["final_dispatch_ts"] = pd.NaT
                #     batch_rows.append(b_copy)
    # -----------------------------
    # Convert to DataFrames
    # -----------------------------
    events_df = pd.DataFrame(event_rows)
    batches_df = pd.DataFrame(batch_rows)

    return events_df, batches_df, active_batches, next_batch_id


@njit
def run_simulation_chunk(
    T_ambient_arr,
    RH_ambient_arr,
    P_arr,
    GHI_arr,
    WS_arr,
    T2MDEW_arr,
    T_ground_arr,

    hour_arr,
    minute_arr,
    second_arr,
    doy_arr,
    month_arr,
    year_arr,

    # params (only what you need)
    setpoint,
    deadband,
    tau_cool,
    tau_sensor,
    dt_internal,
    R_const,
    M_CO2,
    M_air,
    target_rh,
    rh_deadband,
    tau_humid_frac,
    tau_humid_sensor,
    m_max,
    f_evap_humid,
    tau_condense,
    L,
    W,
    H,
    alpha_roof,
    k_fans,
    U_wall,
    U_floor,
    # T_ground,
    Q_rated,
    f_structure,
    k_wind_U,
    Cp_air,
    T_lookup,
    R_lookup,
    BF,
    T_coil_ref,
    n_ach_eff_per_sec,

    h_respiration,
    tunnel_exit_fruit_temp,
    Cp_fruit,
    # chosen zone only; do not keep the whole k_by_zone dict
    k_zone,   # cold_storage
    k_p,
    f_min,
    rho_load_bulk,

    CO2_outdoor_ppm,
    O2_outdoor_pct,

    cooling_call_init,
    cooling_frac_init,
    humidifier_call_init,
    humidifier_frac_init,
    condense_frac_init,

    fruit_mass_kg_init,
    total_water_loss_kg_init,
    eta_init,

    float64_dict,
    int8_dict,

    T_plant_a,
    T_plant_b,
    dT_plant,
    tau_plant,
    RH_plant,

    tau_door_ext,
    tau_door_int,
    W_door_ext,
    H_door_ext,
    W_door_int,
    H_door_int,
    td_coeff,

    door_ext_schedule,
    door_int_schedule,
    fruit_mass_delta_kg,
    incoming_temperature_arr,

    k_door_ext,
    k_door_int,

    V_free_min,
    m_air_room_min,

    timestep,

    m_dot_evap_air_kg_s,

    eps,

    h_i_walls,
    h_i_roof,

    # fruit_type,

    seed,
    ):

    # Cp_fruit = FRUIT_CONFIGS[fruit_type]["Cp_fruit"]
    # setpoint = FRUIT_CONFIGS[fruit_type]["setpoint"]
    # k_p = FRUIT_CONFIGS[fruit_type]["k_p"]
    # target_rh = FRUIT_CONFIGS[fruit_type]["target_rh"]
    # tunnel_exit_fruit_temp = FRUIT_CONFIGS[fruit_type]["tunnel_exit_fruit_temp"]

    new_seed = (seed + int(hour_arr[0]) + int(minute_arr[0])) * int(month_arr[0]) * int(year_arr[0])
    if int(doy_arr[0]) == 1:
        new_seed *= 13
    else:
        new_seed *= int(doy_arr[0])
    np.random.seed(new_seed)
    n = len(T_ambient_arr)

    cooling_frac = cooling_frac_init
    # A_walls_ceiling = 2 * (L * H) + 2 * (W * H) + (L * W)

    # Internal wall connected to the plant
    A_wall_internal = W * H
    # External walls (3 walls + ceiling)
    A_walls_external = (2 * L * H) + (W * H) # + (L * W)
    A_roof = L * W
    A_floor = A_roof
    # Structure thermal mass (always present, empty or not)
    C_floor = A_floor * 0.15 * 2300 * 880        # 15cm concrete slab
    C_panels = (A_wall_internal + A_walls_external) * 0.12 * 40 * 1400 # 12cm PIR panels
    C_structure = f_structure*(C_floor + C_panels)

    V_room = L * W * H

    A_door_ext = W_door_ext*H_door_ext
    A_door_int = W_door_int*H_door_int

    cooling_call = cooling_call_init
    cooling_frac = cooling_frac_init
    humidifier_call = humidifier_call_init
    humidifier_frac = humidifier_frac_init
    condense_frac = condense_frac_init

    fruit_mass_floor_kg = 1e-9 # 50.0
    fruit_mass_kg_init = max(fruit_mass_kg_init, fruit_mass_floor_kg)
    fruit_mass_kg = fruit_mass_kg_init

    total_water_loss_kg = total_water_loss_kg_init
    eta = eta_init

    CO2_ppm = CO2_outdoor_ppm
    O2_pct = O2_outdoor_pct


    T_pulp = manual_clipping(
                np.random.normal(tunnel_exit_fruit_temp + 1, 0.4),
                tunnel_exit_fruit_temp - 0.5,
                tunnel_exit_fruit_temp + 2.0
            )
    T_room = setpoint
    T_sensor = T_room
    P_sat_room_start = p_sat_magnus(T_room) # Magnus-Tetens Approximation
    P_w_room_start = target_rh*P_sat_room_start

    P_0 = P_arr[0]
    W_room = w_from_partial_pressure(P_w_room_start, P_0)
    P_w_room = partial_pressure_from_w(W_room, P_0)
    # P_sat_room = 610.78 * np.exp(17.27 * T_room / (T_room + 237.3))
    RH_room = manual_clipping(P_w_room / P_sat_room_start, 0.0, 1.0)
    RH_room_sensor = RH_room
    #####

    V_load_eff = fruit_mass_kg / rho_load_bulk
    f_free = 1.0 - V_load_eff / V_room
    f_free = manual_clipping(f_free, f_min, 1.0)
    V_free = f_free * V_room
    # V_free_floor = 0.0
    V_free = max(V_free, V_free_min)
    V_free_liters = V_free * 1000.0

    C_fruit = fruit_mass_kg * Cp_fruit

    steps_per_min = timestep // dt_internal
    n_total = int(n * steps_per_min)

    R_dry = R_const/(M_air*1e-3)

    T, R, T_min, T_max, T_lo, T_hi, T_ref, Q_10, R_ref = get_fruit_resp_params(T_lookup, R_lookup)

    T_room_arr = np.empty(n_total, dtype=np.float64)
    T_sensor_arr = np.empty(n_total, dtype=np.float64)
    cooling_frac_arr = np.empty(n_total, dtype=np.float64)

    CO2_ppm_arr = np.empty(n_total, dtype=np.float64)
    O2_pct_arr = np.empty(n_total, dtype=np.float64)

    T_pulp_arr = np.empty(n_total, dtype=np.float64)
    weight_loss_pct_arr = np.empty(n_total, dtype=np.float64)

    m_humidifier_arr = np.empty(n_total, dtype=np.float64)

    W_room_arr = np.empty(n_total, dtype=np.float64)
    RH_room_arr = np.empty(n_total, dtype=np.float64)
    RH_room_sensor_arr = np.empty(n_total, dtype=np.float64)
    m_transp_rate_arr = np.empty(n_total, dtype=np.float64)

    P_sat_pulp_arr = np.empty(n_total, dtype=np.float64)
    P_w_room_arr = np.empty(n_total, dtype=np.float64)

    m_removed_arr = np.empty(n_total, dtype=np.float64)

    humidifier_frac_arr = np.empty(n_total, dtype=np.float64)

    COP_arr = np.empty(n_total, dtype=np.float64)
    W_compressor_kw_arr = np.empty(n_total, dtype=np.float64)
    Q_condenser_kw_arr = np.empty(n_total, dtype=np.float64)
    Q_cooling_w_arr = np.empty(n_total, dtype=np.float64)

    T_evap_in_air_arr = np.empty(n_total, dtype=np.float64)
    T_evap_out_air_arr = np.empty(n_total, dtype=np.float64)
    W_evap_in_arr = np.empty(n_total, dtype=np.float64)
    W_evap_out_arr = np.empty(n_total, dtype=np.float64)
    m_dot_evap_air_arr = np.empty(n_total, dtype=np.float64)
    RH_evap_out_arr = np.empty(n_total, dtype=np.float64)

    fruit_mass_kg_arr = np.empty(n_total, dtype=np.float64)

    # ints (compact, Numba-friendly)
    cooling_call_arr = np.empty(n_total, dtype=np.int8)
    humidifier_call_arr = np.empty(n_total, dtype=np.int8)

    T_target = T_plant_a + T_plant_b*T_ambient_arr[0]
    T_plant = T_target
    door_ext_open_fraction = 0.0
    door_int_open_fraction = 0.0

    

    for i in range(n):
        T_ambient = T_ambient_arr[i]
        RH_ambient = RH_ambient_arr[i]
        P_Pa = P_arr[i]
        GHI = GHI_arr[i]
        wind_speed = WS_arr[i]
        T2MDEW = T2MDEW_arr[i]
        T_ground = T_ground_arr[i]

        if hour_arr[i] == 0 and minute_arr[i] == 0 and second_arr[i] == 0:
            eta = manual_clipping(np.random.normal(0.55, 0.02), 0.40, 0.70)
            total_water_loss_kg = 0.0

        delta_mass = fruit_mass_delta_kg[i]

        old_mass = fruit_mass_kg

        if delta_mass > 0.0:
            incoming_temp = incoming_temperature_arr[i]
            if np.isnan(incoming_temp):
                incoming_temp = incoming_temperature_arr[i-1]
            new_mass = old_mass + delta_mass
            if new_mass > fruit_mass_floor_kg:
                T_pulp = (old_mass * T_pulp + delta_mass * incoming_temp) / new_mass
            fruit_mass_kg = new_mass

        elif delta_mass < 0.0:
            max_removal = fruit_mass_kg - fruit_mass_floor_kg
            if max_removal < 0.0:
                max_removal = 0.0
            if -delta_mass > max_removal:
                delta_mass = -max_removal
            fruit_mass_kg += delta_mass

        fruit_mass_kg = max(fruit_mass_kg, fruit_mass_floor_kg)

        V_load_eff = fruit_mass_kg / rho_load_bulk
        f_free = 1.0 - V_load_eff / V_room
        f_free = manual_clipping(f_free, f_min, 1.0)

        V_free = f_free * V_room
        V_free = max(V_free, V_free_min)
        V_free_liters = V_free * 1000.0

        C_fruit = max(fruit_mass_kg * Cp_fruit, 1e4)

        if door_ext_open_fraction <= eps and door_int_open_fraction <= eps:
            leak_noise_factor = manual_clipping(np.random.normal(loc=1.0, scale=0.1), 0.85, 1.15)

        for j in range(steps_per_min):
            k = int(i * steps_per_min + j)
            mdot_door_ext = 0.0
            mdot_door_int = 0.0
            # =========================================================
            # 1. CURRENT-STATE PSYCHROMETRICS / AIR PROPERTIES
            # =========================================================
            # ambient psychrometrics
            P_sat_ambient = p_sat_magnus(T_ambient)
            P_w_ambient = RH_ambient * P_sat_ambient
            W_ambient = w_from_partial_pressure(P_w_ambient, P_Pa)

            # plant psychrometrics
            P_sat_plant = p_sat_magnus(T_plant)
            P_w_plant = RH_plant * P_sat_plant
            W_plant = w_from_partial_pressure(P_w_plant, P_Pa)

            # moist-air gas constants / densities
            R_moist_room = R_dry * (1 + 0.608 * W_room)
            R_moist_ambient = R_dry * (1 + 0.608 * W_ambient)
            R_moist_plant = R_dry * (1 + 0.608 * W_plant)

            rho_air_room = P_Pa / (R_moist_room * (T_room + 273.15))
            rho_air_ambient = P_Pa / (R_moist_ambient * (T_ambient + 273.15))
            rho_air_plant = P_Pa / (R_moist_plant * (T_plant + 273.15))

            # =========================================================
            # 2. EVENT / DISTURBANCE LOGIC
            # =========================================================

            # =========================================================
            # 3. CONTROL LOGIC
            # =========================================================
            # cooling controller from lagged temp sensor
            if T_sensor > setpoint + deadband:
                cooling_call = True
            elif T_sensor < setpoint - deadband:
                cooling_call = False

            target_cooling = 1.0 if cooling_call else 0.0
            cooling_frac += (target_cooling - cooling_frac) * dt_internal / tau_cool
            cooling_frac = manual_clipping(cooling_frac, 0.0, 1.0)

            # humidity controller from lagged RH sensor
            if cooling_call:
                rh_deadband_eff = rh_deadband * 0.5 # Tighten control during cooling
            else:
                rh_deadband_eff = rh_deadband

            if RH_room_sensor < (target_rh - rh_deadband_eff *.33):
                humidifier_call = True
            elif RH_room_sensor > (target_rh + rh_deadband_eff):
                humidifier_call = False

            target_humid = 1.0 if humidifier_call else 0.0
            humidifier_frac += (target_humid - humidifier_frac) * dt_internal / tau_humid_frac
            humidifier_frac = manual_clipping(humidifier_frac, 0.0, 1.0)

            # =========================================================
            # 4. HVAC / ROOM LOAD TERMS
            # =========================================================
            h_fg = (2501 - 2.361 * T_room) * 1000.0

            # humidifier
            m_humidifier = m_max * humidifier_frac
            Q_humidifier_cooling = f_evap_humid*m_humidifier*h_fg

            # room envelope / fan / solar
            # f_wind = 1.0 + k_wind_U * wind_speed
            h_o = 5.7 + 3.8 * wind_speed
            T_sol_air = T_ambient + (alpha_roof * GHI) / h_o
            
            # Q_walls_ext = f_wind * U_wall * A_walls_external * (T_ambient - T_room)
            U_wall_ext_eff = 1.0 / (1.0/h_o + 1.0/U_wall + 1.0/h_i_walls)  # h_i ≈ 8 W/m²·°C internal
            U_roof_eff = 1.0 / (1.0/h_o + 1.0/U_wall + 1.0/h_i_roof)    # h_i ≈ 5 W/m²·°C internal

            Q_walls_ext = U_wall_ext_eff * A_walls_external * (T_ambient - T_room)
            Q_roof = U_roof_eff * A_roof * (T_sol_air - T_room) 

            U_wall_int_eff = 1.0 / (1.0/h_i_walls + 1.0/U_wall + 1.0/h_i_walls)
            Q_walls_int = U_wall_int_eff * A_wall_internal * (T_plant - T_room)
            Q_floor = U_floor * A_floor * (T_ground - T_room)
            Q_fans = k_fans * Q_rated
            # Q_solar = alpha_roof * GHI * A_floor

            # cooling
            # outputs_dict_empty, calls_dict_empty = create_numba_dicts()
            Q_cooling_actual = Q_rated * (1.0 - 0.007 * (T_ambient - 35.0))
            Q_cooling = Q_cooling_actual * cooling_frac

            # optional compressor metrics if needed for output only
            Q_total_evap = Q_cooling
            COP_actual, W_compressor_kw, Q_condenser_kw = compressor_metrics(T_room, T_ambient, eta, Q_total_evap)

            # coil latent removal
            T_coil = T_coil_ref + BF * (T_room - T_coil_ref)
            P_sat_coil = p_sat_magnus(T_coil)
            W_coil_sat = w_from_partial_pressure(P_sat_coil, P_Pa)

            # W_evap_out = BF * W_room + (1.0 - BF) * W_coil_sat ## Added for 'industrial' COP calculation

            if cooling_frac > 0.0 and W_room > W_coil_sat:
                condense_call = True
            else:
                condense_call = False

            target_condense = 1.0 if condense_call else 0.0
            condense_frac += (target_condense - condense_frac) * dt_internal / tau_condense
            condense_frac = manual_clipping(condense_frac, 0.0, 1.0)

            m_removed = condense_frac * (1.0 - BF) * (Q_cooling / h_fg)
            Q_latent_removed = m_removed * h_fg # Energy "spent" on turning vapor into liquid water
            Q_sensible_cooling = max(Q_total_evap - Q_latent_removed, 0.0) # Energy "left over" to actually cool the air

            # effective room air capacity
            C_air = V_free * rho_air_room * Cp_air
            C_room = C_air + C_structure
            m_air_room = V_free * rho_air_room
            m_air_room = max(m_air_room, m_air_room_min)

            mdot_leak = n_ach_eff_per_sec * m_air_room

            # 1. External Door Logic
            target_door_ext = 1.0 if door_ext_schedule[i] else 0.0
            # Only run math if the door is open or moving (fraction > 0)
            if target_door_ext > 0 or door_ext_open_fraction > 0:
                Q_total, rho_source = calculate_door_infiltration_gosney(
                    T_room=T_room,
                    T_source=T_ambient,  # Pass T_ambient OR T_plant here
                    P_Pa=P_Pa,
                    WS2M=wind_speed,
                    A_door=A_door_ext,    # Pass A_door_ext OR A_door_int
                    H_door=H_door_ext,    # Pass H_door_ext OR H_door_int
                    R_dry=R_dry,
                    is_outdoor_door=True,
                    )
                door_ext_open_fraction += (target_door_ext - door_ext_open_fraction) * dt_internal / tau_door_ext
                door_ext_open_fraction = manual_clipping(door_ext_open_fraction, 0.0, 1.0)
                mdot_door_ext = k_door_ext * Q_total * rho_source * door_ext_open_fraction

            # 2. Internal Door Logic
            target_door_int = 1.0 if door_int_schedule[i] else 0.0
            if target_door_int > 0 or door_int_open_fraction > 0:
                Q_total, rho_source = calculate_door_infiltration_gosney(
                    T_room=T_room,
                    T_source=T_plant,  # Pass T_ambient OR T_plant here
                    P_Pa=P_Pa,
                    WS2M=wind_speed,
                    A_door=A_door_int,    # Pass A_door_ext OR A_door_int
                    H_door=H_door_int,    # Pass H_door_ext OR H_door_int
                    R_dry=R_dry,
                    is_outdoor_door=False,
                    )
                door_int_open_fraction += (target_door_int - door_int_open_fraction) * dt_internal / tau_door_int
                door_int_open_fraction = manual_clipping(door_int_open_fraction, 0.0, 1.0)
                mdot_door_int = k_door_int * Q_total * rho_source * door_int_open_fraction

            if door_ext_open_fraction > eps and door_int_open_fraction > eps:
                # A common engineering heuristic is that cross-flow
                # increases exchange by ~20-50% depending on wind.
                penalty_scale = max(door_ext_open_fraction, door_int_open_fraction) # 1.3
                cross_flow_factor = 1.0 + (td_coeff * penalty_scale)
                mdot_total = mdot_leak + (mdot_door_ext + mdot_door_int) * cross_flow_factor
            elif door_ext_open_fraction <= eps and door_int_open_fraction <= eps:
                mdot_total = mdot_leak*leak_noise_factor + mdot_door_ext + mdot_door_int
            else:
                mdot_total = mdot_leak + mdot_door_ext + mdot_door_int

            # mdot_total = mdot_leak + mdot_door_ext + mdot_door_int

            f_exchange_sec = mdot_total / m_air_room

            # infiltration loads using effective ACH
            # Q_infiltration_latent = n_ach_eff_per_sec * V_room * rho_air_ambient * h_fg * (W_ambient - W_room)

            Q_inf_sens_ext = (mdot_leak + mdot_door_ext) * Cp_air * (T_ambient - T_room)
            Q_inf_sens_int = mdot_door_int * Cp_air * (T_plant - T_room)

            # =========================================================
            # 5. FRUIT SOURCE / SINK TERMS
            # =========================================================
            # respiration rate from clipped LOCAL temp, do not mutate T_pulp here
            R_fruit = get_r_fruit(
                T_pulp, T, R,
                T_min, T_max, T_lo, T_hi, T_ref, R_ref, Q_10
            )

            Q_respiration = R_fruit * fruit_mass_kg * h_respiration * (1.0 / 3600.0)

            Q_fruit_exchange = C_fruit * k_zone * (T_pulp - T_room)

            P_sat_pulp = p_sat_magnus(T_pulp)
            m_transp_rate = max(k_p * fruit_mass_kg * (P_sat_pulp - P_w_room), 0.0)
            Q_evap_fruit = m_transp_rate * h_fg

            # =========================================================
            # 6. RATE EQUATIONS
            # =========================================================
            heat_balance = (
                Q_walls_ext + Q_walls_int + Q_roof
                + Q_floor
                + Q_fans
                + Q_fruit_exchange
                # + Q_solar
                + Q_inf_sens_ext + Q_inf_sens_int
                - Q_sensible_cooling
                - Q_humidifier_cooling
            ) / C_room

            # gas rates
            R_CO2_mL = (R_fruit * 1e-3) / M_CO2 * (R_const * (T_pulp + 273.15) / P_Pa * 1e6)
            CO2_prod_ppm_per_hr = R_CO2_mL * fruit_mass_kg / V_free_liters * 1e3


            dT_CO2_ppm = (
                CO2_prod_ppm_per_hr/3600.0
                + f_exchange_sec * (CO2_outdoor_ppm - CO2_ppm)
            ) * dt_internal

            O2_cons_pct_per_hr = CO2_prod_ppm_per_hr / 1e4
            dT_O2_pct = (
                - O2_cons_pct_per_hr/3600.0
                + f_exchange_sec * (O2_outdoor_pct - O2_pct)
            ) * dt_internal

            # moisture balance
            dm_water = (
                (mdot_leak + mdot_door_ext) * (W_ambient - W_room) # Ambient source
                + (mdot_door_int) * (W_plant - W_room) # Plant source
                + m_transp_rate
                + m_humidifier
                - m_removed
            ) * dt_internal

            # fruit temperature rate
            dT_pulp = (
                - k_zone * (T_pulp - T_room)
                + Q_respiration / C_fruit
                - Q_evap_fruit / C_fruit
            ) * dt_internal

            # =========================================================
            # 7. STATE UPDATES
            # =========================================================
            CO2_ppm += dT_CO2_ppm
            O2_pct += dT_O2_pct

            total_water_loss_kg += m_transp_rate * dt_internal

            fruit_mass_kg -= m_transp_rate * dt_internal
            fruit_mass_kg = max(fruit_mass_kg, fruit_mass_floor_kg)
            weight_loss_pct = (total_water_loss_kg / max(fruit_mass_kg_init, 1e-6)) * 100.0


            V_load_eff = fruit_mass_kg / rho_load_bulk
            f_free = 1.0 - V_load_eff / V_room
            f_free = manual_clipping(f_free, f_min, 1.0)
            V_free = f_free * V_room
            V_free = max(V_free, V_free_min)
            V_free_liters = V_free * 1000.0

            C_fruit = max(fruit_mass_kg * Cp_fruit, 1e4)

            T_pulp += dT_pulp

            dW = dm_water / m_air_room
            # dW = manual_clipping(dW, -0.01, 0.01)
            W_room += dW
            W_room = max(W_room, 0.0)

            T_room += heat_balance * dt_internal

            # =========================================================
            # 8. RECOMPUTE DERIVED ROOM HUMIDITY
            # =========================================================
            P_w_room = partial_pressure_from_w(W_room, P_Pa)
            P_sat_room = p_sat_magnus(T_room)
            RH_room = manual_clipping(P_w_room / P_sat_room, 0.0, 1.0)

            # =========================================================
            # 9. SENSOR UPDATES
            # =========================================================
            T_sensor += (T_room - T_sensor) * dt_internal / tau_sensor
            RH_room_sensor += (RH_room - RH_room_sensor) * dt_internal / tau_humid_sensor

            # =========================================================
            # 10. OUTPUT WRITES
            # =========================================================
            # write true values and/or noisy sensor values to arrays
            T_room_arr[k] = T_room
            T_sensor_arr[k] = T_sensor
            cooling_frac_arr[k] = cooling_frac

            CO2_ppm_arr[k] = CO2_ppm
            O2_pct_arr[k] = O2_pct

            T_pulp_arr[k] = T_pulp
            weight_loss_pct_arr[k] = weight_loss_pct

            m_humidifier_arr[k] = m_humidifier

            W_room_arr[k] = W_room
            RH_room_arr[k] = RH_room
            RH_room_sensor_arr[k] = RH_room_sensor
            m_transp_rate_arr[k] = m_transp_rate

            P_sat_pulp_arr[k] = P_sat_pulp
            P_w_room_arr[k] = P_w_room

            m_removed_arr[k] = m_removed

            humidifier_frac_arr[k] = humidifier_frac

            COP_arr[k] = COP_actual
            W_compressor_kw_arr[k] = W_compressor_kw
            Q_condenser_kw_arr[k] = Q_condenser_kw
            Q_cooling_w_arr[k] = Q_cooling

            T_evap_in_air_arr[k] = T_room
            T_evap_out_air_arr[k] = T_coil
            W_evap_in_arr[k] = W_room
            W_evap_out_arr[k] = BF * W_room + (1.0 - BF) * W_coil_sat
            m_dot_evap_air_arr[k] = m_dot_evap_air_kg_s

            RH_evap_out_arr[k] = manual_clipping(
                partial_pressure_from_w(BF * W_room + (1.0 - BF) * W_coil_sat, P_Pa) / p_sat_magnus(T_coil),
                0.0,
                1.0
            )

            fruit_mass_kg_arr[k] = fruit_mass_kg

            # ints
            cooling_call_arr[k] = int(cooling_call)
            humidifier_call_arr[k] = int(humidifier_call)

        T_target = T_plant_a + T_plant_b*T_ambient
        T_plant += (dT_plant/tau_plant) * (T_target - T_plant)

    float64_dict['T_room'] = T_room_arr
    float64_dict['T_sensor'] = T_sensor_arr
    float64_dict['cooling_frac'] = cooling_frac_arr

    float64_dict['CO2_ppm'] = CO2_ppm_arr
    float64_dict['O2_pct'] = O2_pct_arr

    float64_dict['T_pulp'] = T_pulp_arr
    float64_dict['weight_loss_pct'] = weight_loss_pct_arr

    float64_dict['m_humidifier'] = m_humidifier_arr

    float64_dict['W_room'] = W_room_arr
    float64_dict['RH_room'] = RH_room_arr
    float64_dict['RH_room_sensor'] = RH_room_sensor_arr
    float64_dict['m_transp_rate'] = m_transp_rate_arr

    float64_dict['P_sat_pulp'] = P_sat_pulp_arr
    float64_dict['P_w_room'] = P_w_room_arr

    float64_dict['m_removed'] = m_removed_arr

    float64_dict['humidifier_frac'] = humidifier_frac_arr

    float64_dict['COP'] = COP_arr
    float64_dict['W_compressor_kw'] = W_compressor_kw_arr
    float64_dict['Q_condenser_kw'] = Q_condenser_kw_arr
    float64_dict['Q_cooling_w'] = Q_cooling_w_arr

    float64_dict['T_evap_in_air'] = T_evap_in_air_arr
    float64_dict['T_evap_out_air'] = T_evap_out_air_arr
    float64_dict['W_evap_in'] = W_evap_in_arr
    float64_dict['W_evap_out'] = W_evap_out_arr
    float64_dict['m_dot_evap_air_kg_s'] = m_dot_evap_air_arr
    float64_dict['RH_evap_out'] = RH_evap_out_arr

    float64_dict["fruit_mass_kg"] = fruit_mass_kg_arr

    int8_dict['cooling_call'] = cooling_call_arr
    int8_dict['humidifier_call'] = humidifier_call_arr

    return float64_dict, int8_dict

def expand_minute_timestamps_to_internal(dt_arr, dt_internal):
    offsets = np.arange(0, 60, int(dt_internal), dtype="timedelta64[s]")
    # offsets = np.arange(0, 60, dt_internal, dtype="timedelta64[s]")
    expanded = (dt_arr[:, None] + offsets[None, :]).reshape(-1)
    return expanded

def build_telemetry_table(
    plant_id,
    telemetry_dt_arr,
    outputs_dict,
    calls_dict,
    f_RH_noise,
    fruit_type,
    seed=42,
):
    datetimes = pd.to_datetime(telemetry_dt_arr)

    n = len(datetimes)
    assert len(outputs_dict['T_sensor']) == n, f"T_sensor {len(outputs_dict['T_sensor'])} != datetime {n}"
    assert len(outputs_dict['T_pulp']) == n, f"T_pulp {len(outputs_dict['T_pulp'])} != datetime {n}"
    assert len(calls_dict['cooling_call']) == n, f"cooling_call {len(calls_dict['cooling_call'])} != datetime {n}"

    ## Add noise to simulation outputs
    new_seed = (telemetry_dt_arr[0].astype('int64') + telemetry_dt_arr[5].astype('int64')) + seed
    rng = np.random.default_rng(new_seed)

    cooling_call = calls_dict['cooling_call']
    humidifier_call = calls_dict['humidifier_call']

    T_room = outputs_dict['T_sensor']
    sigma_T_room = np.where(cooling_call == 1, 0.08, 0.15)
    T_room_noisy = T_room + rng.normal(0.0, sigma_T_room)

    T_pulp = outputs_dict['T_pulp']
    sigma_T_pulp = .04 # 0.15
    T_pulp_noisy = T_pulp + rng.normal(0.0, sigma_T_pulp, size=T_pulp.shape)

    RH_room_frac = outputs_dict['RH_room_sensor']
    RH_room_pct = RH_room_frac * 100.0
    sigma_RH_room_pct = 0.5 + f_RH_noise * np.maximum(0.0, (RH_room_pct - 85.0) / 15.0)
    RH_room_noisy_pct = RH_room_pct + rng.normal(0.0, sigma_RH_room_pct, size=RH_room_pct.shape)
    RH_room_noisy = np.clip(RH_room_noisy_pct / 100.0, 0.0, 1.0)

    CO2_ppm = outputs_dict['CO2_ppm']
    sigma_CO2_ppm = 0.005*CO2_ppm + 1.0
    CO2_ppm_noisy = CO2_ppm + rng.normal(0.0, sigma_CO2_ppm, size=CO2_ppm.shape)
    CO2_ppm_noisy = np.maximum(CO2_ppm_noisy, 0.0)

    O2_pct = outputs_dict['O2_pct']
    sigma_O2_pct = 0.05
    O2_pct_noisy = O2_pct + rng.normal(0.0, sigma_O2_pct, size=O2_pct.shape)
    O2_pct_noisy = np.maximum(O2_pct_noisy, 0.0)

    W_compressor_kw = outputs_dict['W_compressor_kw']
    sigma_W_compressor_kw = np.maximum(0.005 * W_compressor_kw, 0.02)
    W_compressor_kw_noisy = W_compressor_kw + rng.normal(0.0, sigma_W_compressor_kw, size=W_compressor_kw.shape)
    W_compressor_kw_noisy = np.maximum(W_compressor_kw_noisy, 0.0)

    T_evap_in_air = outputs_dict['T_evap_in_air']
    sigma_T_evap_in_air = np.where(cooling_call == 1, 0.08, 0.15)
    T_evap_in_air_noisy = T_evap_in_air + rng.normal(0.0, sigma_T_evap_in_air, size=T_evap_in_air.shape)

    T_evap_out_air = outputs_dict['T_evap_out_air']
    sigma_T_evap_out_air = np.where(cooling_call == 1, 0.10, 0.18)
    T_evap_out_air_noisy = T_evap_out_air + rng.normal(0.0, sigma_T_evap_out_air, size=T_evap_out_air.shape)

    m_dot_evap_air_kg_s = outputs_dict['m_dot_evap_air_kg_s']
    sigma_m_dot_evap_air = np.maximum(0.03 * m_dot_evap_air_kg_s, 0.05)
    m_dot_evap_air_kg_s_noisy = (
        m_dot_evap_air_kg_s
        + rng.normal(0.0, sigma_m_dot_evap_air, size=m_dot_evap_air_kg_s.shape)
    )
    m_dot_evap_air_kg_s_noisy = np.maximum(m_dot_evap_air_kg_s_noisy, 0.1)

    RH_evap_out = outputs_dict['RH_evap_out']
    sigma_RH_evap_out = 0.5 + f_RH_noise * np.maximum(0, (RH_evap_out * 100.0 - 85.0)/15.0)
    RH_evap_out_noisy = RH_evap_out + rng.normal(0.0, sigma_RH_evap_out / 100.0, size=RH_evap_out.shape)
    RH_evap_out_noisy = np.clip(RH_evap_out_noisy, 0.0, 1.0)

    telemetry_df = pd.DataFrame({
        'plant_id': [plant_id] * n,
        'datetime': datetimes,

        'T_room': T_room_noisy,
        'T_pulp': T_pulp_noisy,
        'RH_room': RH_room_noisy,
        'CO2_ppm': CO2_ppm_noisy,
        'O2_pct': O2_pct_noisy,
        'W_compressor_kw': W_compressor_kw_noisy,

        'T_evap_in_air': T_evap_in_air_noisy,
        'T_evap_out_air': T_evap_out_air_noisy,
        'RH_evap_out': RH_evap_out_noisy,
        'm_dot_evap_air_kg_s': m_dot_evap_air_kg_s_noisy,

        'cooling_call': cooling_call,
        'humidifier_call': humidifier_call,

        'fruit_type': fruit_type,
    })

    telemetry_df = telemetry_df.iloc[::10].reset_index(drop=True)

    return telemetry_df
