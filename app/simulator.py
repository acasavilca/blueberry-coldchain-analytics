import os
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
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from timezonefinder import TimezoneFinder
from config import MISSING_TSOIL_54CM, WEATHER_DATASET_DTYPES
from utils import get_persistent_session
from respiration_data import *
# import uuid

SAFE_FLOOR = 1e-30
MASS_KG_FLOOR = 1e-9
SENTINEL_FLOAT64 = -999.0
SENTINEL_INT64 = -999
SENTINEL_FLOAT8 = -1.0
SENTINEL_INT8 = -1

_SESSION = get_persistent_session()

def enforce_datatypes():
    raise

def get_timezone_name(latitude, longitude):
    tf = TimezoneFinder()
    return tf.timezone_at(lng=longitude, lat=latitude) 

def get_timezone_transitions(year, zone_name):
    tz = ZoneInfo(zone_name)
    for d in range(365):
        dt = datetime(year, 1, 1) + timedelta(days=d)
        if dt.replace(tzinfo=tz).dst() != (dt + timedelta(days=1)).replace(tzinfo=tz).dst():
            print(f"Transition on: {dt.date()}")

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


ELEVATION_CACHE_PATH = Path("elevation_cache.json")
def fetch_elevation_from_api(latitude: float, longitude: float) -> float:
    url = "https://api.open-meteo.com/v1/elevation"
    params = {"latitude": latitude, "longitude": longitude}
    r = _SESSION.get(url, params=params, timeout=30)
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

def format_dates_for_data_retrieval(
    start_dt_str,
    end_dt_str,
    input_time_grain="hourly",
    inclusive_end=True,
):
    start_dt, end_dt = pd.to_datetime(start_dt_str), pd.to_datetime(end_dt_str)

    if input_time_grain == "hourly":
        start_dt = start_dt.floor("h")
        end_dt = end_dt.floor("h")
        if inclusive_end:
            end_dt += pd.Timedelta(hours=1)

    elif input_time_grain == "daily":
        start_dt = start_dt.floor("D")
        end_dt = end_dt.floor("D")
        if inclusive_end:
            end_dt += pd.Timedelta(days=1)

    elif input_time_grain == "weekly":
        # Floors to the nearest Monday
        start_dt = start_dt.floor("W")
        end_dt = end_dt.floor("W")
        if inclusive_end:
            end_dt += pd.Timedelta(weeks=1)

    elif input_time_grain == "monthly":
        # Floors to the 1st day of the month
        start_dt = start_dt.to_period("M").to_timestamp()
        end_dt = end_dt.to_period("M").to_timestamp()
        if inclusive_end:
            end_dt += pd.offsets.MonthEnd(1) + pd.Timedelta(days=1)

    start_epoch = start_dt.timestamp()
    end_epoch = end_dt.timestamp()

    return start_epoch, end_epoch

def retrieve_satellite_data(
    rng,
    start_hour_epoch,
    end_hour_epoch,
    latitude,
    longitude,
    dtypes,
    measurements,
    timezone_param,
    frequency="hourly",
    is_backfill=False,
    earliest_date_with_available_data='2023-03-23',
):
    needs_clipping = False
    location_id = encode_coordinates(latitude, longitude)
    
    historical = "https://historical-forecast-api.open-meteo.com/v1/forecast"
    forecast ="https://api.open-meteo.com/v1/forecast"

    # start_dt, end_dt = pd.to_datetime(start_dt), pd.to_datetime(end_dt)

    # if input_time_grain == "daily":
    #     next_day_midnight = (end_dt + pd.Timedelta(days=1)).normalize()
    #     end_dt = next_day_midnight

    # elif input_time_grain == "hourly":
    #     next_hour = (end_dt + pd.Timedelta(hours=1))
    #     end_dt = next_hour

    # else:
    #     print("Please enter valid input_time_grain ('hourly' or 'daily')")
    #     raise

    # today = pd.Timestamp.today().normalize()
    # start_dt_norm = start_dt.normalize()
    # end_dt_norm = end_dt.normalize()
    # days_since_start_sim = (today - start_dt_norm).days
    # days_since_end_sim = (today - end_dt_norm).days

    earliest_epoch_with_available_data = pd.to_datetime(earliest_date_with_available_data).timestamp()

    today_epoch_midnight = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    start_epoch_midnight = (start_hour_epoch // 86400) * 86400
    end_epoch_midnight = (end_hour_epoch // 86400) * 86400

    days_since_start_sim = (today_epoch_midnight - start_epoch_midnight) // 86400
    days_since_end_sim = (today_epoch_midnight - end_epoch_midnight) // 86400
    
    if days_since_start_sim <= 60 and days_since_end_sim <= 60:
        base_url = forecast
    elif days_since_start_sim > 60 and days_since_end_sim > 60:
        base_url = historical
    else:
        base_url = historical
    
    if days_since_end_sim < 4 and base_url == historical:
        print("Please select an end date at least 4 days in the past to ensure historical data availability, or select more recent dates to use forecast data.")
        sys.exit("Error: Exiting program.")

    if start_epoch_midnight < earliest_epoch_with_available_data or end_epoch_midnight < earliest_epoch_with_available_data:
        print("Please select start and end dates from 2021-03-23 onward to ensure data availability, or select more recent dates to use forecast data.")
        sys.exit("Error: Exiting program.")

    print(f"Using {base_url} endpoint based on selected date range and current date.")
    
    start_hour = datetime.fromtimestamp(start_hour_epoch, tz=timezone.utc)
    end_hour = datetime.fromtimestamp(end_hour_epoch, tz=timezone.utc)

    start_hour_str, end_hour_str = start_hour.strftime("%Y-%m-%dT%H:%M"), end_hour.strftime("%Y-%m-%dT%H:%M")

    params = {
        "start_hour": start_hour_str,
        "end_hour": end_hour_str,
        "latitude": str(latitude),
        "longitude": str(longitude),
        frequency: measurements,
        "wind_speed_unit": "ms",
        "timezone": timezone_param,
    }

    try:
        response = _SESSION.get(base_url, params=params, timeout=15)
        # Force an exception if the status code is 5xx (includes 504)
        response.raise_for_status() # turns any 4xx or 5xx status code into a requests.exceptions.HTTPError
    except (requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
            requests.exceptions.HTTPError) as e:
        base_url = forecast
        print(f"The request failed ({type(e).__name__}): {e}.\nFalling back to {base_url} endpoint.")
        params.pop("start_hour", None)
        params.pop("end_hour", None)
        if is_backfill and days_since_start_sim > 73:
            print(f"Your start date is not available in {base_url} endpoint. Please select a more recent date if the Kestra retries keep failing.")
            raise
        elif is_backfill and days_since_start_sim <= 73:
            params["past_days"] = 73
        else:
            params["past_days"] = 32
        try:
            needs_clipping = True
            response = _SESSION.get(base_url, params=params, timeout=15)
        except Exception as e:
            print(f"An error occurred: {e}. Kestra will retry the data retrieval 5 times. Exception type: {type(e)}")
            raise
    except requests.exceptions.RequestException as e:
        print(f"An error occurred {e}. Kestra will retry the data retrieval 5 times. Exception type: {type(e)}")
        raise

    # # Insert this before line 200 in simulator.py
    # if not response.text:
    #     print(f"DEBUG: Empty response from API.")
    #     print(f"DEBUG: URL used: {response.url}")
    #     print(f"DEBUG: Status Code: {response.status_code}")
    #     # This will show you exactly what is coming back from the server locally
    #     raise ValueError("API returned empty body.")
    # elif not response.headers.get('Content-Type', '').startswith('application/json'):
    #     print(f"DEBUG: Non-JSON response received: {response.text[:200]}")
    #     raise ValueError(f"API did not return JSON. Received: {response.text[:200]}")

    data = response.json()
    df = pd.DataFrame(data['hourly'])

    datetime_cols = ['time']
    df[datetime_cols] = df[datetime_cols].apply(
        pd.to_datetime
    )

    # Clip the dataframe if needed
    if needs_clipping:
        df.astype(dtypes)
        df = df[df['time'].between(start_hour_str, end_hour_str)]
        df = df.reset_index(drop=True)

    year = int(start_hour.year)
    fill_value = MISSING_TSOIL_54CM.get(year, 27.0)  # fallback to 27 if year not in dict
    noise = rng.normal(0, 0.1, size=df["soil_temperature_54cm"].isna().sum())
    df.loc[df["soil_temperature_54cm"].isna(), "soil_temperature_54cm"] = fill_value + noise

    if df.isna().any().any():
        base_url = historical if base_url == forecast else forecast
        print(f"Missing values detected.\nFalling back to {base_url} endpoint.")
        try:
            response = _SESSION.get(base_url, params=params, timeout=15)
            # Force an exception if the status code is 5xx (includes 504)
            response.raise_for_status() # turns any 4xx or 5xx status code into a requests.exceptions.HTTPError
        except (requests.exceptions.Timeout,
                requests.exceptions.ConnectionError,
                requests.exceptions.HTTPError) as e:
            base_url = forecast
            print(f"The request failed ({type(e).__name__}): {e}.\nFalling back to {base_url} endpoint.")
            params.pop("start_hour", None)
            params.pop("end_hour", None)
            if is_backfill and days_since_start_sim > 73:
                print(f"Your start date is not available in {base_url} endpoint. Please select a more recent date if the Kestra retries keep failing.")
                raise
            elif is_backfill and days_since_start_sim <= 73:
                params["past_days"] = 73
            else:
                params["past_days"] = 32
            try:
                needs_clipping = True
                response = _SESSION.get(base_url, params=params, timeout=15)
            except Exception as e:
                print(f"An error occurred: {e}. Kestra will retry the data retrieval 5 times. Exception type: {type(e)}")
                raise
        except requests.exceptions.RequestException as e:
            print(f"An error occurred {e}. Kestra will retry the data retrieval 5 times. Exception type: {type(e)}")
            raise
        data = response.json()
        df = pd.DataFrame(data['hourly'])
        datetime_cols = ['time']
        df[datetime_cols] = df[datetime_cols].apply(
            pd.to_datetime
        )

        # Clip the dataframe if needed
        if needs_clipping:
            df.astype(dtypes)
            df = df[df['time'].between(start_hour_str, end_hour_str)]
            df = df.reset_index(drop=True)

        year = int(start_hour.year)
        fill_value = MISSING_TSOIL_54CM.get(year, 27.0)  # fallback to 27 if year not in dict
        noise = rng.normal(0, 0.1, size=df["soil_temperature_54cm"].isna().sum())
        df.loc[df["soil_temperature_54cm"].isna(), "soil_temperature_54cm"] = fill_value + noise

    if df.isna().any().any():
        raise ValueError("Function failed: Retrieved weather data contains NaN values. Simulation is seeded on weather data, therefore there can't be NaN values.")

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

    df['PS'] /= 10.0 # hPa to kPa
    
    ##
    df['latitude'], df['longitude'] = latitude, longitude
    ##
    
    df['location_id'] = location_id

    dtypes = dtypes['DTYPES']

    return df.astype(dtypes)

def prepare_forcing_arrays(df, compressor_installation_date, resample_rate='1min'):
    latitude = df['latitude'][0]
    longitude = df['longitude'][0]
    df = df.drop(columns=['latitude', 'longitude'])
    df_resampled = (
        df.set_index('datetime')
          .resample(resample_rate)
          .asfreq()
    )

    # 2. SEPARATE: Isolate numeric columns (the ones that CAN be interpolated)
    numeric_cols = df_resampled.select_dtypes(include=['number']).columns

    # 3 INTERPOLATE: Only the numbers
    df_resampled[numeric_cols] = df_resampled[numeric_cols].interpolate(method='time')

    # 4. STITCH: Reset index to bring 'datetime' back as a column
    df_resampled = df_resampled.reset_index()

    # clip the last row, which corresponds to midnight of day after end date and was used for interpolation
    df_resampled = df_resampled[:-1]

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

    zone_name = get_timezone_name(latitude, longitude)
    localtime_series = df_resampled['datetime'].dt.tz_localize('UTC').dt.tz_convert(zone_name)
    localtime_arr = localtime_series.dt.tz_localize(None).values

    localtime_reference = {
        # full datetime, for Python-side scheduling / labeling / storage
        'dt_arr': localtime_arr,

        # Numba-friendly calendar fields
        'year':  localtime_arr.astype('datetime64[Y]').astype(np.int64) + 1970,
        'month': (localtime_arr.astype('datetime64[M]').astype(np.int64) % 12) + 1,
        'day':   (localtime_arr.astype('datetime64[D]') - localtime_arr.astype('datetime64[M]')).astype(np.int64) + 1,
        'hour':  (localtime_arr.astype('datetime64[h]').astype(np.int64) % 24),
        'minute': (localtime_arr.astype('datetime64[m]').astype(np.int64) % 60),
        'second': (localtime_arr.astype('datetime64[s]').astype(np.int64) % 60),
        # 'day_of_year': (
        #     dt_arr.astype('datetime64[D]') - localtime_arr.astype('datetime64[Y]')
        # ).astype(np.int64) + 1
    }

    days_since_comp_installation_arr = ((df_resampled['datetime'] - pd.to_datetime(compressor_installation_date)).dt.total_seconds() / 86400).to_numpy()
    days_since_comp_installation_arr = days_since_comp_installation_arr.astype(np.float64)

    return (
        forcing, 
        localtime_reference, 
        df_resampled, 
        float(timeparse(resample_rate)), 
        days_since_comp_installation_arr,
    )

def create_seed(base_seed, year, month, day_of_year):
    seed = int(base_seed + year * 10000 + month * 100 + day_of_year)
    return seed

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
        T_lookup = pd.to_numeric(sub["temperature_C"].values, errors="coerce")
        R_lookup = pd.to_numeric(sub["respiration"].values, errors="coerce")

    mask = ~np.isnan(R_lookup)

    return np.asarray(T_lookup[mask], dtype=float), np.asarray(R_lookup[mask], dtype=float)

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
def compressor_metrics(
    T_coil_evap, 
    T_approach_condenser, 
    T_ambient, 
    eta, 
    Q_cooling_actual_w
):
    ## COP and compressor power
    T_evap_K = T_coil_evap + 273.15      # evaporator 8°C below setpoint
    T_cond_K = (T_ambient + T_approach_condenser) + 273.15    # condenser 12°C above ambient # old values 12 18
    COP_carnot = T_evap_K / max(T_cond_K - T_evap_K, 0.0)
    COP_actual = max(COP_carnot * eta, 0.5)  # floor at 0.5, physically unreachable below this
    W_compressor_kw = (Q_cooling_actual_w/1000) / COP_actual
    Q_condenser_kw = W_compressor_kw + (Q_cooling_actual_w/1000)
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

@njit
def calculate_door_infiltration_gosney(
    T_room,
    T_source,  # Pass T_ambient OR T_plant here
    P_Pa,
    WS2M,
    W_door,    # Pass W_door_ext OR W_door_int
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

    A_door = W_door * H_door

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

@njit
def sample_gamma_minutes(rng, mean_min: float, shape: float) -> float:
    scale = mean_min / shape
    return float(rng.gamma(shape, scale))

################# GARBAGE
# def build_door_and_mass_schedules(
#     hour_arr: np.ndarray,
#     minute_arr: np.ndarray,
#     doy_arr: np.ndarray,
#     month_arr: np.ndarray,
#     year_arr: np.ndarray,
#     # tunnel_exit_fruit_temp: float,
#     max_inventory_kg: float,
#     arrival_scale: float,
#     shipment_scale: float,
#     fruit_type: str,
#     forcing_dt_sec: float = 60.0,
#     min_ship_mass: float = 20.0,
#     seed: int = 42,
# ):
#     new_seed = int(seed)
#     rng = np.random.default_rng(new_seed)
# 
#     tunnel_exit_fruit_temp = FRUITS_CONFIG[fruit_type]["tunnel_exit_fruit_temp"]
# 
#     n = len(hour_arr)
#     dt_hr = forcing_dt_sec/3600.0
# 
#     door_ext_open = np.zeros(n, dtype=np.int8)
#     door_int_open = np.zeros(n, dtype=np.int8)
#     fruit_mass_delta_kg = np.zeros(n, dtype=np.float64)
#     incoming_temperature_arr = np.full(n, np.nan, dtype=np.float64)
# 
#     # monthly_weight = np.array([
#     #     0.70, 0.75, 0.80, 0.85, 0.90, 1.00,
#     #     1.10, 1.25, 1.40, 1.50, 1.35, 1.00
#     # ], dtype=float)
# 
#     # monthly_weight = np.array([
#     #     0.15, 0.10, 0.08, 0.08, 0.12, 0.25,
#     #     0.50, 0.85, 1.15, 1.40, 1.30, 0.80
#     # ], dtype=float)
# 
#     # Background door openings not directly tied to product movement
#     lambda_ext_bg_per_hour = np.array([
#         0.00, 0.00, 0.00, 0.00, 0.00, 0.02,
#         0.05, 0.10, 0.20, 0.25, 0.20, 0.15,
#         0.10, 0.10, 0.15, 0.20, 0.25, 0.20,
#         0.10, 0.05, 0.02, 0.00, 0.00, 0.00
#     ], dtype=float)
# 
#     lambda_int_bg_per_hour = np.array([
#         0.02, 0.02, 0.02, 0.02, 0.02, 0.05,
#         0.20, 0.50, 1.00, 1.20, 1.30, 1.20,
#         1.00, 0.80, 1.00, 1.20, 1.30, 1.10,
#         0.60, 0.20, 0.08, 0.05, 0.02, 0.02
#     ], dtype=float) # * 0.6
# 
#     # Logistics event rates
#     # Internal arrivals into cold room from process side
#     lambda_arrival_per_hour = np.array([
#         0.00, 0.00, 0.00, 0.00, 0.00, 0.05,
#         0.20, 0.60, 1.20, 1.50, 1.20, 0.80,
#         0.60, 0.50, 0.80, 1.00, 1.20, 1.00,
#         0.50, 0.20, 0.05, 0.00, 0.00, 0.00
#     ], dtype=float)
# 
#     # lambda_arrival_per_hour = np.array([
#     # 0.00, 0.00, 0.00, 0.00, 0.00, 0.20,
#     # 0.80, 2.40, 4.80, 6.00, 4.80, 3.20,
#     # 2.40, 2.00, 3.20, 4.00, 4.80, 4.00,
#     # 2.00, 0.80, 0.20, 0.00, 0.00, 0.00
#     # ], dtype=float)
# 
#     # External shipments out of cold room to trucks
#     lambda_shipment_per_hour = np.array([
#         0.00, 0.00, 0.00, 0.00, 0.00, 0.00,
#         0.00, 0.00, 0.05, 0.10, 0.15, 0.20,
#         0.40, 0.50, 0.80, 1.00, 1.20, 1.00,
#         0.50, 0.20, 0.05, 0.00, 0.00, 0.00
#     ], dtype=float)
# 
#     # Door open durations
#     ext_duration_mean_min = 1.5
#     int_duration_mean_min = 0.5
#     duration_shape = 2.0
# 
#     ext_open_until = -1
#     int_open_until = -1
# 
#     event_code = np.zeros(n, dtype=np.int8)
# 
#     inventory_kg = 0.0
#     for i in range(n):
#         hr = int(hour_arr[i])
#         minute = int(minute_arr[i])
# 
#         # simple seasonality weights
#         month_idx = int(month_arr[i]) - 1
#         season_weight = FRUITS_CONFIG[fruit_type]["monthly_weight"][month_idx]
#         year = int(year_arr[i])
#         yearly_w = FRUITS_CONFIG[fruit_type]["yearly_weight"]
#         year_weight = yearly_w.get(year, yearly_w[max(yearly_w.keys())])
#         throughput_weight = season_weight * year_weight
#         # year_offset = int(year_arr[i]) - 2023
#         # trend_weight = 1.0 + 0.08 * year_offset
#         # trend_weight = max(1.0 + 0.08 * year_offset, 0.1)
#         # throughput_weight = season_weight * trend_weight
# 
#         # ---- 1) Generate logistics events first ----
#         p_arrival = min(1.0, lambda_arrival_per_hour[hr] * dt_hr * throughput_weight)
#         p_shipment = min(1.0, lambda_shipment_per_hour[hr] * dt_hr * throughput_weight)
# 
#         if inventory_kg >= max_inventory_kg:
#             p_arrival = 0.0
# 
#         if inventory_kg <= min_ship_mass:
#             p_shipment = 0.0
# 
#         u = rng.random()
# 
#         # Arrival into cold room through internal door
#         if u < p_arrival:
#             raw_mass = rng.gamma(shape=2.0, scale=arrival_scale*throughput_weight) #*throughput_weight)
#             mass = min(raw_mass, max_inventory_kg - inventory_kg)
# 
#             if mass > 0:
#                 fruit_mass_delta_kg[i] += mass
#                 inventory_kg += mass
#                 event_code[i] = 1
#                 incoming_temperature_arr[i] = np.clip(
#                     rng.normal(tunnel_exit_fruit_temp + 1, 0.4),
#                     tunnel_exit_fruit_temp - 0.5,
#                     tunnel_exit_fruit_temp + 2.0
#                 )
# 
#                 dur_min = sample_gamma_minutes(rng, int_duration_mean_min, duration_shape)
#                 dur_steps = max(1, int(round((dur_min * 60.0) / forcing_dt_sec)))
#                 int_open_until = max(int_open_until, i + dur_steps)
#         
#         # Shipment out of cold room through external door
#         elif u < p_arrival + p_shipment: #  and inventory_kg > 0.0:
#             requested_mass = rng.gamma(shape=2.0, scale=shipment_scale *throughput_weight)
#             mass = min(requested_mass, inventory_kg) # *throughput_weight)
#             if mass > min_ship_mass or mass == inventory_kg:
#                 fruit_mass_delta_kg[i] -= mass
#                 inventory_kg -= mass
#                 event_code[i] = 2
# 
#                 dur_min = sample_gamma_minutes(rng, ext_duration_mean_min, duration_shape)
#                 dur_steps = max(1, int(round((dur_min * 60.0) / forcing_dt_sec)))
#                 ext_open_until = max(ext_open_until, i + dur_steps)
# 
#         if (
#             hr == 23 and minute == 50
#             and inventory_kg > 0.0
#             and event_code[i]==0
#         ):
#             mass = inventory_kg
#             fruit_mass_delta_kg[i] -= mass
#             inventory_kg = 0.0
#             event_code[i] = 2
# 
#             dur_min = sample_gamma_minutes(rng, ext_duration_mean_min, duration_shape)
#             dur_steps = max(1, int(round((dur_min * 60.0) / forcing_dt_sec)))
#             ext_open_until = max(ext_open_until, i + dur_steps)
# 
#          # ---- 2) Add background door openings ----
# 
#         p_ext_bg = min(1.0, lambda_ext_bg_per_hour[hr] * dt_hr)
#         if rng.random() < p_ext_bg:
#             if rng.random() < 0.50:
#                 dur_min = sample_gamma_minutes(rng, ext_duration_mean_min, duration_shape)
#                 dur_steps = max(1, int(round((dur_min * 60.0) / forcing_dt_sec)))
#                 ext_open_until = max(ext_open_until, i + dur_steps)
# 
#         p_int_bg = min(1.0, lambda_int_bg_per_hour[hr] * dt_hr)
#         if rng.random() < p_int_bg:
#             if rng.random() < 0.70:
#                 dur_min = sample_gamma_minutes(rng, int_duration_mean_min, duration_shape)
#                 dur_steps = max(1, int(round((dur_min * 60.0) / forcing_dt_sec)))
#                 int_open_until = max(int_open_until, i + dur_steps)
# 
#         # ---- 3) Write schedules ----
# 
#         if i < ext_open_until:
#             door_ext_open[i] = 1
#         if i < int_open_until:
#             door_int_open[i] = 1
# 
#     return door_ext_open, door_int_open, fruit_mass_delta_kg, event_code, incoming_temperature_arr
#################

@njit
def calculate_throughput_weight(
    month_local,
    year_local,
    monthly_weight_arr,
    yearly_weight_dict, # numba-compatible dict
):
    month_idx = int(month_local) - 1
    season_weight = monthly_weight_arr[month_idx]
    year_local = year_local
    if year_local in yearly_weight_dict:
        year_weight = yearly_weight_dict[year_local]
    else:
        ref_key = -1
        for key in yearly_weight_dict:
            if key > ref_key:
                max_year = key
            ref_key = key
        year_weight = yearly_weight_dict[max_year]
    throughput_weight = season_weight * year_weight
    return throughput_weight

@njit
def calculate_fruit_quality_grade(
    throughput_weight,
    base_quality_arr,
    rng,
):
    # We map throughput impact to quality. If throughput_weight is low,
    # it compresses Grade A and pushes fruits down to Grades B and C.
    p_A_base = base_quality_arr[0]
    p_B_base = base_quality_arr[1]
    p_C_base = base_quality_arr[2]
    
    if throughput_weight < 1.0:
        # Penalize Grade A based on poor seasonal/yearly throughput
        # (e.g., El Niño 2024 reduces Grade A ratio)
        p_A = p_A_base * (0.6 + 0.4 * throughput_weight)
        
        # Proportional allocation of the remaining probability space to B and C
        remaining = 1.0 - p_A
        denom = p_B_base + p_C_base
        p_B = remaining * (p_B_base / denom)
        p_C = remaining * (p_C_base / denom)
    else:
        # Optimal conditions: slightly improve Grade A, compress lower grades
        p_A = min(0.90, p_A_base * (1.0 + (throughput_weight - 1.0) * 0.1))
        remaining = 1.0 - p_A
        denom = p_B_base + p_C_base
        p_B = remaining * (p_B_base / denom)
        p_C = remaining * (p_C_base / denom)

    u = rng.random()
    # 3. Fast Inverse Transform Sampling
    if u < p_A:
        sampled_grade = 0  # Grade A
    elif u < p_A + p_B:
        sampled_grade = 1  # Grade B
    else:
        sampled_grade = 2  # Grade C
        
    return sampled_grade

@njit
def door_and_mass_single_events(
    step_min, # outer-loop-grain step
    rng, # pass in the rng object directly instead of seed, to avoid issues with Numba and random state management
    fruit_mass_kg, # current fruit mass in room
    throughput_weight,
    lambda_ext_bg_per_hour,
    lambda_int_bg_per_hour,
    lambda_arrival_per_hour,
    lambda_shipment_per_hour,
    ext_duration_mean_min,
    int_duration_mean_min,
    ext_open_until,
    int_open_until,
    duration_shape,
    max_dwell_days,
    tunnel_exit_fruit_temp: float,
    max_inventory_kg: float,
    arrival_scale: float,
    shipment_scale: float,
    forcing_dt_sec: float = 60.0,
    min_ship_mass: float = 20.0,
    min_arrived_mass: float = 20.0,
    min_dispatch_weight: float = 0.5,
):
    dt_hr = forcing_dt_sec/3600.0

    door_ext_open = 0 # np.zeros(n, dtype=np.int8)
    door_int_open = 0 # np.zeros(n, dtype=np.int8)
    fruit_mass_delta_kg = 0.0 # np.zeros(n, dtype=np.float64)
    incoming_temperature = SENTINEL_FLOAT64

    # ext_open_until = -1
    # int_open_until = -1

    event_code = 0 # np.zeros(n, dtype=np.int8)

    inventory_kg = fruit_mass_kg

    # ---- 1) Generate logistics events first ----
    p_arrival = min(1.0, lambda_arrival_per_hour * dt_hr * throughput_weight)
    p_shipment = min(1.0, lambda_shipment_per_hour * dt_hr * max(throughput_weight, min_dispatch_weight))

    if inventory_kg >= max_inventory_kg:
        p_arrival = 0.0

    if inventory_kg <= min_ship_mass:
        p_shipment = 0.0

    u = rng.random()

    # Arrival into cold room through internal door
    if u < p_arrival:
        raw_mass = rng.gamma(shape=2.0, scale=arrival_scale*throughput_weight) #*throughput_weight)
        mass = min(raw_mass, max_inventory_kg - inventory_kg)
        # mass = max(mass, min_arrived_mass)
        if mass > min_arrived_mass:
            # mass = min_arrived_mass if mass <= min_arrived_mass else mass
            fruit_mass_delta_kg += mass
            inventory_kg += mass
            event_code = 1
            incoming_temperature = manual_clipping(
                rng.normal(tunnel_exit_fruit_temp + 1, 0.4),
                tunnel_exit_fruit_temp - 0.5,
                tunnel_exit_fruit_temp + 2.0
            )

            dur_min = sample_gamma_minutes(rng, int_duration_mean_min, duration_shape)
            dur_steps = max(1, int(round((dur_min * 60.0) / forcing_dt_sec)))
            int_open_until = max(int_open_until, step_min + dur_steps)
    
    # Shipment out of cold room through external door
    elif u < p_arrival + p_shipment: #  and inventory_kg > 0.0:
        requested_mass = rng.gamma(shape=2.0, scale=shipment_scale *throughput_weight)
        mass = min(requested_mass, inventory_kg) # *throughput_weight)
        if mass > min_ship_mass or mass == inventory_kg:
            fruit_mass_delta_kg -= mass
            inventory_kg -= mass
            event_code = 2

            dur_min = sample_gamma_minutes(rng, ext_duration_mean_min, duration_shape)
            dur_steps = max(1, int(round((dur_min * 60.0) / forcing_dt_sec)))
            ext_open_until = max(ext_open_until, step_min + dur_steps)

    # ---- 2) Add background door openings ----

    p_ext_bg = min(1.0, lambda_ext_bg_per_hour * dt_hr)
    if rng.random() < p_ext_bg:
        if rng.random() < 0.50:
            dur_min = sample_gamma_minutes(rng, ext_duration_mean_min, duration_shape)
            dur_steps = max(1, int(round((dur_min * 60.0) / forcing_dt_sec)))
            ext_open_until = max(ext_open_until, step_min + dur_steps)

    p_int_bg = min(1.0, lambda_int_bg_per_hour * dt_hr)
    if rng.random() < p_int_bg:
        if rng.random() < 0.70:
            dur_min = sample_gamma_minutes(rng, int_duration_mean_min, duration_shape)
            dur_steps = max(1, int(round((dur_min * 60.0) / forcing_dt_sec)))
            int_open_until = max(int_open_until, step_min + dur_steps)

    # ---- 3) Write schedules ----

    if step_min < ext_open_until:
        door_ext_open = 1
    if step_min < int_open_until:
        door_int_open = 1

    return (
        door_ext_open,
        door_int_open,
        ext_open_until,
        int_open_until,
        fruit_mass_delta_kg,
        event_code,
        incoming_temperature,
    )

@njit
def batch_event_tracking(
    incoming_temperature,
    max_active_batches,
    fruit_mass_delta_kg,
    batch_ids,
    batch_masses,
    batch_incoming_temps,
    batch_quality_grades,
    active_batch_mask,
    n_active,
    next_batch_id,
    front_idx,
    back_idx,

    max_batches_per_dispatch,

    throughput_weight,
    base_quality_arr,
    rng,
):
    arrival_slot = SENTINEL_INT64
    dispatch_slots_arr = np.full(max_batches_per_dispatch, SENTINEL_INT64, dtype=np.int64)
    dispatched_masses_arr = np.full(max_batches_per_dispatch, SENTINEL_FLOAT64, dtype=np.float64)
    batch_masses_at_dispatch_arr = np.full(max_batches_per_dispatch, SENTINEL_FLOAT64, dtype=np.float64)
    # batch_masses_remaining_arr = np.full(max_batches_per_dispatch, SENTINEL_FLOAT64, dtype=np.float64)

    delta = fruit_mass_delta_kg
    if delta > 0 and n_active < max_active_batches:
        mass_in = float(delta)
        slot = back_idx % max_active_batches
        batch_ids[slot] = next_batch_id
        batch_masses[slot] = mass_in
        batch_incoming_temps[slot] = incoming_temperature
        batch_quality_grades[slot] = calculate_fruit_quality_grade(
            throughput_weight=throughput_weight,
            base_quality_arr=base_quality_arr,
            rng=rng,
        )
        active_batch_mask[slot] = 1
        arrival_slot = slot
        n_active += 1
        next_batch_id += 1
        back_idx += 1
    elif delta < 0:
        mass_to_remove = float(-delta)
        touched_batches_count = 0
        while mass_to_remove > MASS_KG_FLOOR and n_active > 0 and touched_batches_count < max_batches_per_dispatch:
            slot = front_idx % max_active_batches
            current_batch_mass = batch_masses[slot]
            batch_masses_at_dispatch_arr[touched_batches_count] = current_batch_mass
            removable = min(current_batch_mass, mass_to_remove)
            batch_masses[slot] -= removable
            mass_to_remove -= removable
            dispatch_slots_arr[touched_batches_count] = slot
            dispatched_masses_arr[touched_batches_count] = removable 
            if batch_masses[slot] <= MASS_KG_FLOOR:
                active_batch_mask[slot] = 0
                n_active -= 1
                front_idx += 1
                touched_batches_count += 1
    return (
        # Fixed-sized batch arrays
        batch_ids,
        batch_masses,
        batch_incoming_temps,
        batch_quality_grades,
        active_batch_mask,
        # Scalars for batch tracking
        next_batch_id,
        n_active,
        # Moving indices to avoid array re-sizing
        front_idx,
        back_idx,
        # Incoming/outgoing batch slot(s)
        arrival_slot,
        dispatch_slots_arr,
        dispatched_masses_arr,
        batch_masses_at_dispatch_arr,
    )

################# GARBAGE
# def build_batch_event_tables(
#     dt_arr,
#     fruit_mass_delta_kg,
#     incoming_temperature_arr,
#     plant_id,
#     fruit_type="blueberry",
#     active_batches=None,
#     next_batch_id=1,
#     seed=42,
# ):
#     new_seed = int(seed + 1)
#     rng = np.random.default_rng(new_seed)
# 
#     if active_batches is None:
#         active_batches = []
#     
#     event_rows = []
#     batch_rows = []
# 
#     n = len(dt_arr)
# 
#     for i in range(n):
#         ts = dt_arr[i]
#         delta = fruit_mass_delta_kg[i]
# 
#         # -----------------------------
#         # ARRIVAL
#         # -----------------------------
#         if delta > 0:
#             mass_in = float(delta)
# 
#             incoming_temperature = incoming_temperature_arr[i]
#             # sampled_temp = np.clip(
#             #     rng.normal(tunnel_exit_fruit_temp + 1.0, 0.4),
#             #     tunnel_exit_fruit_temp - 0.5,
#             #     tunnel_exit_fruit_temp + 2.0
#             # )   # tunnel exit temp
#             sampled_grade = rng.choice(["A", "B", "C"], p=[0.7, 0.2, 0.1]) # ADD SEASONAL WEIGHTS
# 
#             # # schedule dispatch same day
#             # residence_hours = rng.uniform(4.0, 12.0)
#             # dispatch_ts = ts + np.timedelta64(int(residence_hours * 60), "m")
# 
#             # day_end = ts.astype("datetime64[D]") + np.timedelta64(1, "D")
#             # if dispatch_ts >= day_end:
#             #     dispatch_ts = day_end - np.timedelta64(1, "m")
# 
#             batch = {
#                 "plant_id": plant_id,
#                 "batch_id": next_batch_id,
#                 "fruit_type": fruit_type,
#                 "arrival_ts": ts,
#                 # "scheduled_dispatch_ts": dispatch_ts,
#                 "mass_kg_initial": mass_in,
#                 "mass_kg_remaining": mass_in,
#                 "tunnel_exit_temp_c": incoming_temperature,
#                 "quality_grade": sampled_grade,
#                 "first_dispatch_ts": pd.NaT,
#                 "final_dispatch_ts": pd.NaT,
#             }
# 
#             active_batches.append(batch)
# 
#             event_rows.append({
#                 "plant_id": plant_id,
#                 "timestamp": ts,
#                 "event_type": "arrival",
#                 "batch_id": next_batch_id,
#                 "mass_kg": mass_in,
#                 "fruit_type": fruit_type,
#                 "quality_grade": sampled_grade,
#                 "tunnel_exit_temp_c": incoming_temperature,
#             })
# 
#             next_batch_id += 1
# 
#         # -----------------------------
#         # DISPATCH (FIFO)
#         # -----------------------------
#         elif delta < 0:
#             mass_to_remove = float(-delta)
# 
#             while mass_to_remove > 1e-9 and active_batches:
#                 b = active_batches[0]
#                 removable = min(b["mass_kg_remaining"], mass_to_remove)
# 
#                 if pd.isna(b["first_dispatch_ts"]):
#                     b["first_dispatch_ts"] = ts
# 
#                 b["mass_kg_remaining"] -= removable
#                 mass_to_remove -= removable
# 
#                 event_rows.append({
#                     "plant_id": plant_id,
#                     "timestamp": ts,
#                     "event_type": "dispatch",
#                     "batch_id": b["batch_id"],
#                     "mass_kg": removable,
#                     "fruit_type": b["fruit_type"],
#                     "quality_grade": b["quality_grade"],
#                     "tunnel_exit_temp_c": b["tunnel_exit_temp_c"],
#                 })
# 
#                 if b["mass_kg_remaining"] <= 1e-9:
#                     b["final_dispatch_ts"] = ts
#                     batch_rows.append(b.copy())
#                     active_batches.pop(0)
# 
#         # -----------------------------
#         # END-OF-DAY FORCE CLEAR
#         # -----------------------------
#         # simple: detect last timestep of the day
#         if i < n - 1:
#             next_day = dt_arr[i + 1].astype("datetime64[D]")
#             current_day = ts.astype("datetime64[D]")
# 
#             if next_day > current_day:
#                 # day ended --> clear remaining batches
#                 while active_batches:
#                     b = active_batches[0]
# 
#                     if pd.isna(b["first_dispatch_ts"]):
#                         b["first_dispatch_ts"] = ts
# 
#                     event_rows.append({
#                         "plant_id": plant_id,
#                         "timestamp": ts,
#                         "event_type": "dispatch_eod",
#                         "batch_id": b["batch_id"],
#                         "mass_kg": b["mass_kg_remaining"],
#                         "fruit_type": b["fruit_type"],
#                         "quality_grade": b["quality_grade"],
#                         "tunnel_exit_temp_c": b["tunnel_exit_temp_c"],
#                     })
# 
#                     b["final_dispatch_ts"] = ts
#                     batch_rows.append(b.copy())
#                     active_batches.pop(0)
# 
#                 # for b in active_batches:
#                 #     b_copy = b.copy()
#                 #     b_copy["final_dispatch_ts"] = pd.NaT
#                 #     batch_rows.append(b_copy)
#     # -----------------------------
#     # Convert to DataFrames
#     # -----------------------------
#     events_df = pd.DataFrame(event_rows)
#     batches_df = pd.DataFrame(batch_rows)
# 
#     return events_df, batches_df, active_batches, next_batch_id
#################

# def dict_to_numba_dict(regular_dict, key_type, value_type):
#     numba_dict = Dict.empty(
#         key_type=key_type,
#         value_type=value_type,
#     )
# 
#     cast_key = numba_dict.key_type.get_python_type()
#     cast_value = numba_dict.value_type.get_python_type()
# 
#     for key, value in regular_dict.items():
#         numba_dict[cast_key(key)] = cast_value(value)
# 
#     return numba_dict

@njit
def concat_datetime_to_int(year, month, day, hour, minute, second):
    # Combines components into a single YYYYMMDDHHMMSS fixed-length integer
    fixed_len_int = (
        (year * 10_000_000_000)
        + (month * 100_000_000)
        + (day * 1_000_000)
        + (hour * 10_000)
        + (minute * 100)
        + second
    )
    return fixed_len_int

@njit(fastmath=True)
def run_simulation_chunk(
    T_ambient_arr,
    RH_ambient_arr,
    P_arr,
    GHI_arr,
    WS_arr,
    T2MDEW_arr,
    T_ground_arr,

    # year_arr,
    # month_arr,
    # day_arr,
    # hour_arr,
    # minute_arr,
    # second_arr,

    start_epoch_timestamp,

    days_since_comp_installation_arr,
    compressor_degradation_rate,

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
    Q_rated,
    TD_design,
    f_structure,
    # k_wind_U,
    Cp_air,
    T_lookup,
    R_lookup,
    BF,
    n_ach_eff_per_sec,
    degradation_factor,

    cooling_frac_floor,

    # Compressor parameters for T_approach_condenser calculation
    a_cond,
    b_cond,

    h_respiration,
    tunnel_exit_fruit_temp_ref,
    Cp_fruit,
    # chosen zone only; do not keep the whole k_by_zone dict
    k_zone_ref,   # cold_storage
    k_p,
    f_min,
    rho_load_bulk,

    CO2_outdoor_ppm,
    O2_outdoor_pct,

    # Numba-compatible dicts for catching output data
    telemetry_array_dict_float64,
    telemetry_array_dict_int8,

    last_state_scalar_dict_float64,
    last_state_array_dict_float64,
    last_state_array_dict_int64,
    last_state_scalar_dict_int64,
    last_state_scalar_dict_int8,
    last_state_array_dict_int8,

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

    # door_ext_open,
    # door_int_open,
    # fruit_mass_delta_kg,
    # incoming_temperature_arr,

    k_door_ext,
    k_door_int,

    V_free_min,
    m_air_room_min,

    timestep,

    eps,

    h_i_walls,
    h_i_roof,

    eta_ref,

    ##  Initialization variables/parameters

    # check if this is the first run
    is_first_run,

    # global step
    global_step_init,

    # controllers
    cooling_call_init,
    humidifier_call_init,
    cooling_frac_init,
    humidifier_frac_init,
    condense_frac_init,

    # Temperature
    T_room_init,
    T_sensor_init,
    T_plant_init,
    T_plant_target_init,
    T_pulp_init,

    # Gases
    CO2_ppm_init,
    O2_pct_init,

    # Humidity parameters
    W_room_init,
    P_w_room_init,
    RH_room_init,
    RH_room_sensor_init,
    W_coil_sat_init,

    # Logistics
    door_ext_open_fraction_init,
    door_int_open_fraction_init,
    ext_open_until_init,
    int_open_until_init,

    batch_ids_init,
    batch_masses_init,
    batch_incoming_temps_init,
    batch_quality_grades_init,
    active_batch_mask_init,
    n_active_init,
    next_batch_id_init,
    front_idx_init,
    back_idx_init,

    # Fruit mass
    fruit_mass_kg_init,
    total_water_loss_kg_init,

    ## Other inputs
    hour_local_arr,
    minute_local_arr,
    month_local_arr,
    year_local_arr,

    lambda_ext_bg_per_hour_arr,
    lambda_int_bg_per_hour_arr,
    lambda_arrival_per_hour_arr,
    lambda_shipment_per_hour_arr,

    monthly_weight_arr, # fruit-specific 12-month array
    yearly_weight_dict, # fruit-specific numba-compatible dict
    base_quality_arr, # base probability distribution for fruit-specific A-B-C quality grading

    ext_duration_mean_min,
    int_duration_mean_min,
    duration_shape,
    max_inventory_kg,
    arrival_scale,
    shipment_scale,
    forcing_dt_sec,
    min_ship_mass,
    min_arrived_mass,
    min_dispatch_weight,
    max_dwell_days,

    max_active_batches,
    max_arrivals_per_run,
    max_dispatches_per_run,
    max_batches_per_dispatch,

    num_fields_arrival,
    num_fields_dispatch,

    rng,
):
   
    ## Define static parameters 

    # new_seed = int(seed + 2)
    # np.random.seed(new_seed)

    n = len(T_ambient_arr)

    epoch_timestamp = start_epoch_timestamp

    fruit_mass_floor_kg = MASS_KG_FLOOR 

    # Coil-related parameters
    UA_coil_evap = Q_rated/TD_design
    m_dot_air_evap = (UA_coil_evap / (Cp_air*(1 - BF)))*degradation_factor

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

    # A_door_ext = W_door_ext*H_door_ext
    # A_door_int = W_door_int*H_door_int

    steps_per_min = timestep // dt_internal
    n_total = int(n * steps_per_min)

    R_dry = R_const/(M_air*1e-3)

    T, R, T_min, T_max, T_lo, T_hi, T_ref, Q_10, R_ref = get_fruit_resp_params(T_lookup, R_lookup)

    ## Initialize arrays

    # float64 arrays (Numba-friendly)
    epoch_timestamp_arr = np.empty(n_total, dtype=np.float64)
    T_room_arr = np.empty(n_total, dtype=np.float64)
    T_sensor_arr = np.empty(n_total, dtype=np.float64)
    cooling_frac_arr = np.empty(n_total, dtype=np.float64)

    condense_frac_arr = np.empty(n_total, dtype=np.float64)

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
    Q_sensible_w_arr = np.empty(n_total, dtype=np.float64)
    Q_latent_w_arr = np.empty(n_total, dtype=np.float64)
    Q_cooling_capacity_arr = np.empty(n_total, dtype=np.float64)
    T_coil_evap_arr = np.empty(n_total, dtype=np.float64)
    T_evap_out_arr = np.empty(n_total, dtype=np.float64)
    RH_evap_out_arr = np.empty(n_total, dtype=np.float64)
    m_dot_air_evap_arr = np.empty(n_total, dtype=np.float64)

    fruit_mass_kg_arr = np.empty(n_total, dtype=np.float64)

    arrivals_arr = np.full((max_arrivals_per_run, num_fields_arrival), SENTINEL_FLOAT64, dtype=np.float64)
    dispatches_arr = np.full((max_dispatches_per_run*max_batches_per_dispatch, num_fields_dispatch), SENTINEL_FLOAT64, dtype=np.float64)

    arrival_row_idx = 0
    dispatch_row_idx = 0

    # int8 arrays (compact, Numba-friendly)
    cooling_call_arr = np.empty(n_total, dtype=np.int8)
    humidifier_call_arr = np.empty(n_total, dtype=np.int8)

    door_int_open_arr = np.empty(n_total, dtype=np.int8)
    door_ext_open_arr = np.empty(n_total, dtype=np.int8)

    # Define initialization variables

    is_first_run = bool(is_first_run)
    cooling_call_init = bool(cooling_call_init)
    humidifier_call_init = bool(humidifier_call_init)

    if is_first_run:
        CO2_ppm_init = CO2_outdoor_ppm 
        O2_pct_init = O2_outdoor_pct
        T_room_init = setpoint
        T_sensor_init = T_room_init

        T_plant_target_init = T_plant_a + T_plant_b*T_ambient_arr[0]
        T_plant_init = T_plant_target_init

        T_pulp_init = manual_clipping(
                    rng.normal(tunnel_exit_fruit_temp_ref + 1, 0.4),
                    tunnel_exit_fruit_temp_ref - 0.5,
                    tunnel_exit_fruit_temp_ref + 2.0
        )
        
        P_sat_room_start = p_sat_magnus(T_room_init) # Magnus-Tetens Approximation
        P_w_room_start = target_rh*P_sat_room_start    
        P_0 = P_arr[0]
        W_room_init = w_from_partial_pressure(P_w_room_start, P_0)
        P_w_room_init = partial_pressure_from_w(W_room_init, P_0)
        RH_room_init = manual_clipping(P_w_room_init / P_sat_room_start, 0.0, 1.0)
        RH_room_sensor_init = RH_room_init

        W_coil_sat_init = w_from_partial_pressure(p_sat_magnus(T_room_init), P_0)

        batch_ids_init = np.full(max_active_batches, SENTINEL_INT64, dtype=np.int64)
        batch_masses_init = np.full(max_active_batches, SENTINEL_FLOAT64, dtype=np.float64)
        batch_incoming_temps_init = np.full(max_active_batches, SENTINEL_FLOAT64, dtype=np.float64)
        batch_quality_grades_init = np.full(max_active_batches, SENTINEL_INT8, dtype=np.int8)
        active_batch_mask_init = np.zeros(max_active_batches, dtype=np.int8)
        n_active_init = 0
        next_batch_id_init = 1
        front_idx_init = 0
        back_idx_init = 0

        global_step_init = 0

        ext_open_until_init = -1
        int_open_until_init = -1

    
    ## Initialize variables

    # Global step offset
    global_step_offset = global_step_init

    # Controllers
    cooling_call = cooling_call_init
    cooling_frac = cooling_frac_init
    humidifier_call = humidifier_call_init
    humidifier_frac = humidifier_frac_init
    condense_frac = condense_frac_init

    # Temperature
    T_room = T_room_init
    T_sensor = T_sensor_init
    T_plant = T_plant_init
    T_plant_target = T_plant_target_init
    T_pulp = T_pulp_init

    # Gases
    CO2_ppm = CO2_ppm_init
    O2_pct = O2_pct_init

    # Humidity parameters
    W_room = W_room_init
    P_w_room = P_w_room_init    
    RH_room_sensor = RH_room_sensor_init # CHECK
    W_coil_sat = W_coil_sat_init
 
    # Logistics
    door_ext_open_fraction = door_ext_open_fraction_init
    door_int_open_fraction = door_int_open_fraction_init
  
    fruit_mass_kg = max(fruit_mass_kg_init, fruit_mass_floor_kg)

    total_water_loss_kg = total_water_loss_kg_init

    # Batch logistics
    batch_ids = batch_ids_init
    batch_masses = batch_masses_init
    batch_incoming_temps = batch_incoming_temps_init
    batch_quality_grades = batch_quality_grades_init
    active_batch_mask = active_batch_mask_init
    n_active = n_active_init
    next_batch_id = next_batch_id_init
    front_idx = front_idx_init
    back_idx = back_idx_init

    ext_open_until = ext_open_until_init
    int_open_until = int_open_until_init

    for i in range(n):
        T_ambient = T_ambient_arr[i]
        RH_ambient = RH_ambient_arr[i]
        P_Pa = P_arr[i]
        GHI = GHI_arr[i]
        wind_speed = WS_arr[i]
        T2MDEW = T2MDEW_arr[i]
        T_ground = T_ground_arr[i]

        if i == 0: # hour_arr[i] == 0 and minute_arr[i] == 0 and second_arr[i] == 0:
            days_since_install = days_since_comp_installation_arr[i]
            eta = manual_clipping(
                rng.normal(eta_ref - compressor_degradation_rate*days_since_install, 0.02),
                0.40, 0.70
            )
            # total_water_loss_kg = 0.0
            # P_sat_coil = p_sat_magnus(T_room)
            # W_coil_sat = w_from_partial_pressure(P_sat_coil, P_Pa)
            # T_coil_evap = T_room - TD_design # T_room - (Q_rated / UA_coil_evap)

        ## Fruit bacth logistics (designed at the minute grain)
        # simple seasonality weights

        year_local = year_local_arr[i]
        month_local = month_local_arr[i]
        hour_local = hour_local_arr[i]
        minute_local = minute_local_arr[i]

        throughput_weight = calculate_throughput_weight(
            month_local=month_local,
            year_local=year_local,
            monthly_weight_arr=monthly_weight_arr,
            yearly_weight_dict=yearly_weight_dict, # numba-compatible dict
        )

        door_ext_open, door_int_open, ext_open_until, int_open_until, fruit_mass_delta_kg, event_code, incoming_temperature = door_and_mass_single_events(
            step_min=global_step_offset+i, # outer-loop-grain step
            rng=rng, # pass in the rng object directly instead of seed, to avoid issues with Numba and random state management
            # hour_local=hour_local,
            # minute_local=minute_local,
            fruit_mass_kg=fruit_mass_kg, # current bulk fruit mass in room
            throughput_weight=throughput_weight,
            lambda_ext_bg_per_hour=lambda_ext_bg_per_hour_arr[hour_local],
            lambda_int_bg_per_hour=lambda_int_bg_per_hour_arr[hour_local],
            lambda_arrival_per_hour=lambda_arrival_per_hour_arr[hour_local],
            lambda_shipment_per_hour=lambda_shipment_per_hour_arr[hour_local],
            ext_duration_mean_min=ext_duration_mean_min,
            int_duration_mean_min=int_duration_mean_min,
            ext_open_until=ext_open_until,
            int_open_until=int_open_until,
            duration_shape=duration_shape,
            tunnel_exit_fruit_temp=tunnel_exit_fruit_temp_ref,
            max_inventory_kg=max_inventory_kg,
            arrival_scale=arrival_scale,
            shipment_scale=shipment_scale,
            forcing_dt_sec=forcing_dt_sec,
            min_ship_mass=min_ship_mass,
            min_arrived_mass=min_arrived_mass,
            min_dispatch_weight=min_dispatch_weight,
        )

        # maybe fill up some logistics-related events here? like incoming_temperature_arr (if needed)
        # create arrays for to track batch-specific parameters. Which parameters? T_pulp? fruit_mass_kg?

        #####

        (
            # Fixed-sized batch arrays
            batch_ids,
            batch_masses,
            batch_incoming_temps,
            batch_quality_grades,
            active_batch_mask,
            # Scalars for batch tracking
            next_batch_id,
            n_active,
            # Moving indices to avoid array re-sizing
            front_idx,
            back_idx,
            # Incoming/outgoing batch slot(s)
            arrival_slot,
            dispatch_slots_arr,
            dispatched_masses_arr,
            batch_masses_at_dispatch_arr,
        ) = batch_event_tracking(
            incoming_temperature=incoming_temperature,
            max_active_batches=max_active_batches,
            fruit_mass_delta_kg=fruit_mass_delta_kg,
            batch_ids=batch_ids,
            batch_masses=batch_masses,
            batch_incoming_temps=batch_incoming_temps,
            batch_quality_grades=batch_quality_grades,
            active_batch_mask=active_batch_mask,
            n_active=n_active,
            next_batch_id=next_batch_id,
            front_idx=front_idx,
            back_idx=back_idx,
            max_batches_per_dispatch=max_batches_per_dispatch,
            throughput_weight=throughput_weight,
            base_quality_arr=base_quality_arr,
            rng=rng,
        )

        # if event_code != 0:
        # timestamp = concat_datetime_to_int(year_arr[i], month_arr[i], day_arr[i], hour_arr[i], minute_arr[i], second_arr[i])
        # When a batch arrives
        if event_code == 1:
            arrivals_arr[arrival_row_idx, 0] = batch_ids[arrival_slot] # batch_id
            arrivals_arr[arrival_row_idx, 1] = epoch_timestamp # timestamp
            arrivals_arr[arrival_row_idx, 2] = batch_quality_grades[arrival_slot] # quality_grade
            arrivals_arr[arrival_row_idx, 3] = batch_masses[arrival_slot] # mass_incoming
            arrivals_arr[arrival_row_idx, 4] = incoming_temperature # incoming_temperature

            arrival_row_idx += 1

        # When batches are dispatched or partially dispatched
        elif event_code == 2:
            # batch_mask = dispatch_slots_arr != SENTINEL_INT64
            # dispatch_slots_arr = dispatch_slots_arr[batch_mask]
            s = 0
            for s in range(max_batches_per_dispatch):
                dispatch_slot = dispatch_slots_arr[s]
                if dispatch_slot == SENTINEL_INT64:
                    break
                dispatches_arr[dispatch_row_idx, 0] = batch_ids[dispatch_slot] # batch_id
                dispatches_arr[dispatch_row_idx, 1] = epoch_timestamp # timestamp
                dispatches_arr[dispatch_row_idx, 2] = batch_quality_grades[dispatch_slot] # quality_grade
                dispatches_arr[dispatch_row_idx, 3] = batch_masses_at_dispatch_arr[s] # mass_at_dispatch
                dispatches_arr[dispatch_row_idx, 4] = dispatched_masses_arr[s] # mass_removed
                dispatches_arr[dispatch_row_idx, 5] = batch_masses[dispatch_slot] # mass_remaining

                s += 1
                dispatch_row_idx += 1

        # active_mask = batch_masses != SENTINEL_FLOAT64
        # active_batch_masses = batch_masses[active_mask]

        # Update bult fruit parameters
        delta_mass = fruit_mass_delta_kg
        old_mass = fruit_mass_kg

        if event_code == 1:
            new_mass = old_mass + delta_mass
            if new_mass > fruit_mass_floor_kg:
                T_pulp = (old_mass * T_pulp + delta_mass * incoming_temperature) / new_mass # is it important for (bulk) T_pulp to reflect realistic temperature curves
            fruit_mass_kg = new_mass

        elif event_code == 2:
            max_removal = fruit_mass_kg - fruit_mass_floor_kg
            if max_removal < 0.0:
                max_removal = 0.0
            if -delta_mass > max_removal:
                delta_mass = -max_removal
            fruit_mass_kg += delta_mass

        fruit_mass_kg = max(fruit_mass_kg, fruit_mass_floor_kg)
        #####

        # check if this is the best positioning for this function
        if door_ext_open_fraction <= eps and door_int_open_fraction <= eps:
            leak_noise_factor = manual_clipping(rng.normal(loc=1.0, scale=0.1), 0.85, 1.15)

        for j in range(steps_per_min):
            k = int(i * steps_per_min + j)
            mdot_door_ext = 0.0
            mdot_door_int = 0.0
            h_fg_room = (2501 - 2.361 * T_room) * 1000.0

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
            cooling_frac = max(cooling_frac, SAFE_FLOOR)

            # humidity controller from lagged RH sensor
            if cooling_call:
                rh_deadband_eff = rh_deadband * 0.5 # Tighten control during cooling
            else:
                rh_deadband_eff = rh_deadband

            if RH_room_sensor < (target_rh - rh_deadband_eff):
                humidifier_call = True
            elif RH_room_sensor > (target_rh + rh_deadband_eff):
                humidifier_call = False

            target_humid = 1.0 if humidifier_call else 0.0
            humidifier_frac += (target_humid - humidifier_frac) * dt_internal / tau_humid_frac
            humidifier_frac = manual_clipping(humidifier_frac, 0.0, 1.0)
            humidifier_frac = max(humidifier_frac, SAFE_FLOOR)

            m_humidifier = m_max * humidifier_frac * f_evap_humid
            Q_humidifier_cooling = m_humidifier*h_fg_room

            # =========================================================
            # 4. HVAC / ROOM LOAD TERMS
            # =========================================================

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

            # calculate the capacity limit: how much the machine can do based on the weather
            Q_cooling_capacity = Q_rated * (1.0 - 0.007 * (T_ambient - 35.0)) * cooling_frac

            if cooling_frac > cooling_frac_floor and W_room > W_coil_sat:
                condense_call = True
            else:
                condense_call = False

            target_condense = 1.0 if condense_call else 0.0
            condense_frac += (target_condense - condense_frac) * dt_internal / tau_condense
            condense_frac = manual_clipping(condense_frac, 0.0, 1.0)
            condense_frac = max(condense_frac, SAFE_FLOOR)

            # condense_frac is a lag variable representing the thermal inertia of condensation process
            # unlike coooling_frac, it has no independent existence without the compressor running
            # when coooling_frac = 0, coil_surface is equal to room temperature, so condensation should stop immediately

            # calculate the physics demand: how much the machine wants to do based on the air state
                        
            # Supply side: coil temperature from capacity
            T_coil_evap = T_room - (Q_cooling_capacity / UA_coil_evap)
            T_coil_evap = min(T_coil_evap, setpoint - 0.5) if cooling_frac > cooling_frac_floor else T_room

            h_fg_coil = (2501 - 2.361 * T_coil_evap) * 1000.0

            # Demand side
            # 1. Define the heat transfer potential (k)
            k_transfer = m_dot_air_evap * (1.0 - BF) * Cp_air * cooling_frac

            Q_cooling_sensible = k_transfer * (T_room - T_coil_evap)
            Q_latent_headroom = max(Q_cooling_capacity - Q_cooling_sensible, 0.0)

            m_removed_uncapped = m_dot_air_evap * (1.0 - BF) * max(W_room - W_coil_sat, 0.0) * condense_frac
            m_removed_uncapped *= (1.0 if cooling_frac > cooling_frac_floor else 0.0)

            Q_cooling_latent_uncapped = m_removed_uncapped * h_fg_coil

            # Cap moisture removal by cooling capacity left for latent term
            latent_scale = min(1.0, Q_latent_headroom / max(Q_cooling_latent_uncapped, 1e-9))

            m_removed = m_removed_uncapped * latent_scale
            Q_cooling_latent = m_removed * h_fg_coil
            Q_cooling_actual = Q_cooling_sensible + Q_cooling_latent
            
            # optional compressor metrics if needed for output only
            T_approach_condenser = a_cond + b_cond * T_ambient
            COP_actual, W_compressor_kw, Q_condenser_kw = compressor_metrics(T_coil_evap, T_approach_condenser, T_ambient, eta, Q_cooling_actual)
             
            # Derive fake telemetry data for 'real-life' COP calculation from Q_sensible_cooling
            # Q_sensible_cooling = m_dot * Cp_air * (T_in - T_out)

            V_load_eff = fruit_mass_kg / rho_load_bulk
            f_free = 1.0 - V_load_eff / V_room
            f_free = manual_clipping(f_free, f_min, 1.0)
            V_free = f_free * V_room
            V_free = max(V_free, V_free_min)
            V_free_liters = V_free * 1000.0
            C_fruit = max(fruit_mass_kg * Cp_fruit, 1e4)

            k_zone = k_zone_ref*(1.0 - (0.5 * f_free))

            # effective room air capacity
            C_air = V_free * rho_air_room * Cp_air
            C_room = C_air + C_structure
            m_air_room = V_free * rho_air_room
            m_air_room = max(m_air_room, m_air_room_min)

            mdot_leak = n_ach_eff_per_sec * m_air_room

            # 1. External Door Logic
            target_door_ext = 1.0 if door_ext_open else 0.0
            # Only run math if the door is open or moving (fraction > 0)
            if target_door_ext > 0 or door_ext_open_fraction > eps:
                Q_total, rho_source = calculate_door_infiltration_gosney(
                    T_room=T_room,
                    T_source=T_ambient,  # Pass T_ambient OR T_plant here
                    P_Pa=P_Pa,
                    WS2M=wind_speed,
                    W_door=W_door_ext,    # Pass A_door_ext OR A_door_int
                    H_door=H_door_ext*door_ext_open_fraction,    # Pass H_door_ext OR H_door_int
                    R_dry=R_dry,
                    is_outdoor_door=True,
                    )
                door_ext_open_fraction += (target_door_ext - door_ext_open_fraction) * dt_internal / tau_door_ext
                door_ext_open_fraction = manual_clipping(door_ext_open_fraction, 0.0, 1.0)
                door_ext_open_fraction = max(door_ext_open_fraction, SAFE_FLOOR)
                mdot_door_ext = k_door_ext * Q_total * rho_source * door_ext_open_fraction

            # 2. Internal Door Logic
            target_door_int = 1.0 if door_int_open else 0.0
            if target_door_int > 0 or door_int_open_fraction > eps:
                Q_total, rho_source = calculate_door_infiltration_gosney(
                    T_room=T_room,
                    T_source=T_plant,  # Pass T_ambient OR T_plant here
                    P_Pa=P_Pa,
                    WS2M=wind_speed,
                    W_door=W_door_int,    # Pass A_door_ext OR A_door_int
                    H_door=H_door_int*door_int_open_fraction,    # Pass H_door_ext OR H_door_int
                    R_dry=R_dry,
                    is_outdoor_door=False,
                    )
                door_int_open_fraction += (target_door_int - door_int_open_fraction) * dt_internal / tau_door_int
                door_int_open_fraction = manual_clipping(door_int_open_fraction, 0.0, 1.0)
                door_int_open_fraction = max(door_int_open_fraction, SAFE_FLOOR)
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
            Q_evap_fruit = m_transp_rate * h_fg_room

            # =========================================================
            # 6. RATE EQUATIONS
            # =========================================================
            heat_balance = (
                Q_walls_ext + Q_walls_int + Q_roof
                + Q_floor
                + Q_fans
                + Q_fruit_exchange
                + Q_inf_sens_ext + Q_inf_sens_int
                - Q_cooling_sensible
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

            # batch-specific water loss
            for b in range(len(active_batch_mask)):
                if active_batch_mask[b] == 1:
                    m_transp_rate_batch = max(k_p * batch_masses[b] * (P_sat_pulp - P_w_room), 0.0)
                    batch_masses[b] -= m_transp_rate_batch * dt_internal
                    batch_masses[b] = max(batch_masses[b], fruit_mass_floor_kg)

            T_pulp += dT_pulp

            dW = dm_water / m_air_room
            W_room += dW
            W_room = max(W_room, 0.0)

            T_room += heat_balance * dt_internal

            # =========================================================
            # 8. RECOMPUTE DERIVED ROOM HUMIDITY
            # =========================================================
            P_w_room = partial_pressure_from_w(W_room, P_Pa)
            P_sat_room = p_sat_magnus(T_room)
            RH_room = manual_clipping(P_w_room / P_sat_room, 0.0, 1.0)

            # Air State Leaving the Coil
            T_outlet_air = T_coil_evap + BF * (T_room - T_coil_evap)
            
            # Humidity State for the NEXT step
            P_sat_coil = p_sat_magnus(T_coil_evap)
            W_coil_sat = w_from_partial_pressure(P_sat_coil, P_Pa)

            # Calculate RH_evap_out for 'real-life' telemetry
            W_evap_out = BF * W_room + (1.0 - BF) * W_coil_sat
            P_w_evap_out = partial_pressure_from_w(W_evap_out, P_Pa)
            P_sat_evap_out = p_sat_magnus(T_outlet_air)
            RH_evap_out = manual_clipping(P_w_evap_out / P_sat_evap_out, 0.0, 1.0)

            # =========================================================
            # 9. SENSOR UPDATES
            # =========================================================
            T_sensor += (T_room - T_sensor) * dt_internal / tau_sensor
            RH_room_sensor += (RH_room - RH_room_sensor) * dt_internal / tau_humid_sensor

            # ### DEBUG
            # if (np.isnan(T_room) or np.isnan(T_pulp) or np.isnan(W_room)
            #     or np.isnan(fruit_mass_kg) or np.isnan(CO2_ppm) or np.isnan(O2_pct)):
            #     print("=== NaN DETECTED ===")
            #     print("i=", i, "j=", j)
            #     print("T_room=", T_room, "T_pulp=", T_pulp, "T_sensor=", T_sensor)
            #     print("W_room=", W_room, "RH_room=", RH_room, "P_w_room=", P_w_room)
            #     print("fruit_mass_kg=", fruit_mass_kg, "delta_mass=", delta_mass, "old_mass=", old_mass)
            #     print("CO2_ppm=", CO2_ppm, "O2_pct=", O2_pct)
            #     print("heat_balance=", heat_balance, "dm_water=", dm_water, "dT_pulp=", dT_pulp)
            #     print("Q_cooling_sensible=", Q_cooling_sensible, "Q_cooling_latent=", Q_cooling_latent)
            #     print("Q_cooling_capacity=", Q_cooling_capacity, "Q_cooling_actual=", Q_cooling_actual)
            #     print("T_coil_evap=", T_coil_evap, "W_coil_sat=", W_coil_sat)
            #     print("m_removed=", m_removed, "m_humidifier=", m_humidifier, "m_transp_rate=", m_transp_rate)
            #     print("Q_walls_ext=", Q_walls_ext, "Q_roof=", Q_roof, "Q_fans=", Q_fans)
            #     print("Q_fruit_exchange=", Q_fruit_exchange, "Q_respiration=", Q_respiration)
            #     print("Q_inf_sens_ext=", Q_inf_sens_ext, "Q_inf_sens_int=", Q_inf_sens_int)
            #     print("mdot_leak=", mdot_leak, "mdot_door_ext=", mdot_door_ext, "mdot_door_int=", mdot_door_int)
            #     print("C_room=", C_room, "C_fruit=", C_fruit, "m_air_room=", m_air_room)
            #     print("cooling_frac=", cooling_frac, "cooling_call=", cooling_call)
            #     print("condense_frac=", condense_frac, "humidifier_frac=", humidifier_frac)
            #     print("T_ambient=", T_ambient, "RH_ambient=", RH_ambient, "P_Pa=", P_Pa)
            #     print("rho_air_room=", rho_air_room, "V_free=", V_free, "f_free=", f_free)
            #     print("R_moist_room=", R_moist_room, "P_w_ambient=", P_w_ambient)
            #     print("W_coil_sat=", W_coil_sat, "P_sat_coil=", P_sat_coil)
            #     print("latent_scale=", latent_scale, "Q_latent_headroom=", Q_latent_headroom)
            #     print("====================")
            #     break
            # ### DEBUG

            # =========================================================
            # 10. OUTPUT WRITES
            # =========================================================
            # write true values and/or noisy sensor values to arrays
            epoch_timestamp_arr[k] = epoch_timestamp

            T_room_arr[k] = T_room
            T_sensor_arr[k] = T_sensor
            cooling_frac_arr[k] = cooling_frac

            condense_frac_arr[k] = condense_frac

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
            Q_cooling_w_arr[k] = Q_cooling_actual

            Q_sensible_w_arr[k] = Q_cooling_sensible
            Q_latent_w_arr[k] = Q_cooling_latent

            Q_cooling_capacity_arr[k] = Q_cooling_capacity
            
            T_coil_evap_arr[k] = T_coil_evap
            T_evap_out_arr[k] = T_outlet_air
            RH_evap_out_arr[k] = RH_evap_out
            m_dot_air_evap_arr[k] = m_dot_air_evap

            fruit_mass_kg_arr[k] = fruit_mass_kg

            # ints
            cooling_call_arr[k] = int(cooling_call)
            humidifier_call_arr[k] = int(humidifier_call)

            door_int_open_arr[k] = door_int_open
            door_ext_open_arr[k] = door_ext_open

            epoch_timestamp += dt_internal

        T_plant_target = T_plant_a + T_plant_b*T_ambient
        T_plant += (dT_plant/tau_plant) * (T_plant_target - T_plant)

        # batch_masses[active_mask] = active_batch_masses # Write the updated view back into the main array

    if is_first_run:
        is_first_run = False

    telemetry_array_dict_float64['epoch_timestamp'] = epoch_timestamp_arr
    telemetry_array_dict_float64['T_room'] = T_room_arr
    telemetry_array_dict_float64['T_sensor'] = T_sensor_arr
    telemetry_array_dict_float64['cooling_frac'] = cooling_frac_arr

    telemetry_array_dict_float64['condense_frac'] = condense_frac_arr

    telemetry_array_dict_float64['CO2_ppm'] = CO2_ppm_arr
    telemetry_array_dict_float64['O2_pct'] = O2_pct_arr

    telemetry_array_dict_float64['T_pulp'] = T_pulp_arr
    telemetry_array_dict_float64['weight_loss_pct'] = weight_loss_pct_arr

    telemetry_array_dict_float64['m_humidifier'] = m_humidifier_arr

    telemetry_array_dict_float64['W_room'] = W_room_arr
    telemetry_array_dict_float64['RH_room'] = RH_room_arr
    telemetry_array_dict_float64['RH_room_sensor'] = RH_room_sensor_arr
    telemetry_array_dict_float64['m_transp_rate'] = m_transp_rate_arr

    telemetry_array_dict_float64['P_sat_pulp'] = P_sat_pulp_arr
    telemetry_array_dict_float64['P_w_room'] = P_w_room_arr

    telemetry_array_dict_float64['m_removed'] = m_removed_arr

    telemetry_array_dict_float64['humidifier_frac'] = humidifier_frac_arr

    telemetry_array_dict_float64['COP'] = COP_arr
    telemetry_array_dict_float64['W_compressor_kw'] = W_compressor_kw_arr
    telemetry_array_dict_float64['Q_condenser_kw'] = Q_condenser_kw_arr
    telemetry_array_dict_float64['Q_cooling_w'] = Q_cooling_w_arr

    telemetry_array_dict_float64['Q_cooling_sensible_w'] = Q_sensible_w_arr
    telemetry_array_dict_float64['Q_cooling_latent_w'] = Q_latent_w_arr
    telemetry_array_dict_float64['Q_cooling_capacity_w'] = Q_cooling_capacity_arr

    telemetry_array_dict_float64['T_coil_evap'] = T_coil_evap_arr

    telemetry_array_dict_float64['T_evap_in'] = T_sensor_arr
    telemetry_array_dict_float64['T_evap_out'] = T_evap_out_arr
    telemetry_array_dict_float64['RH_evap_in'] = RH_room_sensor_arr
    telemetry_array_dict_float64['RH_evap_out'] = RH_evap_out_arr
    telemetry_array_dict_float64['m_dot_air_evap'] = m_dot_air_evap_arr

    telemetry_array_dict_float64["fruit_mass_kg"] = fruit_mass_kg_arr

    telemetry_array_dict_int8['cooling_call'] = cooling_call_arr
    telemetry_array_dict_int8['humidifier_call'] = humidifier_call_arr
    telemetry_array_dict_int8['door_int_open'] = door_int_open_arr
    telemetry_array_dict_int8['door_ext_open'] = door_ext_open_arr

    ## Last values

    # Fracs
    last_state_scalar_dict_float64['cooling_frac_init'] = cooling_frac
    last_state_scalar_dict_float64['humidifier_frac_init'] = humidifier_frac
    last_state_scalar_dict_float64['condense_frac_init'] = condense_frac
    
    # Temperature
    last_state_scalar_dict_float64['T_room_init'] = T_room
    last_state_scalar_dict_float64['T_sensor_init'] = T_sensor
    last_state_scalar_dict_float64['T_plant_init'] = T_plant
    last_state_scalar_dict_float64['T_plant_target_init'] = T_plant_target
    last_state_scalar_dict_float64['T_pulp_init'] = T_pulp
    
    # Gases
    last_state_scalar_dict_float64['CO2_ppm_init'] = CO2_ppm
    last_state_scalar_dict_float64['O2_pct_init'] = O2_pct
    
    # Humidity parameters
    last_state_scalar_dict_float64['W_room_init'] = W_room
    last_state_scalar_dict_float64['P_w_room_init'] = P_w_room
    last_state_scalar_dict_float64['RH_room_init'] = RH_room
    last_state_scalar_dict_float64['RH_room_sensor_init'] = RH_room_sensor
    last_state_scalar_dict_float64['W_coil_sat_init'] = W_coil_sat
    
    # Logistics
    last_state_scalar_dict_float64['door_ext_open_fraction_init'] = door_ext_open_fraction
    last_state_scalar_dict_float64['door_int_open_fraction_init'] = door_int_open_fraction
    last_state_scalar_dict_float64['ext_open_until_init'] = ext_open_until
    last_state_scalar_dict_float64['int_open_until_init'] = int_open_until
    last_state_array_dict_float64['batch_masses_init'] = batch_masses
    last_state_array_dict_float64['batch_incoming_temps_init'] = batch_incoming_temps
    last_state_array_dict_int8['batch_quality_grades_init'] = batch_quality_grades
    last_state_array_dict_int8['active_batch_mask_init'] = active_batch_mask
    last_state_array_dict_int64['batch_ids_init'] = batch_ids
    last_state_scalar_dict_int64['global_step_init'] = global_step_offset + n
    last_state_scalar_dict_int64['n_active_init'] = n_active
    last_state_scalar_dict_int64['next_batch_id_init'] = next_batch_id
    last_state_scalar_dict_int64['front_idx_init'] = front_idx
    last_state_scalar_dict_int64['back_idx_init'] = back_idx
    
    # Fruit mass
    last_state_scalar_dict_float64['fruit_mass_kg_init'] = fruit_mass_kg
    last_state_scalar_dict_float64['total_water_loss_kg_init'] = total_water_loss_kg

    # Controllers
    last_state_scalar_dict_int8['is_first_run'] = np.int8(is_first_run)
    last_state_scalar_dict_int8['cooling_call_init'] = np.int8(cooling_call)
    last_state_scalar_dict_int8['humidifier_call_init'] = np.int8(humidifier_call)

    # Next_start
    last_state_scalar_dict_float64['last_epoch_timestamp'] = epoch_timestamp_arr[-1]

    return (
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
    )

def expand_minute_timestamps_to_internal(dt_arr, dt_internal):
    offsets = np.arange(0, 60, int(dt_internal), dtype="timedelta64[s]")
    # offsets = np.arange(0, 60, dt_internal, dtype="timedelta64[s]")
    expanded = (dt_arr[:, None] + offsets[None, :]).reshape(-1)
    return expanded

def build_batch_event_tables(
    plant_id,
    fruit_type,
    arrivals_arr,
    dispatches_arr,
):
    arrivals_valid_indices = np.flatnonzero(arrivals_arr[:, 0] != SENTINEL_FLOAT64)
    arrivals_arr_cleaned = arrivals_arr[arrivals_valid_indices, :]

    dispatches_valid_indices = np.flatnonzero(dispatches_arr[:, 0] != SENTINEL_FLOAT64)
    dispatches_arr_cleaned = dispatches_arr[dispatches_valid_indices, :]

    num_arrivals = len(arrivals_valid_indices)
    num_dispatches = len(dispatches_valid_indices)
    num_events = num_arrivals + num_dispatches

    # to be found in arrivals_arr
    batch_ids_arrival = arrivals_arr_cleaned[:, 0]
    timestamps_arrival = arrivals_arr_cleaned[:, 1]
    quality_grades_arrival = arrivals_arr_cleaned[:, 2]
    incoming_temps_arrival = arrivals_arr_cleaned[:, 4]
    masses_kg_arrival = arrivals_arr_cleaned[:, 3]

    # Create final full arrays
    remaining_masses_kg = dispatches_arr_cleaned[:, 5]
    fully_dispatched_indices_dispatches_arr = np.flatnonzero(remaining_masses_kg <= MASS_KG_FLOOR)

    batch_ids_final_dispatch = dispatches_arr_cleaned[fully_dispatched_indices_dispatches_arr, 0]
    timestamps_final_dispatch = dispatches_arr_cleaned[fully_dispatched_indices_dispatches_arr, 1]
    quality_grades_final_dispatch = dispatches_arr_cleaned[fully_dispatched_indices_dispatches_arr, 2]
    masses_kg_at_final_dispatch = dispatches_arr_cleaned[fully_dispatched_indices_dispatches_arr, 3]

    all_batch_ids = np.union1d(batch_ids_arrival, batch_ids_final_dispatch)
    arrived_indices = np.searchsorted(all_batch_ids, batch_ids_arrival)
    fully_dispatched_indices = np.searchsorted(all_batch_ids, batch_ids_final_dispatch)

    batch_ids_final_dispatch_only = np.setdiff1d(batch_ids_final_dispatch, batch_ids_arrival)
    fully_dispatched_only_indices_dispatches_arr = np.searchsorted(batch_ids_final_dispatch, batch_ids_final_dispatch_only)
    fully_dispatched_only_indices_full_arr = np.searchsorted(all_batch_ids, batch_ids_final_dispatch_only)

    num_batch_ids = len(all_batch_ids)

    arrival_ts_arr = np.full(num_batch_ids, SENTINEL_FLOAT64)
    final_dispatch_ts_arr = np.full(num_batch_ids, SENTINEL_FLOAT64)
    mass_kg_initial_arr = np.full(num_batch_ids, SENTINEL_FLOAT64)
    mass_kg_at_final_dispatch_arr = np.full(num_batch_ids, SENTINEL_FLOAT64)
    tunnel_exit_temp_c_arr = np.full(num_batch_ids, SENTINEL_FLOAT64)
    quality_grade_arr = np.full(num_batch_ids, SENTINEL_INT8)

    # found in arrival events
    arrival_ts_arr[arrived_indices] = timestamps_arrival
    mass_kg_initial_arr[arrived_indices] = masses_kg_arrival
    tunnel_exit_temp_c_arr[arrived_indices] = incoming_temps_arrival
    quality_grade_arr[arrived_indices] = quality_grades_arrival

    # found in final dispatch events
    final_dispatch_ts_arr[fully_dispatched_indices] = timestamps_final_dispatch
    mass_kg_at_final_dispatch_arr[fully_dispatched_indices] = masses_kg_at_final_dispatch

    # found in final dispatch events that arrived in previous run
    quality_grade_arr[fully_dispatched_only_indices_full_arr] = quality_grades_final_dispatch[fully_dispatched_only_indices_dispatches_arr]

    batches_dict = {
        "plant_id": np.full(num_batch_ids, plant_id),
        "fruit_type": np.full(num_batch_ids, fruit_type),
        "batch_id": all_batch_ids,
        "arrival_ts": arrival_ts_arr,
        "final_dispatch_ts": final_dispatch_ts_arr,
        "mass_kg_initial": mass_kg_initial_arr,
        "mass_kg_before_clearing": mass_kg_at_final_dispatch_arr,
        "tunnel_exit_temp_c": tunnel_exit_temp_c_arr,
        "quality_grade": quality_grade_arr.astype(np.int8),
    }

    events_dict = {
        "plant_id": np.full(num_events, plant_id),
        "fruit_type": np.full(num_events, fruit_type),
        "batch_id": np.concatenate((arrivals_arr_cleaned[:, 0], dispatches_arr_cleaned[:, 0])),
        "timestamp": np.concatenate((arrivals_arr_cleaned[:, 1], dispatches_arr_cleaned[:, 1])),
        "event_type": np.concatenate((
            np.full(num_arrivals, "arrival"),
            np.full(num_dispatches, "dispatch"),
        )),
        "mass_kg": np.concatenate((arrivals_arr_cleaned[:, 3], dispatches_arr_cleaned[:, 4])),
        "tunnel_exit_temp_c": np.concatenate((arrivals_arr_cleaned[:, 4], np.full(num_dispatches, SENTINEL_FLOAT64))),
        "quality_grade": np.concatenate((arrivals_arr_cleaned[:, 2], dispatches_arr_cleaned[:, 2])).astype(np.int8),
    }

    batches_df = pd.DataFrame(batches_dict)
    events_df = pd.DataFrame(events_dict)

    float_cols_common = ["tunnel_exit_temp_c"]
    for col in float_cols_common:
        batches_df[col] = batches_df[col].replace(SENTINEL_FLOAT64, np.nan)
        events_df[col] = events_df[col].replace(SENTINEL_FLOAT64, np.nan)

    float_cols_batches = ["mass_kg_initial", "mass_kg_before_clearing"]
    for col in float_cols_batches:
        batches_df[col] = batches_df[col].replace(SENTINEL_FLOAT64, np.nan)

    float_cols_events = ["mass_kg"]
    for col in float_cols_events:
        events_df[col] = events_df[col].replace(SENTINEL_FLOAT64, np.nan)

    timestamp_cols_batches = ["arrival_ts", "final_dispatch_ts"]
    for col in timestamp_cols_batches:
        clean_series = batches_df[col].replace(SENTINEL_FLOAT64, np.nan)
        batches_df[col] = pd.to_datetime(
            clean_series,
            unit='s',
            errors='coerce',
        )

    timestamp_cols_events = ["timestamp"]
    for col in timestamp_cols_events:
        clean_series = events_df[col].replace(SENTINEL_FLOAT64, np.nan)
        events_df[col] = pd.to_datetime(
            clean_series,
            unit='s',
            errors='coerce',
        )

    batches_df['batch_id'] = batches_df['batch_id'].astype('Int64')
    events_df['batch_id'] = events_df['batch_id'].astype('Int64')

    grade_map = {0: 'A', 1: 'B', 2: 'C', SENTINEL_INT8: pd.NA}
    batches_df['quality_grade'] = batches_df['quality_grade'].map(grade_map).astype(pd.StringDtype())
    events_df['quality_grade'] = events_df['quality_grade'].map(grade_map).astype(pd.StringDtype())

    batches_df = batches_df.sort_values(by=["arrival_ts", "final_dispatch_ts", "batch_id"], ignore_index=True)
    events_df = events_df.sort_values(by=["timestamp", "batch_id"], ignore_index=True)

    batches_round_spec = {
        "tunnel_exit_temp_c": 2,
        "mass_kg_initial": 3,
        "mass_kg_before_clearing": 3,
    }

    events_round_spec = {
        "tunnel_exit_temp_c": 2,
        "mass_kg": 3,
    }

    batches_df = batches_df.round(batches_round_spec)
    events_df = events_df.round(events_round_spec)

    return batches_df, events_df

def build_telemetry_table(
    plant_id,
    outputs_dict,
    calls_dict,
    f_RH_noise,
    fruit_type,
    m_dot_air_theoretical,
    rng,
):
    all_datetimes = pd.to_datetime(outputs_dict['epoch_timestamp'], unit='s')

    # Define target grain as a mask
    target_grain_mask = (all_datetimes.dt.second == 0).values

    # Apply mask for the target grain
    datetimes = all_datetimes[target_grain_mask]

    cooling_call = calls_dict['cooling_call'][target_grain_mask]
    humidifier_call = calls_dict['humidifier_call'][target_grain_mask]
    door_int_open = calls_dict['door_int_open'][target_grain_mask]
    door_ext_open = calls_dict['door_ext_open'][target_grain_mask]

    cooling_frac = outputs_dict['cooling_frac'][target_grain_mask]
    T_room = outputs_dict['T_sensor'][target_grain_mask]
    T_pulp = outputs_dict['T_pulp'][target_grain_mask]
    RH_room_frac = outputs_dict['RH_room_sensor'][target_grain_mask]
    CO2_ppm = outputs_dict['CO2_ppm'][target_grain_mask]
    O2_pct = outputs_dict['O2_pct'][target_grain_mask]
    W_compressor_kw = outputs_dict['W_compressor_kw'][target_grain_mask]
    T_evap_in = outputs_dict['T_room'][target_grain_mask]
    T_evap_out = outputs_dict['T_evap_out'][target_grain_mask]
    RH_evap_in_frac = outputs_dict['RH_room'][target_grain_mask]
    RH_evap_out_frac = outputs_dict['RH_evap_out'][target_grain_mask]
    T_coil_evap = outputs_dict['T_coil_evap'][target_grain_mask]
    m_dot_air_evap = outputs_dict['m_dot_air_evap'][target_grain_mask]
    fruit_mass_kg = outputs_dict['fruit_mass_kg'][target_grain_mask]

    sigma_cooling_frac = np.where(cooling_call == 1, 0.01, 0.02)
    cooling_frac_noisy = cooling_frac + rng.normal(0.0, sigma_cooling_frac, size=cooling_frac.shape)
    cooling_pct_noisy = np.clip(cooling_frac_noisy*100.0, 0.0, 100.0)

    sigma_T_room = np.where(cooling_call == 1, 0.08, 0.15)
    T_room_noisy = T_room + rng.normal(0.0, sigma_T_room, size=T_room.shape)

    sigma_T_pulp = .04
    T_pulp_noisy = T_pulp + rng.normal(0.0, sigma_T_pulp, size=T_pulp.shape)

    RH_room_pct = RH_room_frac * 100.0
    sigma_RH_room_pct = 0.5 + f_RH_noise * np.maximum(0.0, (RH_room_pct - 85.0) / 15.0)
    RH_room_pct_noisy = RH_room_pct + rng.normal(0.0, sigma_RH_room_pct, size=RH_room_pct.shape)
    RH_room_pct_noisy = np.clip(RH_room_pct_noisy, 0.0, 100.0)

    sigma_CO2_ppm = 0.005*CO2_ppm + 1.0
    CO2_ppm_noisy = CO2_ppm + rng.normal(0.0, sigma_CO2_ppm, size=CO2_ppm.shape)
    CO2_ppm_noisy = np.maximum(CO2_ppm_noisy, 0.0)

    sigma_O2_pct = 0.05
    O2_pct_noisy = O2_pct + rng.normal(0.0, sigma_O2_pct, size=O2_pct.shape)
    O2_pct_noisy = np.maximum(O2_pct_noisy, 0.0)

    sigma_W_compressor_kw = np.maximum(0.005 * W_compressor_kw, 0.02)
    W_compressor_kw_noisy = W_compressor_kw + rng.normal(0.0, sigma_W_compressor_kw, size=W_compressor_kw.shape)
    W_compressor_kw_noisy = np.maximum(W_compressor_kw_noisy, 0.0)

    sigma_T_evap_in = np.where(cooling_call == 1, 0.08, 0.15)
    T_evap_in_noisy = T_evap_in + rng.normal(0.0, sigma_T_evap_in, size=T_evap_in.shape)

    sigma_T_evap_out = np.where(cooling_call == 1, 0.10, 0.18)
    T_evap_out_noisy = T_evap_out + rng.normal(0.0, sigma_T_evap_out, size=T_evap_out.shape)

    RH_evap_in_pct = RH_evap_in_frac * 100.0
    sigma_RH_evap_in_pct = 0.5 + f_RH_noise * np.maximum(0.0, (RH_evap_in_pct - 85.0) / 15.0)
    RH_evap_in_pct_noisy = RH_evap_in_pct + rng.normal(0.0, sigma_RH_evap_in_pct, size=RH_evap_in_pct.shape)
    RH_evap_in_pct_noisy = np.clip(RH_evap_in_pct_noisy, 0.0, 100.0)

    RH_evap_out_pct = RH_evap_out_frac * 100.0
    sigma_RH_evap_out_pct = np.where(cooling_call == 1, 0.8, 0.5) + f_RH_noise * np.maximum(0, (RH_evap_out_pct - 85.0)/15.0)
    RH_evap_out_pct_noisy = RH_evap_out_pct + rng.normal(0.0, sigma_RH_evap_out_pct, size=RH_evap_out_pct.shape)
    RH_evap_out_pct_noisy = np.clip(RH_evap_out_pct_noisy, 0.0, 100.0)

    sigma_T_coil_evap = np.where(cooling_call == 1, 0.05, 0.15)
    T_coil_evap_noisy = T_coil_evap + rng.normal(0.0, sigma_T_coil_evap, size=T_coil_evap.shape)

    sigma_m_dot_air_evap = np.maximum(0.03 * m_dot_air_evap, 0.05)
    m_dot_air_evap_noisy = m_dot_air_evap + rng.normal(0.0, sigma_m_dot_air_evap, size=m_dot_air_evap.shape)
    m_dot_air_evap_noisy = np.minimum(m_dot_air_evap_noisy, m_dot_air_theoretical)
    evap_fan_speed_pct_noisy = (m_dot_air_evap_noisy/m_dot_air_theoretical)*100.0

    # Build telemetry table
    telemetry_df = pd.DataFrame({
        'plant_id': plant_id,
        'datetime': datetimes,
        'temp_room_c': T_room_noisy,
        'temp_pulp_c': T_pulp_noisy,
        'rh_room_pct': RH_room_pct_noisy,
        'co2_ppm': CO2_ppm_noisy,
        'o2_pct': O2_pct_noisy,
        'power_compressor_kw': W_compressor_kw_noisy,
        'temp_evap_inlet_c': T_evap_in_noisy,
        'temp_evap_outlet_c': T_evap_out_noisy,
        'rh_evap_inlet_pct': RH_evap_in_pct_noisy,
        'rh_evap_outlet_pct': RH_evap_out_pct_noisy,
        'evap_fan_speed_pct': evap_fan_speed_pct_noisy,
        'temp_coil_suction_c': T_coil_evap_noisy,
        'fruit_type': fruit_type,
        'fruit_mass_stored_kg': fruit_mass_kg,
        'comp_modulation_pct': cooling_pct_noisy,
        'compressor_on': cooling_call,
        'humidifier_on': humidifier_call,
        'door_int_open': door_int_open,
        'door_ext_open': door_ext_open,
    })
    
    # Specify number of decimals consistent with real-world sensor data
    round_spec = {
        'temp_room_c': 2, 'temp_pulp_c': 2, 'temp_evap_inlet_c': 2,
        'temp_evap_outlet_c': 2, 'temp_coil_suction_c': 2,
        'rh_room_pct': 2, 'rh_evap_inlet_pct': 2, 'rh_evap_outlet_pct': 2,
        'power_compressor_kw': 4, 'evap_fan_speed_pct': 1,
        'co2_ppm': 1, 'o2_pct': 3, 'fruit_mass_stored_kg': 0, 'comp_modulation_pct': 1,
    }
    telemetry_df = telemetry_df.round(round_spec)
    return telemetry_df

def fix_timestamps(df):
    for col in df.select_dtypes(include='datetime64[ns]').columns:
        df[col] = df[col].astype('datetime64[us]')
    return df

def split_df_into_daily_parquets(df):
    df['datetime'] = pd.to_datetime(df['datetime'])
    df = df.sort_values('datetime').set_index('datetime', drop=False)

    current = df['datetime'].iloc[0]
    end_dt = df['datetime'].iloc[-1]
    # run_id = uuid.uuid4().hex[:8]

    days_processed = 0

    while current < end_dt:
        midnight = current + pd.Timedelta(days=1)

        daily_df = df.loc[current:midnight].copy()

        daily_df['loaded_at'] = pd.Timestamp.now()
        daily_df = daily_df.reset_index(drop=True)

        year_str = current.strftime("%Y")
        month_str = current.strftime("%m")
        day_str = current.strftime("%d")

        dir_path = f"satellite/year={year_str}/month={month_str}/day={day_str}"
        os.makedirs(dir_path, exist_ok=True)

        file_name = f"satellite_data.parquet"
        full_path = os.path.join(dir_path, file_name)

        daily_df = fix_timestamps(daily_df)
        daily_df.to_parquet(full_path, engine='pyarrow')

        days_processed += 1
        current += pd.Timedelta(days=1)

    return f"Successfully generated {days_processed} daily Parquet packets."

def enforce_dtypes(df, dtypes, datetime_cols):
    for col in datetime_cols:
        df[col] = pd.to_datetime(df[col]).astype('datetime64[us]')
    return df.astype(dtypes)

