import json
import numpy as np
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import numba
from numba import njit, types, typeof
from numba.typed import Dict

class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        # Converts full numpy arrays to standard nested Python lists
        if isinstance(obj, np.ndarray):
            return obj.tolist() 
        # Converts numpy data types/scalars (like np.int64) to native Python types
        if isinstance(obj, (np.void, np.number)):
            return obj.item()
        return super().default(obj)

def get_persistent_session():
    session = requests.Session()
    # Retry on 504, 502, 503, 500
    retry_strategy = Retry(
        total=5, 
        backoff_factor=2,
        status_forcelist=[500, 502, 503],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    return session

def create_typed_dict(
    key_type=types.unicode_type,
    value_type=types.float64,
    value_structure='scalar',
):
    if value_structure == 'scalar':
        return Dict.empty(
            key_type=key_type,
            value_type=value_type,
        )
    elif value_structure == 'array':
        return Dict.empty(
            key_type=key_type,
            value_type=value_type[:],
        )
    else:
        raise ValueError("Enter a valid data structure: 'scalar' or 'array'")

def plain_dict_to_typed_dict(plain_dict, empty_typed_dict):
    dict_type = typeof(empty_typed_dict)
    cast_key = dict_type.key_type # .get_python_type()
    cast_value = dict_type.value_type # .get_python_type()
    for key, value in plain_dict.items():
        empty_typed_dict[cast_key(key)] = cast_value(value)
    return empty_typed_dict

def typed_dict_to_plain_dict(typed_dict):
    return {k: typed_dict[k] for k in typed_dict}

def clean_rng_state(obj):
    """Recursively converts NumPy ints/arrays into JSON-serializable Python types."""
    if isinstance(obj, dict):
        return {k: clean_rng_state(v) for k, v in obj.items()}
    elif isinstance(obj, np.ndarray):
        return obj.tolist()  # Converts uint32 arrays into [1, 2, 3] lists
    elif isinstance(obj, (np.integer, np.floating)):
        return obj.item()    # Converts numpy scalars into native int/float
    return obj

def get_list_element_type(lst):
    """Recursively finds the type of the underlying data inside a nested list."""
    if isinstance(lst, list) and len(lst) > 0:
        return get_list_element_type(lst[0])
    return type(lst)

def process_kestra_payload(kestra_payload_dict):
    rng = np.random.default_rng()
    rng.bit_generator.state = kestra_payload_dict["rng_state"]

    data_states = kestra_payload_dict["data_states"]
    data_types = kestra_payload_dict["_types"]

    simulation_state = {}

    for key, dtype_str in data_types.items():
        np_scalar_type = getattr(np, dtype_str)

        raw_value = data_states[key]

        if isinstance(raw_value, list):
            simulation_state[key] = np.array(raw_value, dtype=np_scalar_type)
        else: 
            simulation_state[key] = np_scalar_type(raw_value)

    return simulation_state, rng

@njit
def wrapped_index(index, size):
    # double division to account for negative indices
    return ((index % size) + size) % size

class OldNumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super().default(obj)
