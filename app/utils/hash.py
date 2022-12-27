import hashlib
import json
from typing import Dict, Any


def hash_dict(input_dict: Dict[Any, Any]) -> str:
    """
    Hash dictionary into md5 string

    Parameters
    ----------
    input_dict : dict
        The input dictionary to be hashed

    Returns
    -------
    str
        MD5 hash string
    """
    encoded_json = json.dumps(input_dict).encode('utf-8')
    md5_hash = hashlib.md5(encoded_json)
    return md5_hash.hexdigest()
