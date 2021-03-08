import os
import json
import fsspec

from store import META
from core.config import METADATA_SOURCE


def load_instrument_catalog():
    fs = fsspec.filesystem("s3")
    with fs.open(
        os.path.join(METADATA_SOURCE, "instruments_catalog.json")
    ) as f:
        META.update({"instruments_catalog": json.load(f)})
