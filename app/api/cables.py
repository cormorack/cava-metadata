# flake8: noqa

import json
from pathlib import Path

HERE = Path(__file__).parent.absolute()
cable_path = HERE / "rsn-cable.json"
with open(cable_path) as f:
    RSN_CABLE = json.load(f)
