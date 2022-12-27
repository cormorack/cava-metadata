import json
import hashlib

from pydantic import BaseModel, PrivateAttr
from typing import Optional


class InstrumentRequest(BaseModel):
    site: Optional[str]
    group: Optional[str]
    infrastructure: Optional[str]
    area: Optional[str]
    refdes: Optional[str]
    include_params: bool = False

    _key: str = PrivateAttr()

    def __init__(self, **data):
        super().__init__(**data)
        self._set_key()

    def _set_key(self):
        encoded_json = json.dumps(self.dict()).encode('utf-8')
        md5_hash = hashlib.md5(encoded_json)
        self._key = md5_hash.hexdigest()
