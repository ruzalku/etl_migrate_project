from json import JSONEncoder
from typing import Any
from datetime import date, datetime
from uuid import UUID


class CustomJSONEncoder(JSONEncoder):
    def default(self, o: Any) -> Any:
        if isinstance(o, (datetime, date)):
            return o.isoformat()
        
        if isinstance(o, UUID):
            return str(o)
        
        return JSONEncoder.default(self, o)
        
        return o
