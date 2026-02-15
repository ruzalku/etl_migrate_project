import json
import os
from typing import Any, Optional
from src.core.json_encoder import CustomJSONEncoder

class JSONStateManager:
    def __init__(self, file_path: str = 'state.json'):
        self.file_path = file_path

    def get_state(self, key: str) -> Optional[Any]:
        if not os.path.exists(self.file_path):
            return None
        try:
            with open(self.file_path, 'r') as f:
                return json.load(f).get(key)
        except (json.JSONDecodeError, IOError, AttributeError):
            return None

    def set_state(self, key: str, value: Any):
        data = {}
        if os.path.exists(self.file_path):
            try:
                with open(self.file_path, 'r') as f:
                    data = json.load(f)
            except json.JSONDecodeError:
                pass
        
        data[key] = value
        temp_file = f"{self.file_path}.tmp"
        with open(temp_file, 'w') as f:
            json.dump(data, f, indent=4, cls=CustomJSONEncoder)
        os.replace(temp_file, self.file_path)
