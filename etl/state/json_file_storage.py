import json
from typing import Any, Dict

from .base_storage import BaseStorage


class JsonFileStorage(BaseStorage):
    """
    Реализация хранилища, использующего локальный файл.
    Формат хранения: JSON
    """
    def __init__(self, file_path: str) -> None:
        self.file_path: str = file_path

    def save_state(self, state: Dict[str, Any]) -> None:
        """Сохранить состояние в хранилище."""
        with open(self.file_path, 'w') as file:
            json.dump(state, file)

    def retrieve_state(self) -> Dict[str, Any]:
        """Получить состояние из хранилища."""
        try:
            with open(self.file_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            return dict()
        except json.decoder.JSONDecodeError:
            return dict()
