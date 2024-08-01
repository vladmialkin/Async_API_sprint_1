from typing import Any


class State:
    """
    Класс для работы с состояниями.
    Работает с локальной копией состояния данных.
    Восстановливает состояние во время старта приложения, если такое состояние существовало.
    {'state_key': '1990-01-01'} - значение является datetime,
                                  храним последнюю дату, после которой
                                  мы загружали в ES
    """
    def __init__(self, storage) -> None:
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа."""
        state = self.storage.retrieve_state()
        state[key] = value
        self.storage.save_state(state)

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу."""
        return self.storage.retrieve_state().get(key)
