import time
from functools import wraps


def backoff(exception_types=None, start_sleep_time=0.1, factor=2, border_sleep_time=30, timeout=None):
    """
    Функция для повторного выполнения функции через некоторое время, если возникла ошибка.
    Использует наивный экспоненциальный рост времени повтора (factor) до
    граничного времени ожидания (border_sleep_time)

    Формула:
        t = start_sleep_time * (factor ^ n), если t < border_sleep_time
        t = border_sleep_time, иначе
    :param start_sleep_time: начальное время ожидания
    :param factor: во сколько раз нужно увеличивать время ожидания на каждой итерации
    :param border_sleep_time: максимальное время ожидания
    :param exception_types:  список типов исключений, при которых вызов функции надо повторять
    :return: результат выполнения функции
    """

    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            num = 0
            t = start_sleep_time
            conn = None

            while t <= border_sleep_time:
                conn = func(*args, **kwargs)

                if conn is not None:
                    break

                t = start_sleep_time * factor ** num
                num = num + 1
                time.sleep(t)
            return conn
        return inner
    return func_wrapper
