from functools import wraps
from asyncio import sleep
from random import normalvariate
import logging

logger = logging.getLogger(__name__)

def backoff(
    start_time: float = 2,
    end_time: float = 600,
    factor: float = 2,
    jitter: float = 0.1,
    max_attempts: int = 10,
    exceptions: tuple[type[Exception], ...] = (Exception, )
):
    def func_wrapper(func):
        @wraps(func)
        async def inner(*args, **kwargs):
            cur_delay = start_time
            counter = 0
            
            while counter <= max_attempts:
                try:
                    return await func(*args, **kwargs)
                except *exceptions as e:
                    counter += 1
                    jitter_time = normalvariate(mu=cur_delay, sigma=jitter * cur_delay)
                    delay = cur_delay + jitter_time
                    
                    logger.error(
                        f'Исключени: {counter}, '
                        f'пауза: {delay}'
                    )
                    
                    await sleep(delay=delay)
                    
                    cur_delay = min(cur_delay * factor, end_time)
        
        return inner
    return func_wrapper
                    
                    