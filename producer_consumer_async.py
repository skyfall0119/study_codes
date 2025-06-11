## 생산자 소비자 패턴 theading

import asyncio
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import threading
import time
import queue

"""
queue.Queue 는 동기/blocking이라 비동기에는 사용하지 않음
asyncio.Queue 는 비동기/non-blocking
"""

async def producer_consumer_async():
    q = asyncio.Queue()
    
    async def producer(q: asyncio.Queue):
        for i in range(5):
            print(f"Producing {i}")
            await q.put(i)
            await asyncio.sleep(1)  
        await q.put(None)  

    async def consumer(q: asyncio.Queue):
        while True:
            item = await q.get()
            if item is None:
                break
            print(f"Consuming {item}")
            await asyncio.sleep(2) 
    
    
    await asyncio.gather(
        producer(q),
        consumer(q)
    )



"""
ThreadPoolExecutor
쓰레드 풀을 사용하여 비동기 실행 관리. 여러 작업을 동시에 실행하여 I/O 작업 성능 향상
run_in_executor 는 동기, blocking 임 
non-preempt = max_workers 이상의 쓰레드를 실행시키면 max_workers 만큼 먼저 실행하고, 
            나머지 쓰레드는 큐에서 대기.
            작업이 끝나고 worker 를 release 해야 대기중인 쓰레드가 작업 가능.

"""
async def threadPool():
    q = queue.Queue()
    
    def producer(name):
        for i in range(5):
            # print(f"[{threading.current_thread().name}] Producing {i}")
            print(f"[{name}] Producing {i}")
            q.put(i)
            time.sleep(1)
            # await asyncio.sleep(1)
        q.put(None)

    def consumer(name):
        while True:
            item = q.get()
            if item is None:
                break
            print(f"[{name}] Consumed {item}")
            time.sleep(2)
            # await asyncio.sleep(2)
            
            
    

    with ThreadPoolExecutor(max_workers=2) as executor:
        loop = asyncio.get_running_loop()

        print("loop execute start")

        await asyncio.gather(
            loop.run_in_executor(executor, producer, "produce 1"),
            loop.run_in_executor(executor, consumer, "consumer"),
            loop.run_in_executor(executor, producer, "produce 2"),
            )
        
        print("loop execute done")


        
if __name__ == "__main__":
    # asyncio.run(producer_consumer_async())
    asyncio.run(threadPool())