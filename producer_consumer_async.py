## 생산자 소비자 패턴 theading

import asyncio
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import multiprocessing.managers
import threading
import time
import queue
import multiprocessing

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
concurrent.futures
https://docs.python.org/ko/3.7/library/concurrent.futures.html

run_in_executor 는 동기 함수를 비동기 루프에서 실행할 수 있게 해줌.
실제 함수는 executor workers 에서 동기적으로 실행.
non-preempt = max_workers 수만큼만 동시에 실행됨.
            나머지는 작업 큐에서 대기.
            작업이 끝난 worker 가 대기중 작업을 큐에서 꺼내서 실행.

일반 스레드에서는 event.wait() 나 queue.get() 으로 자원을 계속 낭비하는 것을 막았음. 
PoolExecutor 가 worker 자원을 효율적으로 관리해줌 (작업이 없을경우 worker 가 cpu 자원 소모 안함함)

executor 재사용하고 싶으면 그냥 executor 반환 (with 으로 열었을때는 작업이 끝나면 닫힘.)
나중에 다시 사용할때 worker 스레드를 재생성해야하므로 로드가 걸림?
executor 는 모든 작업이 끝나고 꼭 닫아야함!!

executor.submit(func, *args)
    -> 동기 api. future 객체 반환. 
loop.run_in_executor(executor, func, *args)
    -> 비동기 api. 반환값 await 가능. async 함수 내에서 사용.
    -> blocking 코드를 이벤트 루프 블록 시키지 않고 실행 가능.

threadpoolexecutor vs processpoolexecutor
스레드풀은 GIL 공유. I/O 바운드에 적합
프로세스풀은 독립적 파이선 인터프리터. cpu 바운드 작업 병렬처리.
하지만 프로세스풀은 초기화 및 종료가 느림.

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
        
    ## option 2.
    # executor = ThreadPoolExecutor(max_workers=2)
    # loop = asyncio.get_running_loop()
    # print("loop execute start")

    # await asyncio.gather(
    #     loop.run_in_executor(executor, producer, "produce 1"),
    #     loop.run_in_executor(executor, consumer, "consumer"),
    #     loop.run_in_executor(executor, producer, "produce 2"),
    #     )
    
    # print("loop execute done")
    # executor.shutdown(wait=True)




def producer_proc(name, q):
    for i in range(5):
        # print(f"[{threading.current_thread().name}] Producing {i}")
        print(f"[{name}] Producing {i}")
        q.put(i)
        time.sleep(1)
    q.put(None)

def consumer_proc(name, q):
    while True:
        item = q.get()
        if item is None:
            break
        print(f"[{name}] Consumed {item}")
        time.sleep(2)


"""
ProcessPoolExecutor 는 producer-consumer 에 적합하지 않음.
pickle 가능한 객체만 실행,반환 가능
It uses standard "pickle" to serialize all arguments.
(multiprocessing.queue 안에 lock, pipe 등 pickle 가능하지 않은 것들이 있음.)
멀티프로세스에서는 queue가 왜 됨?
    -> linux 에서는 memory space 를 받아서 사용. (피클 x)
    -> windows 에선
        multiprocessing.Queue is picklable via a special method: 
        it serializes a reference to the internal pipe and 
        lock using handles that Windows can duplicate.
        This is handled explicitly in multiprocessing.reduction module, 
        which contains logic for reducing (pickling) 
        IPC primitives like Queue, Pipe, Lock.
        
manager.queue() 로 구현은 가능. (프록시 큐)
효율적이지 않음.
"""
async def processPool():
    ### ERROR !!!!!!!!!!!!!!!!!! ###
    # q = multiprocessing.Queue()  ### q 사용 금지 
    q = multiprocessing.Manager().Queue()
    
    with ProcessPoolExecutor(max_workers=2) as executor:
        loop = asyncio.get_running_loop()

        print("loop execute start")

        await asyncio.gather(
            loop.run_in_executor(executor, producer_proc, "produce 1", q),
            loop.run_in_executor(executor, consumer_proc, "consumer", q),
            loop.run_in_executor(executor, producer_proc, "produce 2", q),
            )
        
        print("loop execute done")

        
if __name__ == "__main__":
    # asyncio.run(producer_consumer_async())
    # asyncio.run(threadPool())
    asyncio.run(processPool())