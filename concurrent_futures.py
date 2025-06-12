import asyncio
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import time
import queue
import multiprocessing


"""
concurrent.futures
https://docs.python.org/ko/3.7/library/concurrent.futures.html

run_in_executor 는 동기 함수를 비동기 루프에서 실행할 수 있게 해줌.
실제 함수는 executor workers 에서 동기적으로 실행.
non-preempt = max_workers 수만큼만 동시에 실행됨.
            나머지는 작업 큐에서 대기.
            작업이 끝난 worker 가 대기중 작업을 큐에서 꺼내서 실행.

일반 스레드에서는 event.wait() 나 queue.get() 으로 자원을 계속 낭비하는 것을 막았음. 
PoolExecutor 가 worker 자원을 효율적으로 관리해줌 (작업이 없을경우 worker 가 cpu 자원 소모 거의 안함)

executor 재사용하고 싶으면 그냥 executor 반환 
(with 으로 열었을때는 작업이 끝나면 닫힘.나중에 다시 사용할때 worker 스레드를 재생성해야하므로 로드가 걸림)
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

async def threadPool_prod_consum():
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



"""
ProcessPoolExecutor 는 producer-consumer 에 적합하지 않음.
    -> pickle 가능한 객체만 실행,반환 가능
    It uses standard "pickle" to serialize all arguments.
    -> multiprocessing.queue 안에 lock, pipe 등 pickle 가능하지 않은 요소 있음
    
멀티프로세스에서는 queue가 왜 됨?
    -> linux 에서는 fork 방식. 부모 프로세스의 memory space 를 그대로 받아서 사용. (피클 x)

    -> windows 에선
        - spawn 방식. (새로운 파이썬 인터프리터 실행 후 main 모듈을 임포트함.)
        - multiprocessing.Queue는 picklable 하도록 특별히 구현됨.
        - multiprocessing.reduction 모듈이 내부적으로 Pipe, Lock 등의 핸들을
          "핸들 복제(handle duplication)" 방식으로 직렬화해서 전달함.

대안
manager.queue() 로 구현은 가능. (프록시 큐)
효율적이지 않음.
    -> 내부적으로 프록시 서버를 통해 통신하므로 latency가 있음.

결론:
    - ProcessPoolExecutor는 공유 큐가 필요한 producer-consumer 구조에는 부적합
    - 대신 단방향 작업 분산(map, submit 등)에는 매우 적합
"""
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

async def processPool_prod_consum():
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


"""
보통..생산자 소비자 패턴이 아닌 일반적인 작업 병렬, 동시 처리를 위해 많이 사용.
time.sleep() 은 I/O blocking 같이 작동하기 때문에 sync에서도 병렬로 처리됐음.
threadpoolexecutor -> processpoolexecutor 로 바꿔도 그대로 작동 가능.
(대신 함수가 picklable , top level) 이여야 함. 지금처럼 nested 인 경우는 안됨 (windows) 
top-level 로 하는게 좋은 습관.
"""
def threadpool_job_sync():
    def calc_num(n):
        print(f"calculating {n}")
        time.sleep(1)
        return n*n

    numbers = range(10)

    start = time.time()
    with ThreadPoolExecutor(max_workers=3) as executor:
        results = executor.map(calc_num, numbers)
    end = time.time()
    print(f"total time {end-start}")
    print("results ", list(results))

#___print____
# total time 4.005882501602173
# results  [0, 1, 4, 9, 16, 25, 36, 49, 64, 81]


async def threadpool_job_async():
    def calc_num(n):
        print(f"calculating {n}")
        time.sleep(1)
        return n*n

    num = range(10)

    start = time.time()
    with ThreadPoolExecutor(max_workers=3) as executor:
        loop = asyncio.get_running_loop()
        
        # ## async
        # await asyncio.gather(
        #     loop.run_in_executor(executor, calc_num, 10),
        #     loop.run_in_executor(executor, calc_num, 50)
        # )
        
        # or
        tasks = [
            loop.run_in_executor(executor, calc_num, n) for n in num
        ]
        
        res = await asyncio.gather(*tasks)
    end = time.time()

    print(f"total time {end-start}")
    print("results ", list(res))

#___print____
# total time 4.011268138885498
# results  [0, 1, 4, 9, 16, 25, 36, 49, 64, 81]


if __name__ == "__main__":
    # asyncio.run(threadPool_prod_consum())
    # asyncio.run(processPool_prod_consum())
    # threadpool_job_sync()
    asyncio.run(threadpool_job_async())
    

