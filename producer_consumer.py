## 생산자 소비자 패턴 theading

from threading import Thread
from queue import Queue
import time
import multiprocessing
import asyncio

## thread 
def producer_consumer_thread():
    q = Queue()

    def producer():
        for i in range(5):
            print(f"producer: {i}")
            q.put(i)
            time.sleep(1)
            
    def consumer():
        while True:
            item = q.get()
            
            if item is None:
                break
            print(f"Consumer : {item}")
            time.sleep(2)

    t1 = Thread(target=producer)
    t2 = Thread(target=consumer)

    t1.start()
    t2.start()

    t1.join()
    q.put(None)
    t2.join()

## thread using class
def producer_consumer_thread_class():
    class Producer(Thread):
        def __init__(self, name, q):
            super().__init__(name=name, daemon=True)
            self.q = q

        def run(self):
            for i in range(5):
                print(f"producer {self.name} : {i}")
                self.q.put(i)
                time.sleep(1)
            self.q.put(None)
        
        def join(self):
            super().join()
            print(f"producer {self.name} custom join.")
            
    class Consumer(Thread):
        def __init__(self, q):
            super().__init__(daemon=True)
            self.q = q

        def run(self):
            while True:
                item = self.q.get()
                if item is None:
                    break
                print(f"consumer : {item}")
                time.sleep(2)
        
        def join(self):
            super().join()
            print("consumer custom join.")
            
    q = Queue()
    t1 = Producer("prod1", q)
    t2 = Producer("prod2", q)
    t3 = Consumer(q)
    
    t1.start()
    t2.start()
    t3.start()
    
    t1.join()
    t2.join()
    t3.join()
        

## process
"""
멀티프로세스는 새로운 파이썬 인터프리터를 시작함.
multiprocessing.Queue 사용하기

windows 에서는 nested 함수는 멀티프로세스로 만들어지지 않음.
windows 에서의 multiprocess => spawn (타겟함수 pickle,  메인 모듈 임포트)
타겟 함수/클래스가 pickle 가능해야 하고 top-level 이여야 함.
nested 타겟은 pickle 불가능 -> WinError 87
linux 에서는 fork (부모 프로세스의 메모리를 복사함 (nested 함수포함))
그렇기 때문에 pickle, import 필요 없음

windows는 fork 를 지원하지 않음.
"""


def producer_process(q:multiprocessing.Queue):
    for i in range(5):
        print(f"producer {i}")
        q.put(i)
        time.sleep(1)
    q.put(None)
    
def consumer_process(q:multiprocessing.Queue):
    while True:
        item = q.get()
        if item is None:
            break
        print(f"consumer {item}")
        time.sleep(2)
        
def producer_consumer_process():

    q = multiprocessing.Queue()
    p1 = multiprocessing.Process(target=producer_process, args=(q,))
    p2 = multiprocessing.Process(target=consumer_process, args=(q,))
      
    p1.start()
    p2.start()

    print("process start")
    p1.join()
    print("p1 join")
    p2.join()
    print("p2 join")
    

## process class
class ProducerProcess(multiprocessing.Process):
    def __init__(self, name, q):
        super().__init__(name=name)
        self.q = q
        
    def run(self):
        for i in range(5):
            print(f"Producing {i}")
            self.q.put(i)
            time.sleep(1)
        self.q.put(None)
        
class ConsumerProcess(multiprocessing.Process):
    def __init__(self, name, q):
        super().__init__(name=name)
        self.q = q

    def run(self):
        while True:
            item = self.q.get()
            if item is None:
                break
            print(f"Consumed {item}")
            time.sleep(2)
            
def producer_consumer_process_class():
    q = multiprocessing.Queue()
    p1 = ProducerProcess("prod1", q)
    p2 = ConsumerProcess("consum", q)
    p1.start()
    p2.start()
    p1.join()
    p2.join()

        
if __name__ == "__main__":
    # producer_consumer_thread()
    # producer_consumer_thread_class()
    # producer_consumer_process()
    producer_consumer_process_class()
