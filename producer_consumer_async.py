import asyncio
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




        
if __name__ == "__main__":
    asyncio.run(producer_consumer_async())
