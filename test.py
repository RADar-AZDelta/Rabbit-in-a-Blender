# import random
# import time
# from concurrent.futures import ThreadPoolExecutor, wait
# from threading import Semaphore

# from tqdm import tqdm


# def worker(pos, sem):
#     t = random.random() * 0.05
#     with sem:
#         # for _ in tqdm(range(100), desc=f'pos {pos}', position=pos):
#         for _ in tqdm(range(100), desc=f"pos {pos}"):
#             time.sleep(t)


# def main():
#     with ThreadPoolExecutor() as executor:
#         sem = Semaphore(3)
#         # sem = Semaphore(10)
#         futures = []
#         for pos in range(10):
#             future = executor.submit(worker, pos, sem)
#             futures.append(future)

#         wait(futures)


# if __name__ == "__main__":
#     main()


import time

import tqdm

pbar = tqdm.tqdm(total=5)
for ii in range(5):
    pbar.write(f"Hello {ii}")
    pbar.update(1)
    time.sleep(1)
