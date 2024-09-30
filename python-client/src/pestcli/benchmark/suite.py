from concurrent.futures import ProcessPoolExecutor, wait, Future
import httpx
import time
from pestcli.client.client import create, write, close, read
from typing import Iterator

chunk_size = 1024**2 # 1mb

def run_creates(keys: int, url: str) -> list[str]:
    with httpx.Client() as cli:
        return [
            cli.put(f"{url}create").json()['key']
            for _ in range(keys)
        ]

def run_writer(chunks: int, url: str, key: str) -> None:

    def gen() -> Iterator[bytes]:
        i = 0
        data = b'a'*chunk_size
        while i < chunks:
            yield data
            i += 1
        
    with httpx.Client() as cli:
        start = time.perf_counter_ns()
        r1 = cli.post(f"{url}write/{key}/0", content=gen())
        if r1.status_code != 200:
            raise ValueError()

        r2 = cli.post(f"{url}close/{key}")
        if r2.status_code != 200:
            raise ValueError()
    
        end = time.perf_counter_ns()
        print(f"writing elapsed {(end-start)/1e9:.3f} sec")

def run_reader(chunks: int, url: str, key: str) -> None:
    with httpx.Client() as cli:
        start = time.perf_counter_ns()
        r = cli.get(f"{url}read/{key}/0/0")
        if r.status_code != 200:
            raise ValueError()
        remaining = chunks * chunk_size
        for chunk in r.iter_bytes():
            remaining -= len(chunk)
        if remaining != 0:
            raise ValueError(f"gotten {remaining} but expected 0")
        end = time.perf_counter_ns()
        print(f"reading elapsed {(end-start)/1e9:.3f} sec")

def run(
    dsc: int,
    dsl: int,
    pw: int,
    pr: int,
    url: str|None = None,
) -> None:
    wp = ProcessPoolExecutor(max_workers=pw)
    wr = ProcessPoolExecutor(max_workers=pr)
    if not url:
        url = "http://localhost:8010/"

    start = time.perf_counter_ns()
    keys = run_creates(dsc, url)
    futs1: list[Future] = [wp.submit(run_writer, dsl, url, k) for k in keys]
    futs2: list[Future] = [wr.submit(run_reader, dsl, url, k) for k in keys]
    wait(futs1 + futs2)
    end = time.perf_counter_ns()
    for f in futs1:
        if (e := f.exception()) is not None:
            print(f"exception in writer: {e}")
    for f in futs2:
        if (e := f.exception()) is not None:
            print(f"exception in reader: {e}")
    print(f"whole thing elapsed {(end-start)/1e9:.3f} sec")

