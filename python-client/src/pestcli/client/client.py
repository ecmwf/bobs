import httpx
from typing import Iterator

def create(cli: httpx.Client, url: str) -> dict:
    r = cli.put(f"{url}create")
    if r.status_code != 200:
        raise ValueError(r)
    return r.json()

def write(cli: httpx.Client, url: str, key: str, offset: int, datagen: Iterator[bytes]) -> None:
    r = cli.post(f"{url}write/{key}/{offset}", content=datagen)
    if r.status_code != 200:
        raise ValueError(r)

def close(cli: httpx.Client, url: str, key: str) -> None:
    r = cli.post(f"{url}close/{key}")
    if r.status_code != 200:
        raise ValueError(r)

def read(client: httpx.Client, url: str, key: str, start: int, end: int) -> Iterator[bytes]:
    r = client.get(f"{url}read/{key}/{start}/{end}")
    if r.status_code != 200:
        raise ValueError(r)
    for chunk in r.iter_bytes():
        yield chunk

