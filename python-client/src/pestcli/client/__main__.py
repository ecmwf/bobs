import fire
import httpx
import sys
from pestcli.client.client import create, write, close, read
from typing import Iterator, cast

default_url = "http://localhost:8010/"

def save_cli(url: str = default_url, data: str|bytes|None = None, stdin: bool = False, path: str|None = None) -> None:
    if int(data is not None) + int(stdin) + int(path is not None) != 1:
        raise ValueError("exactly one of data, stdin and path must be specified")
    if stdin:
        def datagen() -> Iterator[bytes]:
            for line in sys.stdin:
                yield line.encode('ascii')
    elif data is not None:
        def datagen() -> Iterator[bytes]:
            # since we need to assign, we need a new var; otherwise we'd have to `nonlocal data` first
            _data = cast(str|bytes, data)
            if isinstance(_data, str):
                _data = _data.encode('ascii')
            yield _data
    elif path is not None:
        def datagen() -> Iterator[bytes]:
            with open(path, 'rb') as f:
                while True:
                    line = f.read(1024)
                    if line:
                        yield line
            
    else:
        raise ValueError("unexpected internal state")

    with httpx.Client() as cli:
        key = create(cli, url)['key']
        print(f"{key = }")
        write(cli, url, key, 0, datagen())
        close(cli, url, key)

def read_cli(key: str, url: str = default_url) -> None:
    with httpx.Client() as cli:
        for chunk in read(cli, url, key, 0, 0):
            # TODO how to write bytes? Eg if we are piped
            sys.stdout.write(chunk.decode('ascii'))

if __name__ == "__main__":
    fire.Fire({'save': save_cli, 'read': read_cli})
