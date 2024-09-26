from pest.server import app
from starlette.testclient import TestClient
from typing import Iterator
import httpx

def test_status():
    with TestClient(app) as client:
        response = client.get("/status")
        assert response.status_code == 200
        response = client.head("/status")
        assert response.status_code == 200
        response = client.post("/status")
        assert response.status_code == 405

def test_save_read():
    with TestClient(app) as client:
        N = 4
        L = 128
        def content() -> Iterator[bytes]:
            for i in range(N):
                yield b"a"*L

        save_r = client.put("/save", content=content())
        key = save_r.json()['key']

        def get_n(n: int):
            res = b""
            read_r = client.get(f"/read/{key}/0/{n}")
            for chunk in read_r.iter_bytes():
                 res += chunk
            return res

        assert get_n(4) == (b"a"*L*N)[:4]
        assert get_n(0) == (b"a"*L*N)

