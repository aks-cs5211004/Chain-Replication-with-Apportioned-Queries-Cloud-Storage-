import random
import time
from typing import Optional, Final, List

from craq.craq_server import CraqServer
from core.cluster import ClusterManager
from core.message import JsonMessage, JsonMessage
from core.network import TcpClient, ConnectionStub
from core.server import ServerInfo, Server
from collections import defaultdict


POOL_SZ = 32

class CraqClient:
    def __init__(self, infos: list[ServerInfo]) -> None:
        self.conns: list[TcpClient] = []
        for info in infos:
            conn = TcpClient(info, True, 1)
            self.conns.append(conn)
        self.response_times: dict[TcpClient, float] = {conn: 0.0 for conn in self.conns}

        
    def set(self, key: str, val: str) -> bool:
          response: Optional[JsonMessage] = self.conns[0].send(JsonMessage({"type": "SET", "key": key, "val": val}))
          assert response is not None
          return response["status"] == "OK"
        
    def get(self, key: str) -> tuple[bool, Optional[str]]:
        # server = self._get_random_server()
        server = self._get_least_loaded_server()
        start_time = time.time()
        response: Optional[JsonMessage] = server.send(JsonMessage({"type": "GET", "key": key}))
        assert response is not None
        elapsed_time = time.time() - start_time
        self.response_times[server] = elapsed_time * 0.3 + self.response_times[server] * 0.7
        if response["status"] == "OK":
          return True, response["val"]
        return False, None
    
    def _get_least_loaded_server(self) -> TcpClient:
        return min(self.response_times, key=self.response_times.get)

    def _get_random_server(self) -> TcpClient:
        server = random.choice(self.conns)
        return server

class CraqCluster(ClusterManager):
  def __init__(self) -> None:
    self.a = ServerInfo("a", "localhost", 9900)
    self.b = ServerInfo("b", "localhost", 9901)
    self.c = ServerInfo("c", "localhost", 9902)
    self.d = ServerInfo("d", "localhost", 9903)

    self.prev: dict[ServerInfo, Optional[ServerInfo]] = {
      self.a: None,
      self.b: self.a,
      self.c: self.b,
      self.d: self.c,
    }
    self.next: dict[ServerInfo, Optional[ServerInfo]] = {
      self.a: self.b,
      self.b: self.c,
      self.c: self.d,
      self.d: None,
    }

    super().__init__(
      master_name="d",
      topology={self.a: {self.b, self.d},
                self.b: {self.c, self.d},
                self.c: {self.d},
                self.d: set()},
      sock_pool_size=POOL_SZ,
    )

  def connect(self, craq: bool = False) -> CraqClient:
    return CraqClient([self.a, self.b, self.c, self.d])

  def create_server(self, si: ServerInfo, connection_stub: ConnectionStub) -> Server:
    return CraqServer(info=si, connection_stub=connection_stub,
                      next=self.next[si], prev=self.prev[si], tail=self.d)
