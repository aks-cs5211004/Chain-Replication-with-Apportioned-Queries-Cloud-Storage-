import json
from enum import Enum
from typing import Optional, Final
from threading import Lock
from collections import defaultdict

from core.logger import server_logger
from core.message import JsonMessage, JsonMessage
from core.network import ConnectionStub
from core.server import Server, ServerInfo
import threading
        

class RequestType(Enum):
    SET = 1
    GET = 2
    QUERY = 3


class KVGetRequest:
  def __init__(self, msg: JsonMessage):
    self._json_message = msg
    assert "key" in self._json_message, self._json_message

  @property
  def key(self) -> str:
    return self._json_message["key"]

  @property
  def json_msg(self) -> JsonMessage:
    return self._json_message


class KVQueryRequest:
  def __init__(self, msg: JsonMessage):
    self._json_message = msg
    assert "key" in self._json_message, self._json_message

  @property
  def key(self) -> str:
    return self._json_message["key"]

  @property
  def json_msg(self) -> JsonMessage:
    return self._json_message


class KVSetRequest:
  def __init__(self, msg: JsonMessage):
    self._json_message = msg
    assert "key" in self._json_message, self._json_message
    assert "val" in self._json_message, self._json_message

  @property
  def key(self) -> str:
    return self._json_message["key"]

  @property
  def val(self) -> str:
    return self._json_message["val"]

  @property
  def version(self) -> Optional[int]:
    return self._json_message.get("ver")

  @version.setter
  def version(self, ver: int) -> None:
    self._json_message['ver'] = ver

  @property
  def json_msg(self) -> JsonMessage:
    return self._json_message

  def __str__(self) -> str:
    return str(self._json_message)


from threading import Lock
from collections import defaultdict

class CraqServer(Server):
    """CRAQ server with key-level locking. Supports concurrent read while writes are in progress."""
    
    def __init__(self, info: ServerInfo, connection_stub: ConnectionStub,
                 next: Optional[ServerInfo], prev: Optional[ServerInfo],
                 tail: ServerInfo) -> None:
        super().__init__(info, connection_stub)
        self.next: Final[Optional[str]] = None if next is None else next.name
        self.prev: Final[Optional[str]] = None if prev is None else prev.name
        self.tail: Final[str] = tail.name
        self.temp_store: dict[str, dict[int, str]] = {}
        self.store: dict[str, tuple[int, str]] = {} 
        self.locks: defaultdict[str, Lock] = defaultdict(Lock)

    def _process_req(self, msg: JsonMessage) -> JsonMessage:
        if msg.get("type") == RequestType.GET.name:
            return self._get(KVGetRequest(msg))
        elif msg.get("type") == RequestType.SET.name:
            return self._set(KVSetRequest(msg))
        elif msg.get("type") == RequestType.QUERY.name:
            return self._query(KVQueryRequest(msg))
        else:
            server_logger.critical("Invalid message type")
            return JsonMessage({"status": "Unexpected type"})

    def _get(self, req: KVGetRequest) -> JsonMessage:
        _logger = server_logger.bind(server_name=self._info.name)
        _logger.debug(f"GET request for key: {req.key}")
        with self.locks[req.key]:
            if req.key in self.temp_store and self.temp_store[req.key]:
                query_msg = JsonMessage({"type": RequestType.QUERY.name, "key": req.key})
                response = self._connection_stub.send(from_=self._info.name, to=self.next, message=query_msg)
                tail_committed_version = response.get("ver")
                temp_versions_dict = self.temp_store[req.key]
                if tail_committed_version in temp_versions_dict:
                   return JsonMessage({"status": "OK", 
                                       "val": temp_versions_dict[tail_committed_version]})
                elif req.key in self.store:
                    _, val = self.store[req.key]
                    return JsonMessage({"status": "OK", "val": val})
            elif req.key in self.store:
                _, val = self.store[req.key]
                return JsonMessage({"status": "OK", "val": val})
        return JsonMessage({"status": "Key not found"})

    def _query(self, req: KVQueryRequest) -> JsonMessage:
        _logger = server_logger.bind(server_name=self._info.name)
        _logger.debug(f"QUERY request for key: {req.key}")
        with self.locks[req.key]:
            if self.next is None:
                ver, _ = self.store[req.key]
                return JsonMessage({"ver": ver})
            else:
                return self._connection_stub.send(from_=self._info.name, to=self.next, message=req.json_msg)

    def _set(self, req: KVSetRequest) -> JsonMessage:
        _logger = server_logger.bind(server_name=self._info.name)
        _logger.debug(f"SET request for key: {req.key} with value: {req.val}")
        with self.locks[req.key]:
            if self.prev is None:
                if req.key in self.temp_store and self.temp_store[req.key]:
                    current_version = max(self.temp_store[req.key].keys())
                elif req.key in self.store:
                    current_version, _ = self.store[req.key]
                else:
                    current_version = 0
                req.version = current_version + 1

            if self.next is not None:
                if req.key not in self.temp_store:
                    self.temp_store[req.key] = {}
                self.temp_store[req.key][req.version] = req.val
                self._connection_stub.send(from_=self._info.name, to=self.next, message=req.json_msg)
                self.store[req.key] = (req.version, req.val)
                del self.temp_store[req.key][req.version]
                return JsonMessage({"status": "OK"})
            else:
                self.store[req.key] = (req.version, req.val)
                return JsonMessage({"status": "OK"})
