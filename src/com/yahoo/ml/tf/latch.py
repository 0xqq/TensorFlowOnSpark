# Copyright 2017 Yahoo Inc.
# Licensed under the terms of the Apache 2.0 license.
# Please see LICENSE file in the project root for terms.
from __future__ import absolute_import
from __future__ import division
from __future__ import nested_scopes
from __future__ import print_function

import logging
import select
import socket
import threading
import time

class Latch:
  """Simple countdown latch w/o notifications"""
  lock = threading.RLock()
  count = 0

  def __init__(self, count):
    with self.lock:
      self.count = count

  def decrement(self, amount=1):
    with self.lock:
      self.count -= amount

  def get_count(self):
    return self.count

  def reset(self, count):
    with self.lock:
      self.count = count

class LatchServer:
  """Simple socket server w/ polled latch semantics"""
  count = 0
  latch = None
  done = False

  def __init__(self, count):
    assert count > 0
    self.count = count
    self.latch = Latch(self.count)

  def await(self):
    """Block until latch reaches zero"""
    while self.latch.get_count() > 0:
      time.sleep(1)
    logging.info("latch.await() completed")

  def reset(self, count):
    self.latch.reset(count)

  def start(self):
    """Start socket listener in a background thread"""
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind(('',0))
    server_sock.listen(1)

    host = socket.gethostname()
    port = server_sock.getsockname()[1]
    addr = (host,port)
    logging.info("listening for {0} events at {1}".format(self.count, addr))

    def _listen(self, sock):
      CONNECTIONS = []
      RECV_BUFFER = 256
      CONNECTIONS.append(sock)

      while not self.done:
        read_socks, write_socks, err_socks = select.select(CONNECTIONS, [], [], 60)
        for sock in read_socks:
          if sock == server_sock:
            client_sock, client_addr = sock.accept()
            CONNECTIONS.append(client_sock)
            logging.info("client connected from {0}".format(client_addr))
          else:
            try:
              buf = sock.recv(RECV_BUFFER)
              if buf == None:
                raise Exception("client socket closed")

              data = buf.decode()
              logging.debug("client sent: {0}".format(data))
              if data == "REG":
                self.latch.decrement()
                logging.debug("latch: {0}".format(self.latch.get_count()))
                sock.sendall("OK".encode())
              elif data == "QRY":
                logging.debug("latch: {0}".format(self.latch.get_count()))
                sock.sendall(str(self.latch.get_count()).encode())
              else:
                sock.sendall("???".encode())
            except:
              sock.close()
              CONNECTIONS.remove(sock)
      server_sock.close()

    t = threading.Thread(target=_listen, args=(self, server_sock))
    t.daemon = True
    t.start()

    return addr

  def stop(self):
    """Stop the socket listener"""
    self.done = True

class LatchClient:
  """Simple socket client w/ polled latch semantics"""
  sock = None

  def __init__(self, server_addr):
    self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    self.sock.connect(server_addr)
    logging.info("connected to server at {0}".format(server_addr))

  def _send(self, cmd):
    self.sock.sendall(cmd)
    buf = self.sock.recv(256)
    data = buf.decode() if buf else None
    logging.debug("recv: {0}".format(data))
    return data

  def await(self):
    """Register self and poll until latch is ready"""
    self._send("REG".encode())
    done = False
    while not done:
      count = int(self._send("QRY".encode()))
      done = count <= 0
      time.sleep(1)
    self.sock.close()
