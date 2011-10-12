#!/usr/bin/env python
#
# Copyright (c) 2011  Somia Dynamoid Oy
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#

import BaseHTTPServer as basehttpserver
import Queue as queuelib
import argparse
import asyncore
import contextlib
import os
import signal
import socket
import stat
import struct
import syslog
import threading
import time

import MySQLdb as mysql
import signalfd.async

class Stat(object):

	def __init__(self):
		self.value = 0
		self.lock = threading.Lock()

	def increment(self):
		with self.lock:
			self.value += 1

	def get(self):
		with self.lock:
			return self.value

class Stats(object):

	def __init__(self):
		self.__stats = { name: Stat() for name in ["input", "output", "error"] }

	def __getattr__(self, name):
		return self.__stats[name]

	def __iter__(self):
		return self.__stats.iteritems()

class Item(object):

	def __init__(self, database=None, query=None, terminate=False):
		self.database = database
		self.query = query
		self.terminate = terminate

class Listener(asyncore.dispatcher):

	def __init__(self, queue, address, stats):
		asyncore.dispatcher.__init__(self)

		self.__queue = queue
		self.__address = address
		self.__stats = stats

		self.__remove_socket_file()

		self.create_socket(socket.AF_UNIX, socket.SOCK_STREAM)
		self.bind(self.__address)
		self.listen(socket.SOMAXCONN)

	def __remove_socket_file(self):
		if os.path.exists(self.__address) and stat.S_ISSOCK(os.stat(self.__address).st_mode):
			os.remove(self.__address)

	def writable(self):
		return False

	def handle_accept(self):
		self.__stats.input.increment()

		sock, _ = self.accept()
		Receiver(sock, self, self.__stats)

	def handle_close(self):
		self.close()

	def enqueue_query(self, database, query):
		self.__queue.put(Item(database, query))

	def enqueue_terminate(self):
		self.close()
		self.__queue.put(Item(terminate=True))
		self.__remove_socket_file()

class Receiver(asyncore.dispatcher_with_send):

	__version   = 1
	__headsize  = 8

	def __init__(self, sock, listener, stats):
		asyncore.dispatcher_with_send.__init__(self, sock)

		self.__listener = listener
		self.__stats = stats
		self.__head = bytearray()
		self.__data = bytearray()

	def __recv(self, data, size):
		data.extend(self.recv(size - len(data)))
		return len(data) == size

	def readable(self):
		return self.connected and not self.out_buffer

	def handle_read(self):
		if self.__data is None or not self.__listener.accepting:
			self.close()
			return

		if len(self.__head) < self.__headsize:
			self.__recv(self.__head, self.__headsize)
		else:
			version, size = struct.unpack("<II", str(self.__head))
			assert version == self.__version

			if self.__recv(self.__data, size):
				database_size, = struct.unpack("<I", str(self.__data[:4]))
				strings = str(self.__data[4:])
				database = strings[:database_size]
				query = strings[database_size:]

				self.__head = None
				self.__data = None

				self.__listener.enqueue_query(database, query)

				result = 1

				self.send(struct.pack("<II", self.__version, result))

	def handle_write(self):
		if not self.__listener.accepting:
			self.close()
			return

		asyncore.dispatcher_with_send.handle_write(self)

		if not self.out_buffer:
			self.close()

	def handle_close(self):
		self.close()

class Signaler(signalfd.async.dispatcher):

	def __init__(self, listener):
		signalfd.async.dispatcher.__init__(self)

		self.__listener = listener
		self.__count = 0

	def handle_signal(self, signum):
		if signum in (signal.SIGTERM, signal.SIGINT):
			if self.__count == 0:
				syslog.syslog(syslog.LOG_INFO, "Terminating")
				self.__listener.enqueue_terminate()

			self.__count += 1

	@property
	def user_insists(self):
		return self.__count > 1

class Database(object):

	def __init__(self, params, queue, stats, signaler):
		self.params = params
		self.queue = queue
		self.stats = stats
		self.signaler = signaler

		self.connect()

	@property
	def connected(self):
		return self.conn is not None

	def connect(self):
		conn = mysql.connect(**self.params)
		conn.autocommit(True)

		self.conn = conn

	def disconnect(self):
		if self.conn:
			try:
				self.conn.close()
			except:
				pass

		self.conn = None

	def execute(self):
		while True:
			item = self.queue.get()

			if item.terminate:
				syslog.syslog(syslog.LOG_INFO, "Terminated")
				self.queue.task_done()
				break

			if self.execute_item(item):
				self.stats.output.increment()
			else:
				self.stats.error.increment()
				syslog.syslog(syslog.LOG_ERR, "Could not execute query: %s" % item.query)

			self.queue.task_done()

		self.disconnect()

	def execute_item(self, item):
		assert item.database == self.params["db"]

		for i in xrange(2):
			while not self.connected:
				try:
					self.connect()
					break

				except Exception as e:
					syslog.syslog(syslog.LOG_ERR, str(e))

					if self.signaler.user_insists:
						syslog.syslog(syslog.LOG_INFO, "Giving up due to persistent user")
						self.drain_queue(item)
						raise

				time.sleep(1)

			try:
				with contextlib.closing(self.conn.cursor()) as cursor:
					cursor.execute(item.query)
					return True

			except Exception as e:
				syslog.syslog(syslog.LOG_ERR, str(e))

			self.disconnect()

		return False

	def drain_queue(self, item):
		while not item.terminate:
			syslog.syslog(syslog.LOG_ERR, "Could not execute query: %s" % item.query)
			item = self.queue.get()

class StatusServer(basehttpserver.HTTPServer):

	def __init__(self, stats, *args):
		basehttpserver.HTTPServer.__init__(self, *args)

		self.stats = stats

class StatusRequestHandler(basehttpserver.BaseHTTPRequestHandler):

	def do_GET(self):
		if self.path == "/":
			self.send_response(200)
			self.end_headers()

			for name, stat in sorted(self.server.stats):
				print >>self.wfile, "%s: %d" % (name, stat.get())
		else:
			try:
				stat = getattr(self.server.stats, self.path[1:])
			except KeyError:
				self.send_error(404)
				self.end_headers()
			else:
				self.send_response(200)
				self.end_headers()

				print >>self.wfile, stat.get()

	def log_message(self, *args):
		pass

def main():
	parser = argparse.ArgumentParser()
	parser.add_argument(metavar="PARAMS", dest="params", help="MySQLdb connection parameters")
	parser.add_argument("--bufsize", metavar="NUM", dest="maxsize", type=int, default=20000, help="maximum buffer length")
	parser.add_argument("--socket", metavar="PATH", dest="address", default="/tmp/insertbuffer.socket", help="listening socket path")
	parser.add_argument("--status", metavar="ADDR", dest="status", help="status HTTP server address ([HOST]:PORT)")
	args = parser.parse_args()

	params = {
		name: value
		for name, value
		in (
			pair.split("=", 1)
			for pair
			in args.params.split()
		)
	}

	assert "db" in params

	syslog.openlog("insertbufferd")

	queue = queuelib.Queue(args.maxsize)
	stats = Stats()
	listener = Listener(queue, args.address, stats)
	signaler = Signaler(listener)
	database = Database(params, queue, stats, signaler)

	if args.status:
		host, port = args.status.split(":", 1)
		status_server = StatusServer(stats, (host, int(port)), StatusRequestHandler)
	else:
		status_server = None

	receiver_thread = threading.Thread(target=asyncore.loop, kwargs=dict(use_poll=True))
	receiver_thread.daemon = True
	receiver_thread.start()

	if status_server:
		status_thread = threading.Thread(target=status_server.serve_forever)
		status_thread.daemon = True
		status_thread.start()

	syslog.syslog(syslog.LOG_INFO, "Initialized")

	database.execute()

if __name__ == "__main__":
	main()
