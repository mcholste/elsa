import os
import sys
import gzip
import logging
import datetime
import sqlite3
import uuid
from time import time
from multiprocessing import Queue, Process

import ujson as json
from requests import session
from requests_futures.sessions import FuturesSession
from elasticsearch import Elasticsearch
from elasticsearch.client import ClusterClient, IndicesClient
from elasticsearch.helpers import bulk as es_bulk

class Indexer:
	def __init__(self, host, queue, port=9200):
		self.log = logging.getLogger("elsa.indexer")
		self.es = Elasticsearch([{"host": host, "port": port}])
		self.cluster_client = ClusterClient(self.es)
		health = self.cluster_client.health()
		if not health or health.get("number_of_nodes") < 1:
			raise Exception("No Elasticsearch nodes found: %r" % health)
		self.max_chunk_limit = 1000
		self.chunk_limit = self.max_chunk_limit
		self.last_event_time = time()
		self.index_prefix = "elsa-"
		self.index_name = self.get_index_name()
		# Create the index in case it isn't there, it should inherit settings
		# from the template.
		self.indices_client = IndicesClient(self.es)
		if not self.indices_client.exists(self.index_name):
			self.indices_client.create(self.index_name, wait_for_active_shards=1)
		self.queue = queue
		# This will block as it reads from the queue
		es_bulk(self.es, iter(self.queue.get, "STOP"), stats_only=True)

	def get_index_name(self):
		return "%s%s" % (self.index_prefix, datetime.date.today().isoformat())

class Archiver:
	def __init__(self, queue, conf={}):
		self.conf = conf
		self.log = logging.getLogger("elsa.archiver")
		if not conf.has_key("directory"):
			raise Exception("No directory given in conf")
		self.directory_folder = conf["directory"]
		with sqlite3.connect(self.conf.get("directory_file", "archive.db")) as con:
			cur = con.cursor()
			cur.execute("CREATE TABLE IF NOT EXISTS directory (id INTEGER UNSIGNED " +
				"PRIMARY KEY, filename VARCHAR(255), start INTEGER UNSIGNED, " +
				"end INTEGER UNSIGNED, count INTEGER UNSIGNED)")
		self.counter = 0
		self.bytes_counter = 0
		self.file_start = time()
		self.batch_limit = conf.get("batch_limit", 100)
		self.batch_size_limit = conf.get("batch_size_limit", 10 * 1024 * 1024)
		self.queue = queue
		self.current_filename = self.get_new_filename()
		self.out_fh = gzip.GzipFile(self.current_filename, mode="wb")
		for data in iter(self.queue.get, "STOP"):
			self.out_fh.write(data)
			self.counter += 1
			self.bytes_counter += len(data)
			if self.counter >= self.batch_limit or self.bytes_counter >= self.batch_size_limit:
				self.rollover()
		
	def get_new_filename(self):
		args = list(datetime.datetime.now().timetuple()[0:6])
		args.insert(0, self.directory_folder)
		folder = "%s/%04d/%02d/%02d/%02d/%02d/%02d" % tuple(args)
		os.makedirs(folder)
		filename = "%s.json.gz" % str(uuid.uuid4())
		return "%s/%s" % (folder, filename)

	def rollover(self):
		self.log.info("Rolling over archive file at size %d and count %d" %\
			(self.bytes_counter, self.counter))
		self.counter = 0
		self.bytes_counter = 0
		self.out_fh.close()
		self.add_to_directory()
		self.current_filename = self.get_new_filename()
		self.out_fh = gzip.GzipFile(self.current_filename, mode="wb")
		self.file_start = time()

	def add_to_directory(self):
		with sqlite3.connect(self.conf.get("directory_file", "archive.db")) as con:
			cur = con.cursor()
			cur.execute("INSERT INTO directory (filename, start, end, count) VALUES (?,?,?,?)",
				(self.current_filename, self.file_start, time(), self.counter))

class Distributor:
	def __init__(self, conf):
		self.log = logging.getLogger("elsa.distributor")
		self.conf = conf
		def spawn_indexer(host, queue):
			es = Indexer(host, queue)

		def spawn_archiver(conf, queue):
			archiver = Archiver(queue, conf)

		self.destinations = []

		self.destinations.append({ "queue": Queue() })
		self.destinations[-1]["proc"] = Process(target=spawn_archiver, 
			args=(conf, self.destinations[-1]["queue"]))

		self.destinations.append({ "queue": Queue() })
		self.destinations[-1]["proc"] = Process(target=spawn_indexer, 
			args=(conf["host"], self.destinations[-1]["queue"]))

		self.decorator = Decorator()

	def read(self, fh):
		for d in self.destinations:
			d["proc"].start()

		for line in fh:
			try:
				line = json.loads(line)
				line = self.decorator.decorate(line)
			except Exception as e:
				self.log.error("Invalid JSON: %s, Error: %r" % (line, e))
				continue
			for d in self.destinations:
				d["queue"].put(line)

		for d in self.destinations:
			d["queue"].put("STOP")
			d["proc"].join()

class Decorator:
	def __init__(self):
		pass
	
	def decorate(self, doc):
		return doc


if __name__ == "__main__":
	logging.basicConfig(
		format='%(asctime)s.%(name)s:%(levelname)s:%(process)d:%(message)s',
		level=logging.DEBUG,
		filename="/tmp/elsa.log"
	)

	distributor = Distributor(json.load(open(sys.argv[1])))
	distributor.read(sys.stdin)
