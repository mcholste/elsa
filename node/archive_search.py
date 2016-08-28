import sys
import sqlite3
import os
import logging
import gzip
from functools import partial
from multiprocessing.pool import ThreadPool

import ujson as json
import tornado.ioloop
import tornado.web

DEFAULT_LISTEN_PORT = 8088

def search(search, filename):
	log = logger.getLogger("elsa.archive_search.worker")
	results = []
	try:
		for line in gzip.GzipFile(filename):
			try:
				line = json.loads(line)
			except Exception as e:
				log.exception("JSON error: %r" % e, exc_info=e)
				continue
			if type(search) == dict:
				for term in search["query"]["match"]["terms"]:
					if term in line["@message"]:
						results.append(line)
						continue
			else:
				if search in line["@message"]:
					results.append(line)
	except Exception as e:
		log.exception("Other error: %r" % e, exc_info=e)
		pass
	return (filename, results)

class SearchHandler(tornado.web.RequestHandler):
	def initialize(self, directory_file, pool):
		self.directory_file = directory_file
		self.pool = pool
		self.log = logging.getLogger("elsa.archive_search.searchhandler")
		self.outstanding = set()
		self.results = []

	def __init__(self, application, request, **kwargs):
		super(SearchHandler, self).__init__(application, request, **kwargs)

	def callback(self, chunked_results):
		for tup in chunked_results:
			(filename, result) = tup
			self.log.debug("File %s finished with %d results" % (filename, len(result)))
			self.outstanding.remove(filename)
			self.log.debug("outstanding: %r" % self.outstanding)
			self.results.extend(result)
			if not self.outstanding:
				self._finish()

	def _finish(self):
		self.set_header("Content-Type", "application/json")
		self.write(json.dumps(self.results))
		self.finish()

	@tornado.web.asynchronous
	def get(self):
		# Find files that need to be searched
		to_search = []
		with sqlite3.Connection(self.directory_file) as con:
			cur = con.cursor()
			cur.execute("SELECT * FROM directory WHERE start>=? AND end<=?",
				(self.get_argument("start"), self.get_argument("end")))
			
			for row in cur.fetchall():
				self.log.debug("Searching: %r" % list(row))
				to_search.append(row[1])
		self.outstanding = set(to_search)
		self.pool.map_async(partial(search, self.get_argument("search")), to_search, 
			callback=self.callback)
				

class App:
	def __init__(self, loop, directory_file, port=DEFAULT_LISTEN_PORT):
		self.log = logging.getLogger("elsa.archive_search.app")
		self.port = port
		self.loop = loop
		self.pool = ThreadPool()
		self.application = tornado.web.Application([
			("/_search", SearchHandler, dict(directory_file=directory_file, pool=self.pool))	
		])
		
	def start(self):
		self.application.listen(self.port)
		self.loop.start()


if __name__ == "__main__":
	logging.basicConfig()
	log = logging.getLogger()
	log.setLevel(logging.DEBUG)
	app = App(tornado.ioloop.IOLoop.instance(), sys.argv[1])
	app.start()