import os
import sys
import logging
import datetime
import copy
import socket
import struct
from functools import partial
from time import time

import ujson as json
import tornado.ioloop
import tornado.web
from tornado.httpclient import AsyncHTTPClient, HTTPRequest

from queryparser import Parser

def merge(src, dst):
	if dst == None:
		return src
	if type(src) == dict and type(dst) == dict:
		for k, v in src.iteritems():
			if type(v) is dict and dst.has_key(k):
				dst[k] = merge(v, dst[k])
			elif type(v) is list and dst.has_key(k):
				if len(v) == len(dst[k]):
					for i, item in enumerate(v):
						dst[k][i] = merge(item, dst[k][i])
				else:
					raise Exception("Cannot merge arrays of different length")
			elif type(v) is int or type(v) is float and dst.has_key(k):
				dst[k] += v
			else:
				dst[k] = v
	elif type(src) == int or type(src) == float:
		dst += src
	else:
		dst = src
	return dst

TORNADO_ROUTE = "(.+)"

class BaseHandler(tornado.web.RequestHandler):
	def initialize(self, conf,
		loop=tornado.ioloop.IOLoop.instance()):
		self.io_loop = loop
		self.client = AsyncHTTPClient(self.io_loop)
		self.passthrough_node = "%s:%d" % (conf["fed"]["host"], conf["fed"]["port"])

	def __init__(self, application, request, **kwargs):
		super(BaseHandler, self).__init__(application, request, **kwargs)
	
	def _bad_request(self, error):
		self.set_status(400)
		self.write(json.dumps({"error": error}))
		self.finish()

	def passthrough(self, **kwargs):
		self.request.host = self.passthrough_node
		self.request.uri  = "/" + "/".join(self.request.uri.split("/")[2:])
		uri = self.request.full_url()
		req = HTTPRequest(uri,
			method=self.request.method, 
			body=self.request.body,
			headers=self.request.headers,
			follow_redirects=False,
			allow_nonstandard_methods=True
		)
		
		self.log.debug("Passing req through %r" % req.url)
		self.client.fetch(req, self.passthrough_callback, raise_error=False)

	def passthrough_callback(self, response):
		if (response.error and not
			isinstance(response.error, tornado.httpclient.HTTPError)):
			self.set_status(500)
			self.write('Internal server error:\n' + str(response.error))
		else:
			self.set_status(response.code, response.reason)
			self._headers = tornado.httputil.HTTPHeaders() # clear tornado default header

			for header, v in response.headers.get_all():
				if header not in ('Content-Length', 'Transfer-Encoding', 'Content-Encoding', 'Connection'):
					self.add_header(header, v) # some header appear multiple times, eg 'Set-Cookie'

			if response.body:                   
				self.set_header('Content-Length', len(response.body))
				self.write(response.body)
		self.finish()

	@tornado.web.asynchronous
	def put(self, uri):
		self.post(uri)

	@tornado.web.asynchronous
	def head(self, uri):
		self.post(uri)

	@tornado.web.asynchronous
	def post(self, uri):
		# Unless we explicitly want to intercept and federate, pass the req through
		#  to the first node listed in local_nodes conf
		
		self.passthrough()
			
	@tornado.web.asynchronous
	def get(self, uri):
		self.post(uri)		
	
	def _finish(self):
		self.set_header("Content-Type", "application/json")
		self.write(json.dumps(self.results))
		self.finish()

class SearchHandler(BaseHandler):
	def __init__(self, application, request, **kwargs):
		super(SearchHandler, self).__init__(application, request, **kwargs)
		self.log = logging.getLogger("elsa.search_handler")
		self.parser = Parser()
		self.ip_fields = frozenset(["srcip", "dstip", "ip"])

	def initialize(self, *args, **kwargs):
		super(SearchHandler, self).initialize(*args, **kwargs)

	# Using the post() coroutine
	def get(self, uri):
		query_string = self.get_argument("q")
		es_query, parsed = self.parser.parse(query_string, self)
		self.log.debug("es_query: %r" % es_query)
		self.request.parsed = parsed
		self.request.es_query = es_query
		self.request.raw_query = query_string
		self.request.body = json.dumps(es_query)
		return self.post(uri)

	def fixup(self, body):
		body = json.loads(body)
		self.log.debug("body: %r" % body)
		self.log.debug("parsed: %r" % self.request.parsed)
		if body.has_key("hits"):
			for hit in body["hits"]["hits"]:
				hit["_source"]["@timestamp"] = datetime.datetime.fromtimestamp(int(hit["_source"]["@timestamp"])/1000).isoformat()
		if body.has_key("aggregations"):
			for rawfield, buckethash in body["aggregations"].iteritems():
				fields = rawfield.split(",")
				ipfields = []
				for i, field in enumerate(fields):
					if field in self.ip_fields:
						ipfields.append(i)
				self.log.debug("rawfield: %s, ipfields: %r" % (rawfield, ipfields))
				
				for bucket in buckethash["buckets"]:
					if bucket.has_key("key_as_string"):
						values = [ bucket["key_as_string"] ]
					else:
						values = bucket["key"].split("\t")
					newvalues = []
					for i, value in enumerate(values):
						if i in ipfields:
							newvalues.append(socket.inet_ntoa(struct.pack("!I", int(value))))
						else:
							newvalues.append(value)
					bucket["keys"] = newvalues
					bucket["key"] = "-".join(newvalues)
		body = {
			"results": body,
			"query": self.request.parsed,
			"raw_query": self.request.raw_query,
			"es_query": self.request.es_query
		}

		return json.dumps(body)


	@tornado.web.gen.coroutine
	def post(self, uri):
		# Unless we explicitly want to intercept and federate, pass the req through
		#  to the first node listed in local_nodes conf
		
		# query = self.request.get_argument("q", default=None)
		# if not query:
		# 	return self._bad_request("No q param given for query.")


		self.request.host = self.passthrough_node
		self.request.uri  = "/es/_search"
		uri = self.request.full_url()
		req = HTTPRequest(uri,
			method=self.request.method, 
			body=self.request.body,
			headers=self.request.headers,
			follow_redirects=False,
			allow_nonstandard_methods=True
		)
		
		self.log.debug("Passing req through %r" % req.url)
		response = yield self.client.fetch(req, raise_error=False)
		self.log.debug("got response: %r" % response)
		if (response.error and not
			isinstance(response.error, tornado.httpclient.HTTPError)):
			self.set_status(500)
			self.write('Internal server error:\n' + str(response.error))
		else:
			self.set_status(response.code, response.reason)
			self._headers = tornado.httputil.HTTPHeaders() # clear tornado default header

			for header, v in response.headers.get_all():
				if header not in ('Content-Length', 'Transfer-Encoding', 'Content-Encoding', 'Connection'):
					self.add_header(header, v) # some header appear multiple times, eg 'Set-Cookie'

			if response.body:
				# Apply any last minute field translations
				fixedup_body = self.fixup(response.body)
				self.set_header('Content-Length', len(fixedup_body))
				self.write(fixedup_body)
		self.finish()

class BaseWebHandler(tornado.web.RequestHandler):
	def __init__(self, *args, **kwargs):
		super(BaseWebHandler, self).__init__(*args, **kwargs)

	def initialize(self, *args, **kwargs):
		super(BaseWebHandler, self).initialize()
		self.log = logging.getLogger("elsa.web.handler")

class IndexHandler(BaseWebHandler):
	def initialize(self, filename, mimetype="text/html"):
		super(IndexHandler, self).initialize()
		self.filename = filename
		self.mimetype = mimetype

	def get(self):
		self.set_header("Content-Type", self.mimetype)
		self.write(open(self.filename).read())

class StaticHandler(BaseWebHandler):
	def __init__(self, *args, **kwargs):
		super(StaticHandler, self).__init__(*args, **kwargs)
		self.mimemap = {
			"css": "text/css",
			"html": "text/html",
			"js": "application/javascript",
			"png": "image/png",
			"woff": "application/octet-stream"
		}

	def initialize(self, path, mimetype="application/javascript"):
		super(StaticHandler, self).initialize()
		self.content_dir = path
		self.mimetype = mimetype

	def get(self, path):
		extension = path.split(".")[-1]
		self.mimetype = self.mimemap[extension]
		self.set_header("Content-Type", self.mimetype)
		self.set_header("Cache-Control", "no-cache")
		self.write(open(self.content_dir + "/" + path).read())
