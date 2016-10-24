import os
import sys
import logging
import datetime
import zlib
import base64
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
DEFAULT_USER = "default"

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
		self.db = kwargs["db"]
		del kwargs["db"]
		super(SearchHandler, self).__init__(application, request, **kwargs)
		self.log = logging.getLogger("elsa.search_handler")
		self.parser = Parser()
		self.ip_fields = frozenset(["srcip", "dstip", "ip"])
		

	def initialize(self, *args, **kwargs):
		super(SearchHandler, self).initialize(*args, **kwargs)
		self.user = DEFAULT_USER

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
						values = str(bucket["key"]).split("\t")
					newvalues = []
					for i, value in enumerate(values):
						if i in ipfields and "." not in value:
							newvalues.append(socket.inet_ntoa(struct.pack("!I", int(value))))
						else:
							newvalues.append(value)
					bucket["keys"] = newvalues
					bucket["key"] = "-".join(newvalues)
		
		# Build scope
		scope = self.request.es_query["query"]["bool"]["must"][0]["query"]["query_string"]["query"]
		if self.request.parsed.has_key("groupby"):
			scope += " (" + ",".join(self.request.parsed["groupby"][1:]) + ")"

		body = {
			"results": body,
			"query": self.request.parsed,
			"raw_query": self.request.raw_query,
			"es_query": self.request.es_query,
			"scope": scope
		}

		# Log to results
		self.db.execute("INSERT INTO results (user_id, results, timestamp) " +\
			"VALUES ((SELECT id FROM users WHERE user=?),?,?)", 
			(DEFAULT_USER, base64.encodestring(zlib.compress(json.dumps(body))), time()))
		id = self.db.execute("SELECT id FROM results " +\
			"WHERE user_id=(SELECT id FROM users WHERE user=?) " +\
			"ORDER BY id DESC LIMIT 1", (self.user,)).fetchone()
		body["id"] = id["id"]

		self.db.execute("INSERT INTO transcript (user_id, action, scope, ref_id, timestamp) " +\
			"VALUES ((SELECT id FROM users WHERE user=?),?,?,?,?)",
			(self.user, "SEARCH", scope, id["id"], time()))
		newid = self.db.execute("SELECT id FROM transcript " +\
			"ORDER BY timestamp DESC LIMIT 1").fetchone()
		body["transcript_id"] = newid["id"]

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

class TranscriptHandler(BaseWebHandler):
	def __init__(self, application, request, **kwargs):
		super(TranscriptHandler, self).__init__(application, request, **kwargs)
		self.log = logging.getLogger("elsa.transcript_handler")
		self.db = kwargs["db"]


	def initialize(self, *args, **kwargs):
		super(TranscriptHandler, self).initialize(*args, **kwargs)

	def get(self):
		user = DEFAULT_USER
		limit = self.get_argument("limit", 50)
		self.set_status(200)
		self.set_header("Content-Type", "application/javascript")
		self.write(json.dumps(self.db.execute("SELECT * FROM transcript " +\
			"WHERE user_id=(SELECT id FROM users WHERE user=?) AND visible=1 " +\
			"ORDER BY timestamp DESC LIMIT ?", (user, limit)).fetchall()))

	def put(self):
		user = DEFAULT_USER
		action = self.get_argument("action")
		scope = self.get_argument("scope") 
		ref_id = self.get_argument("ref_id", None)
		self.log.debug("user: %s, action: %s, scope: %s, ref_id: %s" % (user, action, scope, ref_id))
		if ref_id:
			self.db.execute("INSERT INTO transcript (user_id, action, scope, ref_id, timestamp) " +\
				"VALUES ((SELECT id FROM users WHERE user=?),?,?,?,?)",
				(user, action, scope, ref_id, time()))
		else:
			self.db.execute("INSERT INTO transcript (user_id, action, scope, timestamp) " +\
			"VALUES ((SELECT id FROM users WHERE user=?),?,?,?)",
			(user, action, scope, time()))
		newid = self.db.execute("SELECT * FROM transcript " +\
			"ORDER BY timestamp DESC LIMIT 1").fetchone()
		if action == "TAG":
			tag = self.get_argument("tag")
			value = self.get_argument("value")
			if not self.db.execute("INSERT INTO tags (user_id, tag, value, timestamp) " +\
				"VALUES (?,?,?,?)",
				(newid["user_id"], tag, value, time())).rowcount:
				self.set_status(400)
				self.write("Error tagging value")
				return
			self.log.debug("New tag %d %s=%s" % (newid["user_id"], tag, value))
		elif action == "FAVORITE":
			value = scope
			if not self.db.execute("INSERT INTO favorites (user_id, value, timestamp) " +\
				"VALUES (?,?,?)",
				(newid["user_id"], value, time())).rowcount:
				self.set_status(400)
				self.write("Error setting favorite value")
				return
			self.log.debug("New favorite %d %s" % (newid["user_id"], value))
		self.set_status(200)
		self.set_header("Content-Type", "application/javascript")
		self.write(newid)

	def post(self):
		user = DEFAULT_USER
		action = self.get_argument("action")
		id = self.get_argument("id")
		self.log.debug("user: %s, action: %s, id: %s" % (user, action, id))
		if action == "HIDE":
			changed = self.db.execute("UPDATE transcript SET visible=0 " +\
				"WHERE user_id=(SELECT id FROM users WHERE user=?) " +\
				"AND id=?", (user, id)).rowcount
			if not changed:
				self.set_status(400)
				self.write("Bad request, unknown user or id")
				return	
		else:
			self.set_status(400)
			self.write("Bad request, unknown action")
			return

		self.set_status(200)
		self.set_header("Content-Type", "application/javascript")
		self.write({"action": action, "id": id, "status": "ok"})

class SearchResultsHandler(BaseWebHandler):
	def __init__(self, application, request, **kwargs):
		super(SearchResultsHandler, self).__init__(application, request, **kwargs)
		self.log = logging.getLogger("elsa.search_result_handler")
		self.db = kwargs["db"]


	def initialize(self, *args, **kwargs):
		super(SearchResultsHandler, self).initialize(*args, **kwargs)

	def get(self, id):
		user = DEFAULT_USER
		try:
			id = int(id)
		except Exception as e:
			self.log.exception("Failed to parse id", exc_info=e)
			self.set_status(400)
			self.write("Invalid id")
			self.finish()
			return
		result = self.db.execute("SELECT * FROM results " +\
			"WHERE user_id=(SELECT id FROM users WHERE user=?) AND id=?", 
			(user, id)).fetchone()
		if not result:
			self.set_status(404)
			self.finish()
			return
		# ret = {
		# 	"id": result["id"],
		# 	"timestamp": result["timestamp"],
		# 	"results": json.loads(zlib.decompress(base64.decodestring(result["results"])))
		# }
		self.set_status(200)
		self.set_header("Content-Type", "application/javascript")
		self.write(zlib.decompress(base64.decodestring(result["results"])))
		# self.write(json.dumps(ret))

class TagsHandler(BaseWebHandler):
	def __init__(self, application, request, **kwargs):
		super(TagsHandler, self).__init__(application, request, **kwargs)
		self.log = logging.getLogger("elsa.search_result_handler")
		self.db = kwargs["db"]


	def initialize(self, *args, **kwargs):
		super(TagsHandler, self).initialize(*args, **kwargs)

	def get(self):
		user = DEFAULT_USER
		limit = self.get_argument("limit", 50)
		self.set_status(200)
		self.set_header("Content-Type", "application/javascript")
		self.write(json.dumps(self.db.execute("SELECT * FROM tags " +\
			"WHERE user_id=(SELECT id FROM users WHERE user=?) " +\
			"ORDER BY timestamp DESC LIMIT ?", (user, limit)).fetchall()))

class FavoritesHandler(BaseWebHandler):
	def __init__(self, application, request, **kwargs):
		super(FavoritesHandler, self).__init__(application, request, **kwargs)
		self.log = logging.getLogger("elsa.search_result_handler")
		self.db = kwargs["db"]


	def initialize(self, *args, **kwargs):
		super(FavoritesHandler, self).initialize(*args, **kwargs)

	def get(self):
		user = DEFAULT_USER
		limit = self.get_argument("limit", 50)
		self.set_status(200)
		self.set_header("Content-Type", "application/javascript")
		self.write(json.dumps(self.db.execute("SELECT * FROM favorites " +\
			"WHERE user_id=(SELECT id FROM users WHERE user=?) " +\
			"ORDER BY timestamp DESC LIMIT ?", (user, limit)).fetchall()))
