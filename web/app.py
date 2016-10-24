import os
import sys
import logging
import importlib
import sqlite3

import ujson as json
import tornado.ioloop
import tornado.web

sys.path.insert(1, sys.path[0] + "/lib")
from handlers import *

DEFAULT_LISTEN_PORT = 8080

class App:
	def __init__(self, conf, loop, port=DEFAULT_LISTEN_PORT):
		self.log = logging.getLogger("elsa.app")
		if conf.has_key("listen_port"):
			port = conf["listen_port"]
		self.port = port
		self.loop = loop
		self.conf = conf
		self._init_db()

		tornado_config = [
			(r"/search(.*)", SearchHandler, {"conf": conf, "loop": loop, "db": self.db }),
			(r"/inc/(.*)", StaticHandler, 
				dict(path=os.path.join(os.path.dirname(__file__), "inc"))),
			("/transcript", TranscriptHandler, dict(db=self.db)),
			("/tags", TagsHandler, dict(db=self.db)),
			("/favorites", FavoritesHandler, dict(db=self.db)),
			(r"/results/(.*)", SearchResultsHandler, dict(db=self.db)),
			("/", IndexHandler, 
				dict(filename=os.path.join(os.path.dirname(__file__), "inc/index.html"),
					mimetype="text/html"))
		]
		self.application = tornado.web.Application(tornado_config, debug=True)
		
	def start(self):
		self.application.listen(self.port)
		self.loop.start()

	def _init_db(self):
		self.db = sqlite3.Connection("%s/elsa.db" % self.conf.get("db_path", "/tmp"))
		# Set autocommit
		self.db.isolation_level = None
		
		def dict_factory(cursor, row):
			d = {}
			for idx, col in enumerate(cursor.description):
				d[col[0]] = row[idx]
			return d
		self.db.row_factory = dict_factory
		self.db = self.db.cursor()

		self.log.debug("Ensuring database tables exists")

		self.db.execute("""
CREATE TABLE IF NOT EXISTS users (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	user TEXT UNIQUE
)
""")
		self.db.execute("INSERT OR IGNORE INTO users (user) VALUES (?)", (DEFAULT_USER,))
		self.db.execute("""
CREATE TABLE IF NOT EXISTS transcript (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	user_id INTEGER,
	action TEXT,
	scope TEXT,
	ref_id INTEGER,
	timestamp INTEGER,
	visible INTEGER NOT NULL DEFAULT 1,
	FOREIGN KEY (user_id) REFERENCES users (id),
	FOREIGN KEY (ref_id) REFERENCES results (id)
)
""")
		self.db.execute("""
CREATE TABLE IF NOT EXISTS results (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	user_id INTEGER,
	results BLOB,
	timestamp INTEGER,
	FOREIGN KEY (user_id) REFERENCES users (id)
)
""")
		self.db.execute("""
CREATE TABLE IF NOT EXISTS tags (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	user_id INTEGER,
	tag TEXT,
	value TEXT,
	timestamp INTEGER,
	UNIQUE (user_id, tag, value) ON CONFLICT IGNORE,
	FOREIGN KEY (user_id) REFERENCES users (id)
)""")
		self.db.execute("""
CREATE TABLE IF NOT EXISTS favorites (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	user_id INTEGER,
	value TEXT,
	timestamp INTEGER,
	UNIQUE (user_id, value) ON CONFLICT IGNORE,
	FOREIGN KEY (user_id) REFERENCES users (id)
)""")

if __name__ == "__main__":
	logging.basicConfig()
	log = logging.getLogger()
	log.setLevel(logging.DEBUG)
	conf = {
		"fed": {
			"host": "localhost",
			"port": 8888
		},
		"db_path": "/tmp"
	}
	if len(sys.argv) > 1:
		conf = json.load(open(sys.argv[1]))
	app = App(conf, tornado.ioloop.IOLoop.instance())
	app.start()