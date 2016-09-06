import os
import sys
import logging
import importlib

import ujson as json
import tornado.ioloop
import tornado.web

sys.path.insert(1, sys.path[0] + "/lib")
from handlers import SearchHandler, StaticHandler, IndexHandler

DEFAULT_LISTEN_PORT = 8080

class App:
	def __init__(self, conf, loop, port=DEFAULT_LISTEN_PORT):
		self.log = logging.getLogger("elsa.app")
		if conf.has_key("listen_port"):
			port = conf["listen_port"]
		self.port = port
		self.loop = loop
		tornado_config = [
			(r"/search(.*)", SearchHandler, {"conf": conf, "loop": loop }),
			(r"/inc/(.*)", StaticHandler, 
				dict(path=os.path.join(os.path.dirname(__file__), "inc"))),
			("/", IndexHandler, 
				dict(filename=os.path.join(os.path.dirname(__file__), "inc/index.html"),
					mimetype="text/html"))
		]
		self.application = tornado.web.Application(tornado_config, debug=True)
		
	def start(self):
		self.application.listen(self.port)
		self.loop.start()


if __name__ == "__main__":
	logging.basicConfig()
	log = logging.getLogger()
	log.setLevel(logging.DEBUG)
	conf = {
		"fed": {
			"host": "localhost",
			"port": 8888
		}
	}
	if len(sys.argv) > 1:
		conf = json.load(open(sys.argv[1]))
	app = App(conf, tornado.ioloop.IOLoop.instance())
	app.start()