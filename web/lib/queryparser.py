import logging
import datetime
from time import time, mktime

from pyparsing import *

class Parser:

	LPAR, RPAR, COLON, GT, LT, EQ, NE, PIPE = map(Suppress, "():><=!|")
	GLOBAL_TERMS = []

	word = Word(alphanums + "_" + "@" + "." + "-")
	term = Group(Or(word + QuotedString('"'))).setResultsName("terms", listAllMatches=True)

	operator = oneOf([":", "=", ">", "<", ">=", "<=", "!="])

	viz_names = ["geomap", "sankey"]
	viz_parsers = []
	for viz_name in viz_names:
		viz_parsers.append(Group(PIPE + \
			Literal(viz_name)).setResultsName("viz", listAllMatches=True))

	transform_offset = Group(PIPE + Literal("offset") + word).setResultsName("offset")
	transform_limit = Group(PIPE + Literal("limit") + word).setResultsName("limit")
	transform_sort = Group(PIPE + Literal("sort") + delimitedList(word)).setResultsName("sort")
	#transform_geomap = Group(PIPE + Literal("geomap")).setResultsName("viz", listAllMatches=True)
	#transform_sankey = Group(PIPE + Literal("sankey")).setResultsName("viz", listAllMatches=True)
	transform_groupby = Group(PIPE + Literal("groupby") + delimitedList(word)).setResultsName("groupby")
	transform_arr = [transform_limit, transform_offset, transform_sort, transform_groupby]
	transform_arr.extend(viz_parsers)
	transforms = Or(transform_arr)

	expr = Forward()

	field_term = Group(word + 
		Group(operator + (word ^ delimitedList(word) ^ nestedExpr('(', ')', content=expr)))
	).setResultsName("fieldterms", listAllMatches=True)
	
	clause = OneOrMore(Or([term, field_term])) + Optional(OneOrMore(transforms))

	expr << (OneOrMore(clause) ^ nestedExpr('(', ')', content=expr))

	DEFAULT_TIME_INTERVAL = 86400

	def __init__(self):
		self.log = logging.getLogger("elsa.parser")

	def parse(self, query_string, request):
		start_time = request.get_argument("start", None)
		if not start_time:
			start_time = time() - self.DEFAULT_TIME_INTERVAL
		else:
			#start_time = mktime(datetime.datetime.strptime(start_time, "%m/%d/%Y").timetuple())
			start_time = int(start_time)
		end_time = request.get_argument("end", None)
		if not end_time:
			end_time = time()
		else:
			#end_time = mktime(datetime.datetime.strptime(end_time, "%m/%d/%Y").timetuple())
			end_time = int(end_time)
		time_span = end_time - start_time
		start_time *= 1000
		end_time *= 1000
		max_time_buckets = 100
		interval = "second"
		if time_span / (86400 * 30) >= max_time_buckets:
			interval = "month"
		elif time_span / (86400) >= max_time_buckets:
			interval = "day"
		elif time_span / (3600) >= max_time_buckets:
			interval = "hour"
		elif time_span / (60) >= max_time_buckets:
			interval = "minute"

		#query_string += " AND (@timestamp>=%d AND @timestamp<=%d)" % (start_time, end_time)

		parsed = self.expr.parseString(query_string).asDict()
		self.log.debug("raw parsed as %r" % parsed)

		

		time_filter = {
			"range": { "@timestamp": {
					"gte": start_time,
					"lte": end_time
				}
			}
		}

		filters = [time_filter]

		max_buckets = 100

		aggs = {
			"date_histogram": {
				"date_histogram": {
					"field": "@timestamp",
					"interval": interval
				},
				"aggs": {
					"host": {
						"terms": {
							"field": "raw.host",
							"size": max_buckets
						}
					},
					"class": {
						"terms": {
							"field": "raw.class",
							"size": max_buckets
						}
					}
				}
			}
		}

		if parsed.has_key("groupby"):
			# Find the offset of the first transform
			end_of_query = query_string.index("| groupby")
			query_string = query_string[:end_of_query]
			multi_groups = []
			if len(parsed["groupby"]) > 2:
				for item in parsed["groupby"][1:]:
					self.log.debug("item: %r" % item)
					multi_groups.append(item)

				multi_groupby_aggs = {
					",".join(multi_groups): {
						"terms": {
							"script": " + ".join([ "doc['%s'].value + '\t'" % x for x in multi_groups ])[:-6],
							"size": int(max_buckets/4)
						}
					}
				}
				aggs.update(multi_groupby_aggs)
			else: 
				# single groupby
				field = parsed["groupby"][1]
				groupby_aggs = {
					field: { "terms": { "field": field, "size": max_buckets }}
				}
				aggs.update(groupby_aggs)
					
		# Default is to groupby time histogram
		return {
				"query": {
					"bool": {
						"must": [
							{
								"query": {
									"query_string": {
										"query": query_string
									}
								}
							}
						],
						"filter": filters
					}
				},
				"aggs": aggs
			}, parsed
		# return {
		# 		"query": {
		# 			"query_string": {
		# 				"query": query_string
		# 			}
		# 		},
		# 		"aggs": {
		# 			"date_histogram": {
		# 				"date_histogram": {
		# 					"field": "@timestamp",
		# 					"interval": "second"
		# 				}
		# 			}
		# 		}
		# 	}, parsed

