import logging

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

	def __init__(self):
		self.log = logging.getLogger("elsa.parser")

	def parse(self, query_string):
		parsed = self.expr.parseString(query_string).asDict()
		self.log.debug("raw parsed as %r" % parsed)

		if parsed.has_key("groupby"):
			# Find the offset of the first transform
			end_of_query = query_string.index("| groupby")
			query_string = query_string[:end_of_query]
			aggs = []
			for item in parsed["groupby"][1:]:
				self.log.debug("item: %r" % item)
				aggs.append(item)

			aggs = {
				",".join(aggs): {
					"terms": {
						"script": " + ".join([ "doc['%s'].value + '-'" % x for x in aggs ])[:-6]
					}
				}
			}
			
			
			return {
				"query": {
					"query_string": {
						"query": query_string
					}
				},
				"aggs": aggs
			}, parsed
		return {
				"query": {
					"query_string": {
						"query": query_string
					}
				}
			}, parsed

