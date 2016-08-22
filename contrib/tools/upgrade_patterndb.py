import sys
import os
import re

"""
Utility to convert a patterndb from the old ELSA field names (i0, s0, etc.) to the
new, normal names in which the field name and class name is directly used.

It expects an old patterndb on stdin and prints a new one to stdout. Use the SCHEMA
environment variable to use a non-default schema.sql.
"""

class Upgrader:
	def __init__(self):
		self.current_class = None
		self.classes = {}
		self.class_id_to_name = {}
		self.field_order_to_name = {
			5: 'i0',
			6: 'i1',
			7: 'i2',
			8: 'i3',
			9: 'i4',
			10: 'i5',
			11: 's0',
			12: 's1',
			13: 's2',
			14: 's3',
			15: 's4',
			16: 's5',
		}
		self.in_comment = False

	def load_schema(self, filename):
		with open(filename) as fh:
			for line in fh:
				if 'INSERT INTO classes' in line:
					matches = re.match(r'INSERT INTO classes \(id, class[^\)]*\) VALUES\((\d+)\,\s+\"([^\"]+)', line)
					if not matches:
						raise Exception("INSERT INTO classes without class id: %s" % line)
					self.class_id_to_name[ int(matches.group(1)) ] = matches.group(2)
				elif "INSERT INTO fields_classes_map" in line and "VALUES ((" in line:
					matches = re.search(r'INSERT INTO fields_classes_map ' +\
						'\(class_id\, field_id\, field_order\) ' +\
						'VALUES \(\(SELECT id FROM classes WHERE class=\"([^\"]+)\"\)\, ' +\
						'\(SELECT id FROM fields WHERE field=\"([^\"]+)\"\)\, (\d+)\)\;', line)
					#print matches.group(1), matches.group(2), matches.group(3)
					if not self.classes.has_key( matches.group(1) ):
						self.classes[ matches.group(1) ] = {}
					#print matches.group(1), self.field_order_to_name[ int(matches.group(3)) ], matches.group(2)
					self.classes[ matches.group(1) ][ self.field_order_to_name[ int(matches.group(3)) ] ] = matches.group(2)


	def get_field_name(self, class_name, field):
		#print class_name, field, self.classes[class_name]
		return self.classes[class_name][field]

	def set_class(self, line):
		if '<rule ' in line:
			return re.sub(r"class=[\"\']?(\d+)[\"\']?", self.class_repl, line)
			#if not matches:
			#	raise Exception("Found <rule but not class: %s" % line)
		return line

	def class_repl(self, matchobj):
		#print self.class_id_to_name
		#print "class_id", int(matchobj.group(1))
		self.current_class = self.class_id_to_name[ int(matchobj.group(1)) ]
		return 'class="' + self.class_id_to_name[ int(matchobj.group(1)) ] + '"'
			
	def repl(self, matchobj):
		#print "current %r" % self.current_class
		return ":" + self.get_field_name(self.current_class, matchobj.group(1)) + ":"

	def test_repl(self, matchobj):
		#print "current %r" % self.current_class
		return "\"" + self.get_field_name(self.current_class, matchobj.group(1)) + "\""

	def should_ignore(self, line):
		if re.match("\s*<!--", line):
			self.in_comment = True	
		if re.search("-->\s*$", line):
			self.in_comment = False
			return True
		return self.in_comment

if __name__ == "__main__":
	upgrader = Upgrader()
	if os.environ.has_key("SCHEMA"):
		upgrader.load_schema(os.environ["SCHEMA"])
	else:
		upgrader.load_schema("../../node/conf/schema.sql")
	
	for i, line in enumerate(sys.stdin):
		line = line.rstrip("\n")
		#print i
		if upgrader.should_ignore(line):
			print line
			continue
		line = upgrader.set_class(line)
		if "<pattern>" in line:
			print re.sub(r'\:([is][0-5])\:', upgrader.repl, line)
		elif "<test_value name=" in line:
			print re.sub(r'[\'\"]([is][0-5])[\'\"]', upgrader.test_repl, line)
		else:
			print line