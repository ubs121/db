#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import codecs
from pymongo import MongoClient

BATCH_SIZE = 1000

if __name__ == '__main__':
	if len(sys.argv)<2:
		print "Usage: load_triple.py <filename>"
		sys.exit()

	f = codecs.open(sys.argv[1], encoding='utf-8')

	client = MongoClient()
	db = client.ci
	cl = db.nodes

	n = 0
	nodes = {}
	links = []

	for line in f:
		parts = line.split(' ', 2)

		# TODO: урт, олон мөр текстийг яаж оруулах вэ?
		if parts[1][0] == ':':
			# :-р эхэлсэн бол талбар
			o = {}
			if parts[0] in nodes:
				o = nodes[parts[0]]

			o[parts[1]] = parts[2]

			nodes[parts[0]] = o

			print repr(o).decode("unicode-escape")
		else:
			# холбоос
			print parts[2]

		n = n + 1
		if n >= BATCH_SIZE:
			# write to db
			nodes.clear()
			n = 0

	db.close()
	f.close()