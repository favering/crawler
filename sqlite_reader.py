#! /usr/bin/env python3

from crawler import SqliteWrapper
import argparse
import os.path
import os

parser = argparse.ArgumentParser(description="Extract image from sqlite db writen by crawler.py to a directory.")
parser.add_argument('-d', '--dbfile')
parser.add_argument('-t', '--to_directory', default="img")
args = parser.parse_args()

reader = SqliteWrapper()
reader.open(args.dbfile)
img_dir = os.path.abspath(args.to_directory)
if not os.path.isdir(img_dir):
    os.mkdir(img_dir)
count = reader.read(img_dir)
print("Total %d images read to \"%s\"" % (count, img_dir))
reader.close()
