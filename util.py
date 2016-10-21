import os
import re
import sys
import gzip
import pickle
import argparse

def get_filename(dirname):
  fnames = []
  for (dirpath, dirnames, filenames) in os.walk(dirname):
    fnames.extend(filenames)
    break
  print("Filenames done. Num : %d" % len(fnames))
  return fnames

def read_line(f):
  line = f.readline()
  if line == '':
    f.close()

  return line

def name_lengths(in_file, out_f):
  f = open(in_file, 'r')
  line = read_line(f)
  while line != '':
    (mid, wid, name, sentence, doc_links_id) = line.strip().split("\t")
    name = name.strip()
    if len(name) > 100:
      out_f.write(mid + '\t' + wid + '\t' + name + '\n')
    line = read_line(f)

  f.close()

if __name__ == '__main__':
  dirname = "/save/ngupta19/wikipedia/wiki_data/mentions/"
  fnames = get_filename(dirname=dirname)
  out_f = open('/save/ngupta19/wikipedia/wiki_data/longnames', 'w')
  for fname in fnames:
    in_file = os.path.join(dirname, fname)
    name_lengths(in_file=in_file, out_f=out_f)
  out_f.close()