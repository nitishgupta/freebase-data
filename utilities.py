import os
import sys
import gzip
import pickle
import argparse

from process_entities import FreebaseData
import process_entities

parser = argparse.ArgumentParser()

parser.add_argument("-object_type_person_fname", "--object_type_person_fname",
                    help="Filepath of people.person - type.object.type gz")
parser.add_argument("-entityID_fname", "--entityID_fname",
                    help="Filepath to pickle dump of entityID set")
parser.add_argument("--entityID_wname_fname",
                    help="Filepath to pickle dump of set of entityIDs with names")
parser.add_argument("-type_object_name_fname", "--type_object_name_fname",
                    help="Filepath to type.object.name gz")
parser.add_argument("-entity_name_fname", "--entity_name_fname",
                    help="Output filepath to \'entityID \\t name\' file")
parser.add_argument("--common_topic_alias_fname",
                    help="Filepath to common.topic.alias.en.gz file")
parser.add_argument("--entity_alias_name_fname",
                    help="Output filepath to \'entityID \\t alias name\' file")
parser.add_argument("--default_flags", action="store_true", default=False,
                    help="use default flags")

def default_flags():
  FLAGS.object_type_person_fname = "/save/ngupta19/freebase/people.person.gz"
  FLAGS.entityID_fname = "/save/ngupta19/freebase/people.person.entityIDs"
  FLAGS.entityID_wname_fname = "/save/ngupta19/freebase/people.person.wname.entityIDs"
  FLAGS.type_object_name_fname = "/save/ngupta19/freebase/type.object.name.en.gz"
  FLAGS.entity_name_fname = "/save/ngupta19/freebase/entity.names"
  FLAGS.common_topic_alias_fname = "/save/ngupta19/freebase/common.topic.alias.en.gz"
  FLAGS.entity_alias_name_fname = "/save/ngupta19/freebase/entity.alias.names"
  FLAGS.entity_name_walias_fname = "/save/ngupta19/freebase/entity.names.w.alias"

class UtilityFunctions(FreebaseData):
  def __init__(self):
    pass

  def read_line(self, f):
    line = f.readline()
    if line == '':
      f.close()

    return line

  def mid_set(self, mid_filepath):
    print("# Making mid set ...")
    assert os.path.exists(mid_filepath)
    f = open(mid_filepath, 'r')
    mid_set = set()
    line = self.read_line(f)
    while line != '':
      mid_set.add(line.strip())
      line = self.read_line(f)
    f.close()

    return mid_set

  def type_set(self, types_filepath):
    print("# Making types set ...")
    assert os.path.exists(types_filepath)
    f = open(types_filepath, 'r')
    types_set = set()
    line = self.read_line(f)
    while line != '':
      types_set.add(line.strip())
      line = self.read_line(f)
    f.close()
    print("# Number of types : %d" % len(types_set))
    return types_set

  def all_names_for_mids(self, fb_data, mid_filepath, output_filepath):
    print("# Get all names for entities provided")
    mid_set = self.mid_set(mid_filepath)
    print("Mid Set contains : %d mids" % len(mid_set))
    out_file = open(output_filepath, 'w')
    f = open(fb_data.entity_alias_name_fname, 'r')
    line = self.read_line(f)
    while line != '':
      mid, name = line.strip().split("\t")
      if mid in mid_set:
        out_file.write(line)
      line = self.read_line(f)
    out_file.close()
    f.close()

  def makeMIDValueFile(self, freebase_pred_file, output_filepath):
    '''Input : freebase data in
    <http://rdf.freebase.com/ns/m.0105jw0f>\t<http://rdf.freebase.com/ns/predicate>\t"VALUE"    .

    Output file:
    m.0105jw0f \t Value
    '''
    def cleanValue(s):
      '''Removes \"s '''
      return s[1:-1]
    #enddef
    print("Making mid \\t Value file...")
    f = gzip.open(freebase_pred_file, 'rt')
    out_f = open(output_filepath, 'w')

    line = self.read_line(f)
    while line != '':
      l_split = line.split("\t")
      mid = process_entities.stripRDF(l_split[0])
      name = cleanValue(l_split[2])
      if process_entities.filter_mention(mid):
        out_f.write(str(mid) + "\t" + name + "\n")
      line = self.read_line(f)
    out_f.close()
    f.close()

  def makeMentionTypeFile(self, freebase_pred_file, output_filepath):
    '''Input : freebase data in
    <http://rdf.freebase.com/ns/m.0105jw0f>\t<http://rdf.freebase.com/ns/predicate>\t<http://rdf.freebase.com/ns/#type#>    .

    Output file:
    m.0105jw0f \t type
    '''
    #enddef
    print("Making mid \\t Value file...")
    f = gzip.open(freebase_pred_file, 'rt')
    out_f = open(output_filepath, 'w')

    line = self.read_line(f)
    while line != '':
      l_split = line.split("\t")
      mid = process_entities.stripRDF(l_split[0])
      typ = process_entities.stripRDF(l_split[2])
      if process_entities.filter_mention(mid):
        out_f.write(str(mid) + "\t" + typ + "\n")
      line = self.read_line(f)
    out_f.close()
    f.close()

  def pruneMidTypes(self, types_file, mid_type_file, output_filepath):
    types_set = self.type_set(types_filepath=types_file)
    f = open(mid_type_file, 'r')
    out_f = open(output_filepath, 'w')

    line = self.read_line(f)
    while line != '':
      l_split = line.split("\t")
      typ = l_split[1].strip()
      if typ in types_set:
        out_f.write(line)
      line = self.read_line(f)
    out_f.close()
    f.close()


if __name__ == '__main__':
  FLAGS = parser.parse_args()
  default_flags()
  b = FreebaseData(object_type_person_fname=FLAGS.object_type_person_fname,
                   entityID_fname=FLAGS.entityID_fname,
                   entityID_wname_fname=FLAGS.entityID_wname_fname,
                   type_object_name_fname=FLAGS.type_object_name_fname,
                   entity_name_fname=FLAGS.entity_name_fname,
                   common_topic_alias_fname=FLAGS.common_topic_alias_fname,
                   entity_alias_name_fname=FLAGS.entity_alias_name_fname,
                   entity_name_walias_fname=FLAGS.entity_name_walias_fname)

  util = UtilityFunctions()
  # util.all_names_for_mids(
  #   b,
  #   "/home/ngupta19/rnn-vae/data/fb_min_15/mid_min_15",
  #   "/home/ngupta19/rnn-vae/data/fb_min_15/fb_min_15_names")

  # util.makeMIDValueFile(freebase_pred_file="/save/ngupta19/freebase/wikipedia.en_id.gz",
  #                       output_filepath="/save/ngupta19/freebase/mid.wikipedia_en_id")

  # util.makeMentionTypeFile(freebase_pred_file="/save/ngupta19/freebase/type.object.type.gz",
  #                          output_filepath="/save/ngupta19/freebase/mid.type")

  util.pruneMidTypes(types_file="/save/ngupta19/freebase/types_xiao",
                     mid_type_file="/save/ngupta19/freebase/mid.type",
                     output_filepath="/save/ngupta19/freebase/mid.types")






