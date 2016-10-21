import os
import re
import sys
import gzip
import pickle
import argparse
import time

parser = argparse.ArgumentParser()

parser.add_argument("-mid_type_fname", "--mid_type_fname",
                    help="Filepath of mid.types")
parser.add_argument("-entityID_fname", "--entityID_fname",
                    help="Filepath to pickle dump of entityID set")
parser.add_argument("--entityID_wname_fname",
                    help="Filepath to pickle dump of set of MIDs with names")
parser.add_argument("-type_object_name_fname", "--type_object_name_fname",
                    help="Filepath to type.object.name gz")
parser.add_argument("-entity_name_fname", "--entity_name_fname",
                    help="Output filepath to \'entityID \\t name\' file")
parser.add_argument("--common_topic_alias_fname",
                    help="Filepath to common.topic.alias.en.gz file")
parser.add_argument("--entity_alias_name_fname",
                    help="Output filepath to \'entityID \\t alias name\' file")
parser.add_argument("--type_object_type_fname",
                    help="Filepath to type.object.type.gz")
parser.add_argument("--types_list",
                    help="List of relevant freebase types")
parser.add_argument("--all_mid_type_file",
                    help="Filepath containing all FB mid \\t type")
parser.add_argument("--wikipedia_en_id_gz",
                    help="Filepath containing wikipedia.en_id predicate")
parser.add_argument("--mid_wiki_id_fname",
                    help="Output file for all FB mid \\t wid")
parser.add_argument("--mid_name_wid_fname",
                    help="Filepath containing relevant mid \\t name \\t wid")
parser.add_argument("--default_flags", action="store_true", default=False,
                    help="use default flags")

def default_flags():
  #FLAGS.mid_type_fname = "/save/ngupta19/freebase/mid.types"
  #FLAGS.entityID_fname = "/save/ngupta19/freebase/mids_xiao"
  #FLAGS.entityID_wname_fname = "/save/ngupta19/freebase/mids_wnames_xiao"
  #FLAGS.entity_name_fname = "/save/ngupta19/freebase/mid.names"
  #FLAGS.entity_alias_name_fname = "/save/ngupta19/freebase/mid.alias.names"
  #FLAGS.entity_name_walias_fname = "/save/ngupta19/freebase/mid.names.w.alias"

  FLAGS.type_object_name_fname = "/save/ngupta19/freebase/type.object.name.en.gz"
  FLAGS.common_topic_alias_fname = "/save/ngupta19/freebase/common.topic.alias.en.gz"
  FLAGS.type_object_type_fname = "/save/ngupta19/freebase/type.object.type.gz"
  FLAGS.types_list = "/save/ngupta19/freebase/types_pruned/types_nitish"
  FLAGS.all_mid_type_file = "/save/ngupta19/freebase/types_pruned/all.mid.types"
  FLAGS.mid_type_fname = "/save/ngupta19/freebase/types_pruned/mid.types.pruned"
  FLAGS.entityID_fname = "/save/ngupta19/freebase/types_pruned/mids_pruned"
  FLAGS.entityID_wname_fname = "/save/ngupta19/freebase/types_pruned/mids_wnames_pruned"
  FLAGS.entity_name_fname = "/save/ngupta19/freebase/types_pruned/mid.names"
  FLAGS.entity_alias_name_fname = "/save/ngupta19/freebase/types_pruned/mid.alias.names"
  FLAGS.wikipedia_en_id_gz = "/save/ngupta19/freebase/wikipedia.en_id.gz"
  FLAGS.mid_wiki_id_fname = "/save/ngupta19/freebase/mid.wikipedia_en_id"
  FLAGS.mid_name_wid_fname = "/save/ngupta19/freebase/types_pruned/mid.names.wiki_id_en"

def save(fname, obj):
  with open(fname, 'wb') as f:
    pickle.dump(obj, f)

def load(fname):
  with open(fname, 'rb') as f:
    return pickle.load(f)

def stripRDF(url):
  if (url.startswith("<http://rdf.freebase.com") and url.endswith(">")):
    url = url.replace("<http://rdf.freebase.com/ns/", "").replace(
      "<http://rdf.freebase.com/key/", "")[:-1]
  return url

def filter_mention(entityID):
  return entityID.startswith("m.")

def eng_filter(s):
  return s.endswith("@en")

class FreebaseData(object):
  def __init__(self, type_object_type_fname,
               types_list_fname, all_mid_type_fname, mid_type_fname,
               entityID_fname, entityID_wname_fname, type_object_name_fname,
               entity_name_fname, common_topic_alias_fname,
               entity_alias_name_fname,
               wikipedia_en_id_gz, mid_wiki_id_fname, mid_name_wid_fname):
    self.type_object_type_fname = type_object_type_fname
    self.types_list_fname = types_list_fname
    self.all_mid_type_fname = all_mid_type_fname
    self.mid_type_fname = mid_type_fname
    self.entityID_fname = entityID_fname
    self.entityID_wname_fname = entityID_wname_fname
    self.type_object_name_fname = type_object_name_fname
    self.entity_name_fname = entity_name_fname
    self.common_topic_alias_fname = common_topic_alias_fname
    self.entity_alias_name_fname = entity_alias_name_fname
    self.wikipedia_en_id_gz = wikipedia_en_id_gz
    self.mid_wiki_id_fname = mid_wiki_id_fname
    self.mid_name_wid_fname = mid_name_wid_fname

    assert os.path.exists(self.types_list_fname), "File with relevant types NE."

    if not os.path.exists(self.mid_type_fname):
      if not os.path.exists(self.all_mid_type_fname):
        self.makeMentionTypeFile(self.type_object_type_fname, self.all_mid_type_fname)
      self.pruneMidTypes(self.types_list_fname, self.all_mid_type_fname,
                         self.mid_type_fname)



    if not os.path.exists(self.mid_type_fname):
      "ERROR: mid.types does not exist : %s" % self.mid_type_fname

    if not os.path.exists(self.entityID_fname):
      print("[#] Creating MID set for entities with relevant types...")
      self.makeEntityIdSet(self.mid_type_fname, self.entityID_fname)

    print("Loading MID set...")
    self.entityIDs = load(self.entityID_fname)
    print("[#] Num of MIDs with relevant type: %d" % len(self.entityIDs))

    if not os.path.exists(self.type_object_name_fname):
      "ERROR: type.object.name.en does not exist : %s" % self.type_object_name_fname

    if not os.path.exists(self.entity_name_fname) or not os.path.exists(self.entityID_wname_fname):
      print("[#] Creating mid.names file for mids with relevant type...")
      self.makeEntityNames(self.type_object_name_fname,
                           self.entity_name_fname,
                           self.entityID_wname_fname)
    print("[#] Loading MIDs with names and relevant type...")
    self.entityIDs_wname = load(self.entityID_wname_fname)
    print("[#] Number of MIDs with name : %d" % len(self.entityIDs_wname))

    if not os.path.exists(self.common_topic_alias_fname):
      "ERROR: common.topic.alias.en does not exist : %s" % self.common_topic_alias_fname

    if not os.path.exists(self.entity_alias_name_fname):
      print("[#] Making Alias Data...")
      self.makeEntityAliasNames(self.common_topic_alias_fname,
                                self.entity_name_fname,
                                self.entity_alias_name_fname)
    else:
      print("[#] Alias Data already exists!")

    if not os.path.exists(self.mid_wiki_id_fname):
      print("[#] Making mid \\t wiki_en_id file ...")
      self.makeMIDValueFile(self.wikipedia_en_id_gz, self.mid_wiki_id_fname)
    else:
      print("[#] mid \\t wiki_en_id file already exists!")

    if not os.path.exists(self.mid_name_wid_fname):
      print("[#] Making mid \\t name \\t wiki_id file ... ")
      self.joinMIDNamesMIDWID(self.entity_name_fname,
                              self.mid_wiki_id_fname,
                              self.mid_name_wid_fname)
    else:
      print("[#] mid \\t name \\t wiki_id file already exists")
    print("# Loaded Freebase Data!!")


  def read_line(sefl, f):
    line = f.readline()
    if line == '':
      f.close()

    return line

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


  def makeEntityIdSet(self, mid_type_fname, entityID_fname):
    '''Input : mid \t type - Pruned from type.object.type with type contained
         in types_xiao
       Only keep topic ids starting with m.
    '''
    entityIDs = set()
    f = open(mid_type_fname, 'r')
    line = self.read_line(f)
    while line != '':
      mid = line.split("\t")[0].strip()
      if filter_mention(mid):
        entityIDs.add(mid)
      line = self.read_line(f)
    f.close()
    save(entityID_fname, entityIDs)

  def makeEntityNames(self, type_object_name_fname, entity_name_fname,
                      entityID_wname_fname):
    '''Input : type.object.name predicate freebase data
    Removes rdf from topic id url
    if id exists in entityIDs then, strip name and store
    '''
    def cleanValue(s):
      '''Removes \" and @en'''
      return s[1:-4]
    #enddef

    f = gzip.open(type_object_name_fname, 'rt')
    out_f = open(entity_name_fname, 'w')
    # Duplicating to make relevant MID set
    entityIDs = set(self.entityIDs)
    pattern = re.compile(r'\s+')

    line = self.read_line(f)
    while line != '' and len(entityIDs) != 0:
      l_split = line.split("\t")
      entity_id = stripRDF(l_split[0])
      if entity_id in entityIDs and eng_filter(l_split[2]):
        name = cleanValue(l_split[2])
        name = re.sub(pattern, ' ', name)
        # ADD LIMIT TO SIZE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        out_f.write(str(entity_id) + "\t" + name + "\n")
        entityIDs.remove(entity_id)
      line = self.read_line(f)
    out_f.close()
    f.close()
    # entityIDs - person entities that do not have name
    # self.entityIDs - all person entities
    # diff should give entities with names.
    entityIDs_wname = self.entityIDs.difference(entityIDs)
    save(entityID_wname_fname, entityIDs_wname)

  def  makeEntityAliasNames(self, common_topic_alias_fname,
                            entity_name_fname,
                            entity_alias_name_fname):
    '''entity_name_fname - name\tentityID

    output_alias_entity_fname - entityID\\tname
    Can have multiple entries for same entityID'''
    def cleanValue(s):
      '''Removes \" and @en'''
      return s[1:-4]
    #enddef
    num_alias_names = 0
    # Making alias entity ids set
    f = gzip.open(common_topic_alias_fname, 'rt')
    out_f = open(entity_alias_name_fname, 'w')
    pattern = re.compile(r'\s+')

    line = self.read_line(f)
    while line != '':
      l_split = line.split("\t")
      entity_url = l_split[0].strip()
      mid = stripRDF(entity_url)
      if mid in self.entityIDs_wname and eng_filter(l_split[2]):
        alias_name = cleanValue(l_split[2])
        alias_name = re.sub(pattern, ' ', alias_name)
        # ADD LIMIT TO SIZE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        out_f.write(str(mid) + "\t" + alias_name + "\n")
        num_alias_names += 1
      line = self.read_line(f)
    f.close()

    # Copying all entity names from mid.names
    f = open(entity_name_fname, 'r')
    line = self.read_line(f)
    while line != '':
      out_f.write(line)
      line = self.read_line(f)

    f.close()
    out_f.close()
    print("Number of alias names written %d : " % num_alias_names)

  def joinMIDNamesMIDWID(self, mid_names, mid_wids, output_file):
    f = open(mid_wids, 'r')

    mid_wid_dict = {}
    line = self.read_line(f)
    while line != '':
      l_split = line.split("\t")
      (mid, wid) = (l_split[0].strip(), l_split[1].strip())
      mid_wid_dict[mid] = wid
      line = self.read_line(f)
    # end while
    f.close()
    print("Total Mids = %d" % len(mid_wid_dict))

    f = open(mid_names, 'r')
    out_f = open(output_file, 'w')
    line = self.read_line(f)
    while line != '':
      l_split = line.split("\t")
      (mid, name) = (l_split[0].strip(), l_split[1].strip())
      if mid in mid_wid_dict:
        out_f.write(str(mid) + "\t" + str(name) + "\t" + str(mid_wid_dict[mid]) + "\n")
      line = self.read_line(f)
    # end while
    f.close()
    out_f.close()
  #end def

  def makeMIDValueFile(self, freebase_pred_file, output_filepath):
    ''' To make mid \t wiki_id file
    Input : freebase data in
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

if __name__ == '__main__':
  FLAGS = parser.parse_args()
  default_flags()
  b = FreebaseData(type_object_type_fname=FLAGS.type_object_type_fname,
                   types_list_fname=FLAGS.types_list,
                   all_mid_type_fname=FLAGS.all_mid_type_file,
                   mid_type_fname=FLAGS.mid_type_fname,
                   entityID_fname=FLAGS.entityID_fname,
                   entityID_wname_fname=FLAGS.entityID_wname_fname,
                   type_object_name_fname=FLAGS.type_object_name_fname,
                   entity_name_fname=FLAGS.entity_name_fname,
                   common_topic_alias_fname=FLAGS.common_topic_alias_fname,
                   entity_alias_name_fname=FLAGS.entity_alias_name_fname,
                   wikipedia_en_id_gz=FLAGS.wikipedia_en_id_gz,
                   mid_wiki_id_fname=FLAGS.mid_wiki_id_fname,
                   mid_name_wid_fname=FLAGS.mid_name_wid_fname)


