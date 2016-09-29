import os
import sys
import gzip
import pickle
import argparse

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
  def __init__(self, object_type_person_fname, entityID_fname,
               entityID_wname_fname, type_object_name_fname,
               entity_name_fname, common_topic_alias_fname,
               entity_alias_name_fname, entity_name_walias_fname):
    self.object_type_person_fname = object_type_person_fname
    self.entityID_fname = entityID_fname
    self.entityID_wname_fname = entityID_wname_fname
    self.type_object_name_fname = type_object_name_fname
    self.entity_name_fname = entity_name_fname
    self.common_topic_alias_fname = common_topic_alias_fname
    self.entity_alias_name_fname = entity_alias_name_fname
    self.entity_name_walias_fname = entity_name_walias_fname

    if not os.path.exists(self.object_type_person_fname):
      "ERROR: people.person does not exist : %s" % self.object_type_person_fname


    if not os.path.exists(self.entityID_fname):
      print("Creating entity ID set...")
      self.makeEntityIdSet(self.object_type_person_fname, self.entityID_fname)

    print("Loading entity ID set...")
    self.entityIDs = load(self.entityID_fname)
    print("Num of person entityIDs : %d" % len(self.entityIDs))

    if not os.path.exists(self.entity_name_fname) or not os.path.exists(self.entityID_wname_fname):
      print("Creating entity name file...")
      self.entityIDs_wname = self.makeEntityNames(self.type_object_name_fname,
                                                  self.entity_name_fname,
                                                  self.entityID_wname_fname)
    print("Loading entity IDs with names set...")
    self.entityIDs_wname = load(self.entityID_wname_fname)
    print("Number of entityIDs with name : %d" % len(self.entityIDs_wname))

    if not os.path.exists(self.entity_alias_name_fname) or not os.path.exists(self.entity_name_walias_fname):
      print("Making Alias Data")
      self.makeEntityAliasNames(self.common_topic_alias_fname,
                                self.type_object_name_fname,
                                self.entity_alias_name_fname,
                                self.entity_name_walias_fname)
    print("# Loaded Freebase Data!!")


  def read_line(sefl, f):
    line = f.readline()
    if line == '':
      f.close()

    return line

  def makeEntityIdSet(self, object_type_person_fname, entityID_fname):
    '''Input : type.object.type = people.person freebase data
    Removes rdf from url of topic ids.
    Only keep topic ids starting with m.
    '''
    entityIDs = set()
    f = gzip.open(object_type_person_fname, 'rt')
    line = self.read_line(f)
    while line != '':
      entity_url = line.split("\t")[0]
      entity_id = stripRDF(entity_url)
      if filter_mention(entity_id):
        entityIDs.add(entity_id)
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
    entityIDs = set(self.entityIDs)

    line = self.read_line(f)
    while line != '' and len(entityIDs) != 0:
      l_split = line.split("\t")
      entity_id = stripRDF(l_split[0])
      if entity_id in entityIDs and eng_filter(l_split[2]):
        name = cleanValue(l_split[2])
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
                            type_object_name_fname,
                            entity_alias_name_fname,
                            entity_name_walias_fname):
    '''entity_name_fname - name\tentityID

    output_alias_entity_fname - entityID\\tname
    Can have multiple entries for same entityID'''
    def cleanValue(s):
      '''Removes \" and @en'''
      return s[1:-4]
    #enddef

    # Making alias entity ids set
    alias_entityIDs = set()
    f = gzip.open(common_topic_alias_fname, 'rt')
    line = self.read_line(f)
    while line != '':
      entity_url = line.split("\t")[0]
      entity_id = stripRDF(entity_url)
      if filter_mention(entity_id):
        alias_entityIDs.add(entity_id)
      line = self.read_line(f)
    f.close()
    person_walias_entity_IDs = alias_entityIDs.intersection(self.entityIDs_wname)
    print("Number of entities with name and alias : %d" % len(person_walias_entity_IDs))
    num_alias_names = 0
    # Open common.topic.alias file
    f = gzip.open(common_topic_alias_fname, 'rt')
    out_f = open(entity_alias_name_fname, 'w')
    line = self.read_line(f)
    while line != '':
      l_split = line.split("\t")
      entity_id = stripRDF(l_split[0])
      if entity_id in person_walias_entity_IDs and eng_filter(l_split[2]):
        alias_name = cleanValue(l_split[2])
        out_f.write(str(entity_id) + "\t" + alias_name + "\n")
        num_alias_names += 1
      line = self.read_line(f)
    f.close()
    print("Number of alias strings : %d" % num_alias_names)
    num_alias_orig_names = 0
    # Open type.object.name.en file
    f = gzip.open(type_object_name_fname, 'rt')
    out_f2 = open(entity_name_walias_fname, 'w')
    line = self.read_line(f)
    while line != '':
      l_split = line.split("\t")
      entity_id = stripRDF(l_split[0])
      if entity_id in person_walias_entity_IDs and eng_filter(l_split[2]):
        name = cleanValue(l_split[2])
        out_f.write(str(entity_id) + "\t" + name + "\n")
        out_f2.write(str(entity_id) + "\t" + name + "\n")
        num_alias_orig_names += 1
      line = self.read_line(f)
    f.close()
    print("Number of original name strings : %d" % num_alias_orig_names)
    out_f.close()
    out_f2.close()


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

