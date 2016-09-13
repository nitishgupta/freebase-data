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
parser.add_argument("-type_object_name_fname", "--type_object_name_fname",
                    help="Filepath to type.object.name gz")
parser.add_argument("-entity_name_fname", "--entity_name_fname",
                    help="Output filepath to \'entityID \\t name\' file")


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

def en_filter(s):
  return s.endswith("@en")

class FreebaseData(object):
  def __init__(self, object_type_person_fname, entityID_fname,
               type_object_name_fname, entity_name_fname):
    self.object_type_person_fname = object_type_person_fname
    self.entityID_fname = entityID_fname
    self.type_object_name_fname = type_object_name_fname
    self.entity_name_fname = entity_name_fname

    if not os.path.exists(self.object_type_person_fname):
      "ERROR: people.person does not exist : %s" % self.object_type_person_fname


    if not os.path.exists(self.entityID_fname):
      print("Creating entity ID set...")
      self.makeEntityIdSet(self.object_type_person_fname, self.entityID_fname)

    print("Loading entity ID set...")
    self.entityIDs = load(self.entityID_fname)
    print("Size of entityIDs set : %d" % len(self.entityIDs))

    self.makeEntityNames(self.type_object_name_fname, self.entity_name_fname)


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

  def makeEntityNames(self, type_object_name_fname, entity_name_fname):
    '''Input : type.object.name predicate freebase data
    Removes rdf from topic id url
    if id exists in entityIDs then, strip name and store
    '''
    def cleanValue(s):
      return s[1:-4]


    f = gzip.open(type_object_name_fname, 'rt')
    out_f = open(entity_name_fname, 'w')
    entityIDs = set(self.entityIDs)

    line = self.read_line(f)
    while line != '' and len(entityIDs) != 0:
      l_split = line.split("\t")
      entity_id = stripRDF(l_split[0])
      if entity_id in entityIDs:
        name = l_split[2]
        if en_filter(name):
          name = cleanValue(name)
          out_f.write(str(entity_id) + "\t" + name + "\n")
          entityIDs.remove(entity_id)
      line = self.read_line(f)

    out_f.close()
    f.close()


if __name__ == '__main__':
  FLAGS = parser.parse_args()
  b = FreebaseData(object_type_person_fname=FLAGS.object_type_person_fname,
                   entityID_fname=FLAGS.entityID_fname,
                   type_object_name_fname=FLAGS.type_object_name_fname,
                   entity_name_fname=FLAGS.entity_name_fname)

