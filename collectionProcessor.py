from processor import Processor
from json import load
from rdflib import Graph, Literal, RDF, URIRef
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore


# classes of resources
EntitywithMetadata = URIRef('')
Collection = URIRef('')
Manifest = URIRef('')
Canvas = URIRef('')
#why not adding also
IdentifiableEntity = URIRef('')
Annotation = URIRef('')
Image = URIRef('')


# attributes related to classes
# attributes in json file or in UML ???? >> I think in the UML 
id = URIRef('')
type = URIRef('') #why this since it the json it addresses the class to which the item belongs?
label = URIRef('')
item = URIRef('') #i think this is viewed as a relation within the collection (see point below), so it's not an attribute (???)
title = URIRef('')
# probably these two are totally useless but let's keep them here??
# creators = URIRef('')
# motivation = URIRef('')

# I think these are also to be added: 
# relations among classes
target = URIRef('')
body = URIRef('')
items = URIRef('')


class CollectionProcessor(Processor):
    def __init__(self):
        super(Processor).__init__()
        # database path
        # self.dbPathOrUrl = None

    def uploadData(self, path):
        graph = create_triples(self, path) 
        store = SPARQLUpdateStore()
        endpoint = ''  # database path == self.dbPathOrUrl

        store.open((endpoint, endpoint))

        for triple in graph.triples((None, None, None)):
            store.add(triple)

        store.close()

    def create_triples(self, path):
        graph = Graph()

        with open(path, "r", encoding="utf-8") as f:
            json_doc = load(f)

            for name, value in json_doc.items(): 
                if name == 'id':
                    subject = URIRef(value)

                if name == 'type':
                    # should we add collection case(???)
                    if value == 'Collection':  # json.dump() ???
                        object = Collection 

                    if value == 'Manifest':  # json.dump() ???
                        object = Manifest

                    if value == 'Canvas':  # json.dump() ???
                        object = Canvas

                    triple = (subject, RDF.type, object)
                    graph.add(triple)

                if name == 'label':
                    object = label
                    for name_l, value_l in value:
                        object = name_l
                        triple= (subject, label, object)
                        graph.add(triple)

                        s = name_l
                        o = value_l
                        triple = (s, RDF.type, o)
                        graph.add(triple)
                        
                if name == 'items': 
                    for d in value:  
                        # another triple for item???
                        create_triples(graph, d)