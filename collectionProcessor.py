from processor import Processor
from json import load
from rdflib import Graph, Literal, RDF, URIRef
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore


# classes of resources
EntitywithMetadata = URIRef('')
Collection = URIRef('')
Manifest = URIRef('')
Canvas = URIRef('')

# attributes related to classes
# attributes in json file or in UML ????
id = URIRef('')
type = URIRef('')
label = URIRef('')
item = URIRef('')
title = URIRef('')
# ...


class CollectionProcessor(Processor):
    def __init__(self):
        super(Processor).__init__()
        # database path

    def uploadData(self, path):
        graph = create_triples(self, path)
        store = SPARQLUpdateStore()
        endpoint = ''  # database path

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
                    for name_l, value_l in value.items():  # might not work
                        object = Literal(value_l)
                        triple = (subject, label, object)
                        graph.add(triple)

                if name == 'item':
                    for d in value:
                        # another triple for item???
                        create_triples(graph, d)
