from data_model import *
from processor import Processor

from json import load
from rdflib import Graph, Literal, RDF, RDFS, URIRef
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore

# classes of resources - pomodoro
Collection = URIRef("https://github.com/datasci2023/datascience/class/Collection")
Manifest = URIRef("https://github.com/datasci2023/datascience/class/Manifest")
Canvas = URIRef("https://github.com/datasci2023/datascience/class/Canvas")

# attributes related to classes - basilico
id = URIRef("https://github.com/datasci2023/datascience/attribute/id")
type = URIRef("https://github.com/datasci2023/datascience/attribute/type")
label = URIRef("https://github.com/datasci2023/datascience/attribute/label")


# relations - spaghetti
items = URIRef("https://github.com/datasci2023/datascience/relation/items")


class CollectionProcessor(Processor):
    def __init__(self):
        super().__init__()

    def uploadData(self, path):
        try:
            graph = Graph()
            with open(path, "r", encoding="utf-8") as f:
                json_doc = load(f)

            if not json_doc:
                print(
                    "File is an empty structure"
                )  # empty dict or empty list -- but consider boolean return case

            # for collection in json_doc:
            #     create_triples(graph, collection)
            # checking if the json file is a list or not might be a better way
            if isinstance(json_doc, list):
                for collection in json_doc:
                    self.createTriples(graph, collection)
            else:
                self.createTriples(graph, json_doc)

            store = SPARQLUpdateStore()

            endpoint = self.getDbPathOrUrl()

            store.open((endpoint, endpoint))

            for triple in graph.triples((None, None, None)):
                store.add(triple)
            store.close()

            return True

        except Exception as e:
            print(e)
            return False

    def createTriples(self, graph, json_doc):
        # create and add triples starting from the collection
        # for URIs, check how peroni defined base and generic URIs in hon#05
        graph.add((URIRef(json_doc["id"]), RDF.type, Collection))
        graph.add(
            (URIRef(json_doc["id"]), RDFS.label, Literal(json_doc["label"]))
        )  # find a way to add label

        for manifest in json_doc["items"]:
            graph.add((URIRef(json_doc["id"]), items, URIRef(manifest["id"])))
            graph.add((URIRef(manifest["id"]), RDF.type, Manifest))
            graph.add(
                (
                    URIRef(manifest["id"]),
                    RDFS.label,
                    Literal("".join(list(manifest["label"].values())[0])),
                )
            )

            for canvas in manifest["items"]:
                graph.add((URIRef(manifest["id"]), items, URIRef(canvas["id"])))
                graph.add((URIRef(canvas["id"]), RDF.type, Canvas))
                graph.add(
                    (
                        URIRef(canvas["id"]),
                        RDFS.label,
                        Literal("".join(list(canvas["label"].values())[0])),
                    )
                )
