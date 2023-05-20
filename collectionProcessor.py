from data_model import *
from processor import Processor

from json import load
from rdflib import Graph, Literal, RDF, RDFS, URIRef
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore


class CollectionProcessor(Processor):
    def __init__(self, dbPathOrUrl: str):
        super(Processor).__init__(dbPathOrUrl)
        # self.dbPathOrUrl = None

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

            endpoint = self.getdbPathOrUrl()

            store.open((endpoint, endpoint))

            for triple in graph.triples((None, None, None)):
                store.add(triple)
            store.close()

            return True

        except Exception as e:
            print(e)
            return False

    def createTriples(self, graph, json_doc):
        # classes of resources
        Collection = URIRef("https://github.com/datasci2023/datascience/res/Collection")
        Manifest = URIRef("https://github.com/datasci2023/datascience/res/Manifest")
        Canvas = URIRef("https://github.com/datasci2023/datascience/res/Canvas")

        # attributes related to classes
        id = URIRef("https://github.com/datasci2023/datascience/attr/id")
        type = URIRef("https://github.com/datasci2023/datascience/attr/type")
        label = URIRef("https://github.com/datasci2023/datascience/attr/label")
        items = URIRef("https://github.com/datasci2023/datascience/attr/items")

        # create graph database
        graph = Graph()

        # create and add triples starting from the collection
        # subject = URIRef(json_doc["id"])
        # # what is the smartest way to define the subject
        graph.add(
            URIRef(json_doc["id"]), id, Literal(json_doc["id"])
        )  # for URIs, check how peroni defined base and generic URIs in hon#05
        graph.add(URIRef(json_doc["id"]), RDF.type, Collection)
        graph.add(
            URIRef(json_doc["id"]), RDFS.label, URIRef(json_doc["label"])
        )  # find a way to add label

        for idx, manifest in json_doc["items"]:
            # subject = URIRef(manifest["id"])
            graph.add(URIRef(json_doc["id"]), items, URIRef(manifest["id"]))
            graph.add(URIRef(manifest["id"]), id, Literal(manifest["id"]))
            graph.add(URIRef(manifest["id"]), RDF.type, Manifest)
            graph.add(URIRef(manifest["id"]), RDFS.label, URIRef(manifest["label"]))

            for idx, canvas in manifest["items"]:
                graph.add(URIRef(manifest["id"]), items, URIRef(manifest["id"]))
                graph.add(URIRef(canvas["id"]), id, Literal(canvas["id"]))
                graph.add(URIRef(canvas["id"]), RDF.type, Canvas)
                graph.add(URIRef(canvas["id"]), RDFS.label, URIRef(canvas["label"]))
