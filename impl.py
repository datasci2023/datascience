from sqlite3 import connect
from pandas import read_csv, Series, DataFrame, merge, read_sql, concat
from json import load
from rdflib import Graph, Literal, RDF, RDFS, URIRef
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from sparql_dataframe import get

import pandas as pd
import os

# --- DATA MODEL ---


class IdentifiableEntity:
    def __init__(self, id):
        self.id = id

    def getId(self) -> str:
        return self.id


class Image(IdentifiableEntity):
    pass


class Annotation(IdentifiableEntity):
    def __init__(self, id, motivation: str, target, body):
        self.motivation = motivation
        self.target = target
        self.body = body

        super().__init__(id)

    def getBody(self) -> Image:
        return self.body

    def getMotivation(self) -> str:
        return self.motivation

    def getTarget(self) -> IdentifiableEntity:
        return self.target


class EntityWithMetadata(IdentifiableEntity):
    def __init__(self, id, label: str, title: str, creators: str):
        self.label = label
        self.title = title
        self.creators = creators

        super().__init__(id)

    def getLabel(self) -> str:
        return self.label

    def getTitle(self) -> str:
        return self.title

    def getCreators(self) -> list[str]:
        return self.creators


class Canvas(EntityWithMetadata):
    def __init__(self, id, label: str, title: str, creators: list[str]):
        self.label = label
        self.title = title
        self.creators = list()

        for creator in creators:
            self.creators.append(creator)

        super().__init__(id, label, title, creators)


class Manifest(EntityWithMetadata):
    def __init__(
        self, id, label: str, title: str, creators: list[str], items: list[Canvas]
    ):
        self.label = label
        self.title = title
        self.creators = list()
        self.items = list()

        for creator in creators:
            self.creators.append(creator)

        for item in items:
            self.items.append(item)

        super().__init__(id, label, title, creators)

    def getItems(self) -> list[Canvas]:
        result = list()
        for item in self.items:
            result.append(item)

        return result


class Collection(EntityWithMetadata):
    def __init__(
        self, id, label: str, title: str, creators: list[str], items: list[Manifest]
    ):
        self.label = label
        self.title = title
        self.creators = list()
        self.items = list()

        for creator in creators:
            self.creators.append(creator.lstrip(" "))

        for item in items:
            self.items.append(item)

        super().__init__(id, label, title, creators)

    def getItems(self) -> list[Manifest]:
        return self.items


# --- PROCESSOR ---


class Processor(object):
    def __init__(self) -> None:
        self.dbPathOrUrl = ""

    def getDbPathOrUrl(self) -> str:
        return self.dbPathOrUrl

    def setDbPathOrUrl(self, pathOrUrl: str) -> bool:
        if os.path.isfile(pathOrUrl) or pathOrUrl.endswith(".db"):
            self.dbPathOrUrl = pathOrUrl
            return True
        elif pathOrUrl.startswith("https:") or pathOrUrl.startswith("http:"):
            self.dbPathOrUrl = pathOrUrl
            return True
        return False


# --- METADATA PROCESSOR ---


class MetadataProcessor(Processor):
    def __init__(self):
        super().__init__()

    def uploadData(self, path):
        try:
            with connect(self.dbPathOrUrl) as con:
                path = read_csv(
                    path,
                    keep_default_na=False,
                    dtype={"id": "string", "title": "string", "creator": "string"},
                )

                creators = pd.DataFrame()
                creator_list = []
                for value in path["creator"]:
                    if value != "":
                        creator_list.append(value)
                creators.insert(0, "creator", Series(creator_list, dtype="string"))
                creators = creators.rename(columns={"creator": "creator_name"})
                creators["creator_name"] = creators["creator_name"].str.split(";")
                creators_def = creators.explode("creator_name")
                creators_def = creators_def.reset_index(drop=True)
                internal_id_dict = {}
                creator_internal_id = []

                for idx, row in creators_def.iterrows():
                    creator = row["creator_name"]

                    if creator in internal_id_dict:
                        creators_def.drop(idx, inplace=True)
                    else:
                        internal_id = "creator-" + str(len(internal_id_dict))
                        creator_internal_id.append(internal_id)
                        internal_id_dict[creator] = internal_id

                creators_def.insert(
                    0,
                    "creator_internal_id",
                    Series(creator_internal_id, dtype="string"),
                )

                metadata_entities = path[["id", "title", "creator"]]
                metadata_entities["creator"] = metadata_entities["creator"].str.split(
                    ";"
                )
                metadata_entities = metadata_entities.explode("creator")
                metadata_entities = metadata_entities.reset_index(drop=True)
                metadata_merged = merge(
                    metadata_entities,
                    creators_def,
                    left_on="creator",
                    right_on="creator_name",
                    how="left",
                )
                metadata_def = metadata_merged[["id", "title", "creator_internal_id"]]
                metadata_def = metadata_def.rename(
                    columns={"creator_internal_id": "creator"}
                )
                internal_id_dict1 = {}
                metadata_internal_id = []
                for idx, row in metadata_def.iterrows():
                    entity = row["id"]

                    if entity in internal_id_dict1:
                        metadata_internal_id.append(internal_id_dict1[entity])
                    else:
                        internal_id1 = "metadata-" + str(len(internal_id_dict1))
                        metadata_internal_id.append(internal_id1)
                        internal_id_dict1[entity] = internal_id1
                metadata_def.insert(
                    0,
                    "metadata_internal_id",
                    Series(metadata_internal_id, dtype="string"),
                )
                try:
                    query = f"""
                    SELECT * FROM Annotations
                    """
                    df = pd.read_sql_query(query, con)
                    if not df.empty:
                        query = "SELECT * FROM 'Annotations'"
                        annotation_temp = pd.read_sql_query(query, con)
                        annotation_merged = merge(
                            annotation_temp,
                            metadata_def,
                            left_on="annotation_targets",
                            right_on="id",
                        )
                        annotation_table = annotation_merged[
                            [
                                "annotation_ids",
                                "annotation_internal_id",
                                "annotation_bodies",
                                "metadata_internal_id",
                                "annotation_motivations",
                            ]
                        ]
                        annotation_table = annotation_table.rename(
                            columns={"metadata_internal_id": "annotation_targets"}
                        )
                        annotation_table.to_sql(
                            "Annotations", con, if_exists="replace", index=False
                        )
                except:
                    pass

                metadata_def.to_sql(
                    "EntitiesWithMetadata", con, if_exists="replace", index=False
                )
                creators_def.to_sql("Creators", con, if_exists="replace", index=False)

                con.commit()

                return True

        except Exception as e:
            print(e)
            return False


# --- ANNOTATION PROCESSOR ---


class AnnotationProcessor(Processor):
    def __init__(self):
        super().__init__()

    def uploadData(self, path2):
        try:
            with connect(self.dbPathOrUrl) as con:
                path2 = read_csv(
                    path2,
                    keep_default_na=False,
                    dtype={
                        "id": "string",
                        "body": "string",
                        "target": "string",
                        "motivation": "string",
                    },
                )

                annotation_table = pd.DataFrame()
                annotation_ids = []
                for idx, value in path2["id"].items():
                    annotation_ids.append(value)

                annotation_table.insert(
                    0, "annotation_ids", Series(annotation_ids, dtype="string")
                )

                annotation_internal_id = []
                for idx, row in path2.iterrows():
                    annotation_internal_id.append("annotation-" + str(idx))

                annotation_table.insert(
                    0,
                    "annotation_internal_id",
                    Series(annotation_internal_id, dtype="string"),
                )

                annotation_bodies = []
                for idx, value in path2["body"].items():
                    annotation_bodies.append(value)

                annotation_table.insert(
                    2, "annotation_bodies", Series(annotation_bodies, dtype="string")
                )

                annotation_targets = []
                for idx, value in path2["target"].items():
                    annotation_targets.append(value)

                annotation_table.insert(
                    3, "annotation_targets", Series(annotation_targets, dtype="string")
                )

                annotation_motivations = []
                for idx, value in path2["motivation"].items():
                    annotation_motivations.append(value)

                annotation_table.insert(
                    4,
                    "annotation_motivations",
                    Series(annotation_motivations, dtype="string"),
                )
                try:
                    query = "SELECT * FROM EntitiesWithMetadata"
                    metadata_temp = pd.read_sql_query(query, con)

                    annotation_merged = merge(
                        annotation_table,
                        metadata_temp,
                        left_on="annotation_targets",
                        right_on="id",
                    )
                    annotation_table = annotation_merged[
                        [
                            "annotation_ids",
                            "annotation_internal_id",
                            "annotation_bodies",
                            "metadata_internal_id",
                            "annotation_motivations",
                        ]
                    ]
                    annotation_table = annotation_table.rename(
                        columns={"metadata_internal_id": "annotation_targets"}
                    )
                except:
                    pass

                image = pd.DataFrame()
                image_ids = []
                for index, value in path2["body"].items():
                    image_ids.append(value)

                image.insert(0, "image_ids", Series(image_ids, dtype="string"))

                image_internal_id = []
                for idx, rows in image.iterrows():
                    image_internal_id.append("images-" + str(idx))

                image.insert(
                    0, "images_internal_id", Series(image_internal_id, dtype="string")
                )

                annotation_merged2 = merge(
                    annotation_table,
                    image,
                    left_on="annotation_bodies",
                    right_on="image_ids",
                )
                annotation_def = annotation_merged2[
                    [
                        "annotation_internal_id",
                        "annotation_ids",
                        "images_internal_id",
                        "annotation_targets",
                        "annotation_motivations",
                    ]
                ]
                annotation_def = annotation_def.rename(
                    columns={"images_internal_id": "annotation_bodies"}
                )

                annotation_def.to_sql(
                    "Annotations", con, if_exists="replace", index=False
                )
                image.to_sql("Images", con, if_exists="replace", index=False)

                con.commit()

                return True

        except Exception as e:
            print(e)
            return False


# --- COLLECTION PROCESSOR ---


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
                ) 

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
        # classes of resources - pomodoro
        Collection = URIRef(
            "https://github.com/datasci2023/datascience/class/Collection"
        )
        Manifest = URIRef("https://github.com/datasci2023/datascience/class/Manifest")
        Canvas = URIRef("https://github.com/datasci2023/datascience/class/Canvas")

        # attributes related to classes - basilico
        id = URIRef("https://github.com/datasci2023/datascience/attribute/id")
        # type = URIRef("https://github.com/datasci2023/datascience/attribute/type")
        label = URIRef("https://github.com/datasci2023/datascience/attribute/label")

        # relations - spaghetti
        items = URIRef("https://github.com/datasci2023/datascience/relation/items")

        graph.add((URIRef(json_doc["id"]), RDF.type, Collection))
        graph.add(
            (
                URIRef(json_doc["id"]),
                RDFS.label,
                Literal("".join(list(json_doc["label"].values())[0])),
            )
        )

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


# --- QUERY PROCESSOR ---


class QueryProcessor(Processor):
    def __init__(self):
        super().__init__()

    def getEntityById(self, entityId: str):
        path = self.getDbPathOrUrl()

        if os.path.isfile(path) or path.endswith(".db"):
            with connect(self.dbPathOrUrl) as con:
                query = f"""
                    SELECT
                        EntitiesWithMetadata.*,
                        Annotations.*,
                        Images.*,
                        TRIM(Creators.creator_name) AS creator_name,
                        CASE
                            WHEN EntitiesWithMetadata.id = '{entityId}' THEN 'EntitiesWithMetadata'
                            WHEN Annotations.annotation_ids = '{entityId}' THEN 'Annotations'
                            WHEN Images.image_ids = '{entityId}' THEN 'Images'
                            ELSE NULL
                        END AS entityId_table
                    FROM EntitiesWithMetadata
                    LEFT JOIN Annotations ON EntitiesWithMetadata.metadata_internal_id = Annotations.annotation_targets
                    LEFT JOIN Images ON Annotations.annotation_bodies = Images.images_internal_id
                    LEFT JOIN Creators ON EntitiesWithMetadata.creator = Creators.creator_internal_id
                    WHERE EntitiesWithMetadata.id = '{entityId}' OR Annotations.annotation_ids = '{entityId}' OR Images.image_ids = '{entityId}'
                """
                df = read_sql(query, con)
                return df

        elif path.startswith("https:") or path.startswith("http:"):
            endpoint = self.getDbPathOrUrl()
            query = (
                (
                    """
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX pomodoro: <https://github.com/datasci2023/datascience/class/>
            PREFIX feslegen: <https://github.com/datasci2023/datascience/attribute/>
            PREFIX spaghetti:  <https://github.com/datasci2023/datascience/relation/>
        
            SELECT ?id ?label ?items (strafter(str(?t), 'class/') AS ?type)
            WHERE {
                ?id rdf:type ?t ;
                    rdfs:label ?label .
                
                OPTIONAL {?id spaghetti:items ?items}
                FILTER (?id = <%s>)
            }
            """
                )
                % entityId
            )
            df_sparql = get(endpoint, query, True)
            return df_sparql
        else:
            print("Error!!!")

            df_sparql = get(endpoint, query, True)
            return df_sparql


# --- RELATIONAL QUERY PROCESSOR ---


class RelationalQueryProcessor(QueryProcessor):
    def __init__(self):
        super().__init__()

    def getAllAnnotations(self):
        with connect(self.dbPathOrUrl) as con:
            query = "SELECT * FROM Annotations"
            df_sql = read_sql(query, con)
            return df_sql

    def getAllImages(self):
        with connect(self.dbPathOrUrl) as con:
            query = "SELECT * FROM Images"
            df_sql = read_sql(query, con)
            return df_sql

    def getAllEntites(self):
        with connect(self.dbPathOrUrl) as con:
            query = "SELECT * FROM EntitiesWithMetadata"
            df_sql = read_sql(query, con)
            return df_sql

    def getAnnotationsWithBody(self, bodyId: str):
        with connect(self.dbPathOrUrl) as con:
            query = f"""
            SELECT * FROM Annotations
            JOIN Images ON Annotations.annotation_bodies == Images.images_internal_id
            WHERE image_ids = '{bodyId}'
            """
            df_sql = read_sql(query, con)
            return df_sql

    def getAnnotationsWithBodyAndTarget(self, bodyId: str, targetId: str):
        with connect(self.dbPathOrUrl) as con:
            query = f"""
            SELECT * FROM Annotations
            JOIN Images ON Annotations.annotation_bodies == Images.images_internal_id 
            JOIN EntitiesWithMetadata ON Annotations.annotation_targets == EntitiesWithMetadata.metadata_internal_id
            WHERE image_ids = '{bodyId}' AND id = '{targetId}'
            """
            df_sql = read_sql(query, con)
            return df_sql

    def getAnnotationsWithTarget(self, targetId: str):
        with connect(self.dbPathOrUrl) as con:
            query = f"""
            SELECT * FROM Annotations
            JOIN EntitiesWithMetadata ON Annotations.annotation_targets == EntitiesWithMetadata.metadata_internal_id
            WHERE id = '{targetId}'
            """
            df_sql = read_sql(query, con)
            return df_sql

    def getEntitiesWithTitle(self, title: str):
        with connect(self.dbPathOrUrl) as con:
            query = f"""
            SELECT *, TRIM(Creators.creator_name) AS new_creator 
            FROM EntitiesWithMetadata
            LEFT JOIN  Creators ON EntitiesWithMetadata.creator == Creators.creator_internal_id WHERE title = '{title}'
            """
            df_sql = read_sql(query, con)
            return df_sql

    def getEntitiesWithCreator(self, creatorName: str):
        with connect(self.dbPathOrUrl) as con:
            query = f"""
            SELECT *,
            TRIM(Creators.creator_name) AS new_creator
            FROM EntitiesWithMetadata
            LEFT JOIN Creators ON EntitiesWithMetadata.creator == Creators.creator_internal_id 
            WHERE new_creator = '{creatorName}'
            """
            df_sql = read_sql(query, con)
            return df_sql

    def getImagesWithTarget(self, targetId: str):
        with connect(self.dbPathOrUrl) as con:
            query = f"""
            SELECT * FROM Images
            LEFT JOIN Annotations ON Images.images_internal_id == Annotations.annotation_bodies
            LEFT JOIN EntitiesWithMetadata ON Annotations.annotation_targets == EntitiesWithMetadata.metadata_internal_id
            WHERE EntitiesWithMetadata.id = '{targetId}'
            """
            df_sql = read_sql(query, con)
            return df_sql


# --- TRIPLESTORE QUERY PROCESSOR ---


class TriplestoreQueryProcessor(QueryProcessor):
    def __init__(self):
        super().__init__()

    def remove_chars(self, s: str) -> str:
        if '\"' in s:
            return s.replace('\"', '\\\"')
        elif '"' in s:
            return s.replace('"', '\\\"')
        else:
            return s

    def getAllCanvases(self) -> DataFrame:
        endpoint = self.getDbPathOrUrl()
        query = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX pomodoro: <https://github.com/datasci2023/datascience/class/>
        PREFIX feslegen: <https://github.com/datasci2023/datascience/attribute/>
        PREFIX spaghetti:  <https://github.com/datasci2023/datascience/relation/>

        SELECT ?id ?label
        WHERE {
            ?id rdf:type pomodoro:Canvas;
                rdfs:label ?label .
        }
        """
        df_sparql = get(endpoint, query, True)
        return df_sparql

    def getAllCollections(self) -> DataFrame:
        endpoint = self.getDbPathOrUrl()
        query = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX pomodoro: <https://github.com/datasci2023/datascience/class/>
        PREFIX feslegen: <https://github.com/datasci2023/datascience/attribute/>
        PREFIX spaghetti:  <https://github.com/datasci2023/datascience/relation/>
        
        SELECT ?id ?label ?items 
        WHERE {
            ?id rdf:type pomodoro:Collection;
                rdfs:label ?label ;
                spaghetti:items ?items .
                }
        """
        df_sparql = get(endpoint, query, True)
        return df_sparql

    def getAllManifests(self) -> DataFrame: 
        endpoint = self.getDbPathOrUrl()
        query = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX pomodoro: <https://github.com/datasci2023/datascience/class/>
        PREFIX feslegen: <https://github.com/datasci2023/datascience/attribute/>
        PREFIX spaghetti:  <https://github.com/datasci2023/datascience/relation/>
        
        SELECT ?id ?label ?items
        WHERE {
            ?id rdf:type pomodoro:Manifest;
                rdfs:label ?label ;
                spaghetti:items ?items .
        }
        """
        df_sparql = get(endpoint, query, True)
        return df_sparql

    def getCanvasesInCollection(self, collectionId: str) -> DataFrame:  
        endpoint = self.getDbPathOrUrl()
        query = (
            """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX pomodoro: <https://github.com/datasci2023/datascience/class/>
        PREFIX feslegen: <https://github.com/datasci2023/datascience/attribute/>
        PREFIX spaghetti:  <https://github.com/datasci2023/datascience/relation/>
        
        SELECT ?id ?label 
        WHERE {
            ?collection_id rdf:type pomodoro:Collection ; 
                spaghetti:items ?manifest_id .
            ?manifest_id rdf:type pomodoro:Manifest;
                spaghetti:items ?id .
            ?id rdf:type pomodoro:Canvas;
                rdfs:label ?label .    
            
            FILTER(?collection_id = <%s> )            
        } 
        """
            % collectionId
        )
        df_sparql = get(endpoint, query, True)
        return df_sparql

    def getCanvasesInManifest(self, manifestId: str) -> DataFrame:
        endpoint = self.getDbPathOrUrl()
        query = (
            """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX pomodoro: <https://github.com/datasci2023/datascience/class/>
        PREFIX feslegen: <https://github.com/datasci2023/datascience/attribute/>
        PREFIX spaghetti:  <https://github.com/datasci2023/datascience/relation/>
        
        SELECT ?id ?label 
        WHERE {
            ?manifest_id rdf:type pomodoro:Manifest;
                spaghetti:items ?id .
            ?id rdf:type pomodoro:Canvas;
                rdfs:label ?label .     
            FILTER ( ?manifest_id = <%s> ) 
           
        }
        """
            % manifestId
        )
        df_sparql = get(endpoint, query, True)
        return df_sparql

    def getEntitiesWithLabel(self, label: str) -> DataFrame:
        endpoint = self.getDbPathOrUrl()
        query = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX pomodoro: <https://github.com/datasci2023/datascience/class/>
        PREFIX feslegen: <https://github.com/datasci2023/datascience/attribute/>
        PREFIX spaghetti:  <https://github.com/datasci2023/datascience/relation/>
        
        SELECT ?id ?type ?label
        WHERE { 
            ?id rdfs:label ?label;
                rdf:type ?type .
            FILTER ( ?label = "%s" ) 
        }
        """ % self.remove_chars(
            label
        )

        df_sparql = get(endpoint, query, True)
        return df_sparql

    def getManifestsInCollection(self, collectionId) -> DataFrame:
        endpoint = self.getDbPathOrUrl()
        query = (
            """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX pomodoro: <https://github.com/datasci2023/datascience/class/>
        PREFIX feslegen: <https://github.com/datasci2023/datascience/attribute/>
        PREFIX spaghetti:  <https://github.com/datasci2023/datascience/relation/>
        
        SELECT ?id ?label ?items
        WHERE { 
            ?collection_id rdf:type pomodoro:Collection;
                spaghetti:items ?id . 
            ?id rdfs:label ?label ;
                spaghetti:items ?items .
            FILTER ( ?collection_id = <%s> ) 
                      
        }
        """
            % collectionId
        )

        df_sparql = get(endpoint, query, True)
        return df_sparql


# --- GENERIC QUERY PROCESSOR ---


class GenericQueryProcessor:
    def __init__(self):
        self.queryProcessors = []

    def cleanQueryProcessors(self) -> bool:
        try:
            self.queryProcessors = list()
            return True
        except Exception as e:
            print(e)
            return False

    def addQueryProcessor(self, processor: QueryProcessor) -> bool: 
        try:
            self.queryProcessors.append(processor)
            self.queryProcessors = sorted(
                [proc for proc in self.queryProcessors], key=lambda x: type(x).__name__
            )
            return True
        except Exception as e:
            print(e)
            return False

    def checkProcessors(self) -> bool:
        if len(self.queryProcessors) == 0:
            raise Exception(
                "Pippe al Sugo is missing two processors: RelationalQueryProcessor and TriplestoreQueryProcessor"
            )
        elif len(self.queryProcessors) == 1:
            if not isinstance(self.queryProcessors[0], RelationalQueryProcessor):
                raise Exception(
                    "Pippe al Sugo is missing one processor: RelationalQueryProcessor"
                )
            elif not isinstance(self.queryProcessors[0], TriplestoreQueryProcessor):
                raise Exception(
                    "Pippe al Sugo is missing one processor: TriplestoreQueryProcessor"
                )
        elif len(self.queryProcessors) > 1:
            if not isinstance(self.queryProcessors[0], RelationalQueryProcessor):
                raise Exception(
                    "Pippe al Sugo is missing one processor: RelationalQueryProcessor"
                )
            if not isinstance(self.queryProcessors[1], TriplestoreQueryProcessor):
                raise Exception(
                    "Pippe al Sugo is missing one processor: TriplestoreQueryProcessor"
                )

        return True

    def getAllAnnotations(self) -> list[Annotation]:
        result = list()
        df_rel = DataFrame()

        if isinstance(self.queryProcessors[0], RelationalQueryProcessor):
            df_rel = self.queryProcessors[0].getAllAnnotations()
        else:
            raise Exception(
                "Pippe al Sugo is missing one processor: RelationalQueryProcessor"
            )

        if not df_rel.empty:
            for idx, row in df_rel.iterrows():
                entity = Annotation(
                    row["annotation_ids"],
                    row["annotation_motivations"],
                    IdentifiableEntity(row["annotation_targets"]),
                    Image(row["annotation_bodies"]),
                )
                result.append(entity)

        return result

    def getAllCanvas(self) -> list[Canvas]:
        result = list()
        df_graph = DataFrame()
        df_rel = DataFrame()
        joined_df = DataFrame()

        if self.checkProcessors():
            df_graph = self.queryProcessors[1].getAllCanvases()

        if not df_graph.empty:
            for idx, row in df_graph.iterrows():
                df_rel = concat(
                    [df_rel, self.queryProcessors[0].getEntityById(row["id"])],
                    ignore_index=True,
                )

            df_rel = (
                df_rel.groupby(["id", "title"])["creator_name"]
                .apply(list)
                .reset_index()
            )
            joined_df = df_graph.merge(df_rel, how="left", on="id").fillna("")

            for idx, row in joined_df.iterrows():
                entity = Canvas(
                    row["id"], row["label"], row["title"], row["creator_name"]
                )
                result.append(entity)

        return result

    def getAllCollections(self) -> list[Collection]:
        result = list()
        df_graph = DataFrame()
        df_rel = DataFrame()
        joined_df = DataFrame()

        if self.checkProcessors():
            df_graph = self.queryProcessors[1].getAllCollections()

        if not df_graph.empty:
            for idx, row in df_graph.iterrows():
                df_rel = concat(
                    [df_rel, self.queryProcessors[0].getEntityById(row["id"])],
                    ignore_index=True,
                )

            df_graph = (
                df_graph.groupby(["id", "label"])["items"].apply(list).reset_index()
            )
            df_rel = (
                df_rel.groupby(["id", "title"])["creator_name"]
                .apply(list)
                .reset_index()
            )

            joined_df = df_graph.merge(df_rel, how="left", on="id").fillna("")

            for idx, row in joined_df.iterrows():
                entity = Collection(
                    row["id"],
                    row["label"],
                    row["title"],
                    row["creator_name"],
                    row["items"],
                )
                result.append(entity)

        return result

    def getAllImages(self) -> list[Image]: 
        result = []
        df_rel = DataFrame()

        if isinstance(self.queryProcessors[0], RelationalQueryProcessor):
            df_rel = self.queryProcessors[0].getAllImages()
        else:
            raise Exception(
                "Pippe al Sugo is missing one processor: RelationalQueryProcessor"
            )

        if not df_rel.empty:
            for idx, row in df_rel.iterrows():
                entity = Image(row["image_ids"])
                result.append(entity)

        return result

    def getAllManifests(self) -> list[Manifest]:
        result = list()
        df_graph = DataFrame()
        df_rel = DataFrame()
        joined_df = DataFrame()

        if self.checkProcessors():
            df_graph = self.queryProcessors[1].getAllManifests()

        if not df_graph.empty:
            for idx, row in df_graph.iterrows():
                df_rel = concat(
                    [df_rel, self.queryProcessors[0].getEntityById(row["id"])],
                    ignore_index=True,
                )

            df_graph = (
                df_graph.groupby(["id", "label"])["items"].apply(list).reset_index()
            )
            df_rel = (
                df_rel.groupby(["id", "title"])["creator_name"]
                .apply(list)
                .reset_index()
            )

            joined_df = df_graph.merge(df_rel, how="left", on="id").fillna("")

            for idx, row in joined_df.iterrows():
                entity = Manifest(
                    row["id"],
                    row["label"],
                    row["title"],
                    row["creator_name"],
                    row["items"],
                )
                result.append(entity)

        return result

    def getAnnotationsToCanvas(self, canvasId: str) -> list[Annotation]:
        result = list()
        df_rel = DataFrame()

        if isinstance(self.queryProcessors[0], RelationalQueryProcessor):
            df_rel = self.queryProcessors[0].getAnnotationsWithTarget(canvasId)
        else:
            raise Exception(
                "Pippe al Sugo is missing one processor: RelationalQueryProcessor"
            )

        if not df_rel.empty:
            for idx, row in df_rel.iterrows():
                entity = Annotation(
                    row["annotation_ids"],
                    row["annotation_motivations"],
                    IdentifiableEntity(row["annotation_targets"]),
                    Image(row["annotation_bodies"]),
                )
                result.append(entity)

        return result

    def getAnnotationsToCollection(
        self, collectionId: str
    ) -> list[Annotation]:
        result = list()
        df_rel = DataFrame()

        if isinstance(self.queryProcessors[0], RelationalQueryProcessor):
            df_rel = self.queryProcessors[0].getAnnotationsWithTarget(collectionId)
        else:
            raise Exception(
                "Pippe al Sugo is missing one processor: RelationalQueryProcessor"
            )

        if not df_rel.empty:
            for idx, row in df_rel.iterrows():
                entity = Annotation(
                    row["annotation_ids"],
                    row["annotation_motivations"],
                    IdentifiableEntity(row["annotation_targets"]),
                    Image(row["annotation_bodies"]),
                )
                result.append(entity)

        return result

    def getAnnotationsToManifest(self, manifestId: str) -> list[Annotation]:
        result = list()
        df_rel = DataFrame()

        if isinstance(self.queryProcessors[0], RelationalQueryProcessor):
            df_rel = self.queryProcessors[0].getAnnotationsWithTarget(manifestId)
        else:
            raise Exception(
                "Pippe al Sugo is missing one processor: RelationalQueryProcessor"
            )

        if not df_rel.empty:
            for idx, row in df_rel.iterrows():
                entity = Annotation(
                    row["annotation_ids"],
                    row["annotation_motivations"],
                    IdentifiableEntity(row["annotation_targets"]),
                    Image(row["annotation_bodies"]),
                )
                result.append(entity)

        return result

    def getAnnotationsWithBody(self, bodyId) -> list[Annotation]:
        result = []
        df_rel = DataFrame()

        if isinstance(self.queryProcessors[0], RelationalQueryProcessor):
            df_rel = self.queryProcessors[0].getAnnotationsWithBody(bodyId)
        else:
            raise Exception(
                "Pippe al Sugo is missing one processor: RelationalQueryProcessor"
            )

        if not df_rel.empty:
            for idx, row in df_rel.iterrows():
                entity = Annotation(
                    row["annotation_ids"],
                    row["annotation_motivations"],
                    IdentifiableEntity(row["annotation_targets"]),
                    Image(row["annotation_bodies"]),
                )
                result.append(entity)

        return result

    def getAnnotationsWithBodyAndTarget(
        self, bodyId: str, targetId: str
    ) -> list[Annotation]: 
        result = []
        df_rel = DataFrame()

        if isinstance(self.queryProcessors[0], RelationalQueryProcessor):
            df_rel = self.queryProcessors[0].getAnnotationsWithBodyAndTarget(
                bodyId, targetId
            )
        else:
            raise Exception(
                "Pippe al Sugo is missing one processor: RelationalQueryProcessor"
            )

        if not df_rel.empty:
            for idx, row in df_rel.iterrows():
                entity = Annotation(
                    row["annotation_ids"],
                    row["annotation_motivations"],
                    IdentifiableEntity(row["annotation_targets"]),
                    Image(row["annotation_bodies"]),
                )
                result.append(entity)

        return result

    def getAnnotationsWithTarget(self, targetId: str) -> list[Annotation]: 
        result = []
        df_rel = DataFrame()

        if isinstance(self.queryProcessors[0], RelationalQueryProcessor):
            df_rel = self.queryProcessors[0].getAnnotationsWithTarget(targetId)
        else:
            raise Exception(
                "Pippe al Sugo is missing one processor: RelationalQueryProcessor"
            )

        if not df_rel.empty:
            for idx, row in df_rel.iterrows():
                entity = Annotation(
                    row["annotation_ids"],
                    row["annotation_motivations"],
                    IdentifiableEntity(row["annotation_targets"]),
                    Image(row["annotation_bodies"]),
                )
                result.append(entity)

        return result

    def getCanvasesInCollection(self, collectionId: str) -> list[Canvas]: 
        result = list()
        df_graph = DataFrame()
        df_rel = DataFrame()
        joined_df = DataFrame()

        if self.checkProcessors():
            df_graph = (
                self.queryProcessors[1]
                .getCanvasesInCollection(collectionId)
                .drop_duplicates()
            )

        if not df_graph.empty:
            for idx, row in df_graph.iterrows():
                df_rel = concat(
                    [df_rel, self.queryProcessors[0].getEntityById(row["id"])],
                    ignore_index=True,
                )

            df_rel = (
                df_rel.groupby(["id", "title"])["creator_name"]
                .apply(list)
                .reset_index()
            )
            joined_df = df_graph.merge(df_rel, how="left", on="id").fillna("")

            for idx, row in joined_df.iterrows():
                entity = Canvas(
                    row["id"], row["label"], row["title"], row["creator_name"]
                )
                result.append(entity)

        return result

    def getCanvasesInManifest(self, manifestId: str) -> list[Canvas]: 
        result = list()
        df_graph = DataFrame()
        df_rel = DataFrame()
        joined_df = DataFrame()

        if self.checkProcessors():
            df_graph = (
                self.queryProcessors[1]
                .getCanvasesInManifest(manifestId)
                .drop_duplicates()
            )

        if not df_graph.empty:
            for idx, row in df_graph.iterrows():
                df_rel = concat(
                    [df_rel, self.queryProcessors[0].getEntityById(row["id"])],
                    ignore_index=True,
                )

            df_rel = (
                df_rel.groupby(["id", "title"])["creator_name"]
                .apply(list)
                .reset_index()
            )

            joined_df = df_graph.merge(df_rel, how="left", on="id").fillna("")

            for idx, row in joined_df.iterrows():
                entity = Canvas(
                    row["id"], row["label"], row["title"], row["creator_name"]
                )
                result.append(entity)

        return result

    def getEntityById(self, entityId: str) -> IdentifiableEntity or None: 
        joined_df = DataFrame()
        df_graph = DataFrame()
        df_rel = DataFrame()
        entity = None

        if self.checkProcessors():
            df_rel = self.queryProcessors[0].getEntityById(entityId)
            df_graph = self.queryProcessors[1].getEntityById(entityId)

        df_graph = (
            df_graph.groupby(["id", "label", "type"])["items"].apply(list).reset_index()
        )
        df_rel = (
            df_rel.groupby(["id", "title"])["creator_name"].apply(list).reset_index()
        )

        if not df_graph.empty:
            joined_df = df_graph.merge(df_rel, how="left", on="id").fillna("")
        else:
            joined_df = df_rel.merge(df_graph, how="left", on="id").fillna("")

        for idx, row in joined_df.iterrows():
            if row["type"] == "Collection":
                entity = Collection(
                    row["id"],
                    row["label"],
                    row["title"],
                    row["creator_name"],
                    row["items"],
                )
            elif row["type"] == "Manifest":
                entity = Manifest(
                    row["id"],
                    row["label"],
                    row["title"],
                    row["creator_name"],
                    row["items"],
                )
            elif row["type"] == "Canvas":
                entity = Canvas(
                    row["id"], row["label"], row["title"], row["creator_name"]
                )
            elif row["entityId_table"] == "Annotations":
                entity = Annotation(
                    row["annotation_ids"],
                    row["annotation_motivations"],
                    IdentifiableEntity(row["annotation_targets"]),
                    Image(row["annotation_bodies"]),
                )
            elif row["entityId_table"] == "Images":
                entity = Image(row["image_ids"])

        return entity

    # def getEntityById(
    #     self, entityId: str
    # ) -> IdentifiableEntity or None:
    #     df = DataFrame()

    #     for processor in self.queryProcessors:
    #         if isinstance(processor, TriplestoreQueryProcessor) or isinstance(
    #             processor, RelationalQueryProcessor
    #         ):
    #             df = concat([df, processor.getEntityById(entityId)], ignore_index=True)
    #             df.drop_duplicates()
    #             for idx, row in df.iterrows():
    #                 if row["id"]:
    #                     entity = IdentifiableEntity(row["id"])
    #                     return entity
    #                 else:
    #                     return None

    def getEntitiesWithCreator(
        self, creatorName: str
    ) -> list[EntityWithMetadata]: 
        result = list()
        df_rel = DataFrame()
        df_graph = DataFrame()
        joined_df = DataFrame()

        if self.checkProcessors():
            df_rel = self.queryProcessors[0].getEntitiesWithCreator(creatorName)
            df_rel.drop_duplicates()

        if not df_rel.empty:
            for idx, row in df_rel.iterrows():
                df_graph = concat(
                    [df_graph, self.queryProcessors[1].getEntityById(row["id"])],
                    ignore_index=True,
                )
                df_graph.drop_duplicates()

            df_rel = (
                df_rel.groupby(["id", "title"])["new_creator"].apply(list).reset_index()
            )

            df_graph = df_graph.groupby(["id", "label"]).apply(list).reset_index()

            joined_df = df_rel.merge(df_graph, how="left", on="id").fillna("")

            for idx, row in joined_df.iterrows():
                entity = EntityWithMetadata(
                    row["id"], row["label"], row["title"], row["new_creator"]
                )
                result.append(entity)

        return result

    def getEntitiesWithLabel(self, label: str) -> list[EntityWithMetadata]: 
        result = list()
        df_graph = DataFrame()
        df_rel = DataFrame()
        joined_df = DataFrame()

        if self.checkProcessors():
            df_graph = self.queryProcessors[1].getEntitiesWithLabel(label)

        if not df_graph.empty:
            for idx, row in df_graph.iterrows():
                df_rel = concat(
                    [df_rel, self.queryProcessors[0].getEntityById(row["id"])],
                    ignore_index=True,
                )

            df_rel = (
                df_rel.groupby(["id", "title"])["creator_name"]
                .apply(list)
                .reset_index()
            )

            df_graph = df_graph.groupby(["id", "label"]).apply(list).reset_index()

            joined_df = df_graph.merge(df_rel, how="left", on="id").fillna("")

            for idx, row in joined_df.iterrows():
                entity = EntityWithMetadata(
                    row["id"], row["label"], row["title"], row["creator_name"]
                )
                result.append(entity)

        return result

    def getEntitiesWithTitle(
        self, title: str
    ) -> list[EntityWithMetadata]:  # forza chiara!!! spacchi tutto
        result = list()
        df_graph = DataFrame()
        df_rel = DataFrame()
        joined_df = DataFrame()

        if self.checkProcessors():
            df_rel = (
                self.queryProcessors[0].getEntitiesWithTitle(title).drop_duplicates()
            )
            df_rel.drop_duplicates()

        if not df_rel.empty:
            for idx, row in df_rel.iterrows():
                df_graph = concat(
                    [df_graph, self.queryProcessors[1].getEntityById(row["id"])],
                    ignore_index=True,
                )

            df_rel = (
                df_rel.groupby(["id", "title"])["new_creator"].apply(list).reset_index()
            )

            df_graph = df_graph.groupby(["id", "label"]).apply(list).reset_index()

            joined_df = df_rel.merge(df_graph, how="left", on="id").fillna("")

            for idx, row in joined_df.iterrows():
                entity = EntityWithMetadata(
                    row["id"], row["label"], row["title"], row["new_creator"]
                )
                result.append(entity)

        return result

    def getImagesAnnotatingCanvas(self, canvasId) -> list[Image]:  # updated
        result = []
        df_rel = DataFrame()

        if isinstance(self.queryProcessors[0], RelationalQueryProcessor):
            df_rel = self.queryProcessors[0].getImagesWithTarget(canvasId)
        else:
            raise Exception(
                "Pippe al Sugo is missing one processor: RelationalQueryProcessor"
            )

        if not df_rel.empty:
            for idx, row in df_rel.iterrows():
                entity = Image(row["image_ids"])
                result.append(entity)

        return result

    def getManifestsInCollection(self, collectionId: str) -> list[Manifest]: 
        result = list()
        df_graph = DataFrame()
        df_rel = DataFrame()
        joined_df = DataFrame()

        if self.checkProcessors():
            df_graph = (
                self.queryProcessors[1]
                .getManifestsInCollection(collectionId)
                .drop_duplicates()
            )

        if not df_graph.empty:
            for idx, row in df_graph.iterrows():
                df_rel = concat(
                    [df_rel, self.queryProcessors[0].getEntityById(row["id"])],
                    ignore_index=True,
                ).drop_duplicates()

            df_graph = (
                df_graph.groupby(["id", "label"])["items"].apply(list).reset_index()
            )
            df_rel = (
                df_rel.groupby(["id", "title"])["creator_name"]
                .apply(list)
                .reset_index()
            )

            joined_df = df_graph.merge(df_rel, how="left", on="id").fillna("")

            for idx, row in joined_df.iterrows():
                entity = Manifest(
                    row["id"],
                    row["label"],
                    row["title"],
                    row["creator_name"],
                    row["items"],
                )
                result.append(entity)

        return result
