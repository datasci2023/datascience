from pandas import DataFrame, merge, concat
from processor import Processor
from RelationalQueryProcessor import RelationalQueryProcessor
from TriplestoreQueryProcessor import TriplestoreQueryProcessor
from queryProcessor import QueryProcessor
from data_model import *


class GenericQueryProcessor:
    def __init__(self):
        self.queryProcessor = [QueryProcessor]

    def cleanQueryProcessor(self):
        self.queryProcessor = []
        return True

    def addQueryProcessor(self, processor: QueryProcessor):
        self.queryProcessor.append(processor)
        return True

    def getAllAnnotations(self):
        result = list()
        df_rel = DataFrame()

        for processor in self.queryProcessors:
            if isinstance(processor, RelationalQueryProcessor):
                df_rel = processor.getAllAnnotations()

        for idx, row in df_rel.iterrows():
            entity = Annotation(
                row["annotation_ids"],
                row["annotation_motivations"],
                IdentifiableEntity(row["annotation_targets"]),
                Image(row["annotation_bodies"]),
            )  # .fillna("")
            result.append(entity)

        return result

    def getAllCanvas(self):
        result = list()
        df_graph = DataFrame()
        df_rel = DataFrame()
        joined_df = DataFrame()

        for processor in self.queryProcessor:
            if isinstance(processor, TriplestoreQueryProcessor):
                df_graph = processor.getAllCanvases()

        for processor in self.queryProcessor:
            if isinstance(processor, RelationalQueryProcessor):
                for idx, row in df_graph.iterrows():
                    df_rel.append(processor.getEntitybyId(row[id]))
                    # joined_df.append(df_rel)

        joined_df = (
            merge(df_rel, df_graph, left_on="id", right_on="id")
            .fillna("")
            .drop_duplicates()
        )

        for idx, row in joined_df.iterrows():
            entity = Canvas(
                row["id"], row["label"], row["title"], row["creator"]
            ).fillna("")
            result.append(entity)

        return result

    def getAllCollections(self) -> list(Collection):
        result = list()
        df_graph = DataFrame()
        df_rel = DataFrame()
        joined_df = DataFrame()

        for processor in self.queryProcessor:
            if isinstance(processor, TriplestoreQueryProcessor):
                df_graph = processor.getAllCollections()

        for processor in self.queryProcessor:
            if isinstance(processor, RelationalQueryProcessor):
                for idx, row in df_graph.iterrows():
                    df_rel.append(processor.getEntityById(row[id]))

        joined_df = merge(df_rel, df_graph, left_on="id", right_on="id").fillna("")

        for idx, row in joined_df.iterrows():
            entity = Collection(
                row["id"], row["label"], row["title"]
            )  # row["items"], row["creator"] TO ADD TO SINGLE CELL

        result.append(entity)
        return result

    def getAllImages(self):
        result = []
        df_rel = DataFrame()

        for processor in self.queryProcessor:
            if isinstance(processor, RelationalQueryProcessor):
                df_rel = processor.getAllImages()

        for idx, row in df_rel.iterrows():
            entity = Image(row["image_ids"]).fillna("")
            result.append(entity)

        return result

    def getAllManifests(self) -> list(Manifest):
        result = list()
        df_graph = DataFrame()
        df_rel = DataFrame()
        joined_df = DataFrame()

        for processor in self.queryProcessor:
            if isinstance(processor, TriplestoreQueryProcessor):
                df_graph = processor.getAllManifests()

        for processor in self.queryProcessor:
            if isinstance(processor, RelationalQueryProcessor):
                for idx, row in df_graph.iterrows():
                    df_rel.append(processor.getEntityById(row[id]))

        joined_df = merge(df_rel, df_graph, left_on="id", right_on="id").fillna("")

        for idx, row in joined_df.iterrows():
            entity = Manifest(
                row["id"], row["label"], row["title"]
            )  # row["creator"], row["items"]) TO ADD TO SINGLE CELL

        result.append(entity)
        return result

    def getAnnotationsToCanvas(self, canvasId: str):
        result = list()
        df_rel = DataFrame()

        for processor in self.queryProcessors:
            if isinstance(processor, RelationalQueryProcessor):
                df_rel = processor.getAnnotationsWithTarget(canvasId)

        for idx, row in df_rel.iterrows():
            entity = Annotation(
                row["annotation_ids"],
                row["annotation_motivations"],
                IdentifiableEntity(row["annotation_targets"]),
                Image(row["annotation_bodies"]),
            )  # .fillna("")
            result.append(entity)

        return result

    def getAnnotationsToCollection(self, collectionId: str):
        result = list()
        df_rel = DataFrame()

        for processor in self.queryProcessors:
            if isinstance(processor, RelationalQueryProcessor):
                df_rel = processor.getAnnotationsWithTarget(collectionId)

        for idx, row in df_rel.iterrows():
            entity = Annotation(
                row["annotation_ids"],
                row["annotation_motivations"],
                IdentifiableEntity(row["annotation_targets"]),
                Image(row["annotation_bodies"]),
            )  # .fillna("")
            result.append(entity)

        return result

    def getAnnotationsToManifest(self, manifestId: str):
        result = list()
        df_rel = DataFrame()

        for processor in self.queryProcessors:
            if isinstance(processor, RelationalQueryProcessor):
                df_rel = processor.getAnnotationsWithTarget(manifestId)

        for idx, row in df_rel.iterrows():
            entity = Annotation(
                row["annotation_ids"],
                row["annotation_motivations"],
                IdentifiableEntity(row["annotation_targets"]),
                Image(row["annotation_bodies"]),
            )  # .fillna("")
            result.append(entity)

        return result

    def getAnnotationsWithBody(self, bodyId):
        result = []
        df_rel = DataFrame()

        for processor in self.queryProcessor:
            if isinstance(processor, RelationalQueryProcessor):
                df_rel = processor.getAnnotationsWithBody(bodyId)

        for idx, row in df_rel.iterrows():
            entity = Annotation(
                row["annotation_ids"],
                row["annotation_motivations"],
                IdentifiableEntity(row["annotation_targets"]),
                Image(row["annotation_bodies"]),
            ).fillna("")
            result.append(entity)

        return result

    def getAnnotationsWithBodyAndTarget(self, bodyId: str, targetId: str):
        result = []
        df_rel = DataFrame()

        for processor in self.queryProcessor:
            if isinstance(processor, RelationalQueryProcessor):
                df_rel = processor.getAnnotationsWithBodyAndTarget(bodyId, targetId)

        for idx, row in df_rel.iterrows():
            entity = Annotation(
                row["annotation_ids"],
                row["annotation_motivations"],
                IdentifiableEntity(row["annotation_targets"]),
                Image(row["annotation_bodies"]),
            ).fillna("")
            result.append(entity)

        return result

    def getAnnotationsWithTarget(self, targetId: str):
        result = []
        df_rel = DataFrame()

        for processor in self.queryProcessor:
            if isinstance(processor, RelationalQueryProcessor):
                df_rel = processor.getAnnotationsWithTarget(targetId)

        for idx, row in df_rel.iterrows():
            entity = Annotation(
                row["annotation_ids"],
                row["annotation_motivations"],
                IdentifiableEntity(row["annotation_targets"]),
                Image(row["annotation_bodies"]),
            ).fillna("")
            result.append(entity)

        return result

    def getCanvasesInCollection(self, collectionId: str):
        result = list()
        df_graph = DataFrame()
        df_rel = DataFrame()
        joined_df = DataFrame()

        for processor in self.queryProcessor:
            if isinstance(processor, TriplestoreQueryProcessor):
                df_graph = processor.getCanvasesInCollection(collectionId)

        for processor in self.queryProcessor:
            if isinstance(processor, RelationalQueryProcessor):
                for idx, row in df_graph.iterrows():
                    df_rel.append(processor.getEntitybyId(row[id]))
                    # joined_df.append(df_rel)

        joined_df = (
            merge(df_rel, df_graph, left_on="id", right_on="id")
            .fillna("")
            .drop_duplicates()
        )

        for idx, row in joined_df.iterrows():
            entity = Canvas(
                row["id"], row["label"], row["title"], row["creator"]
            ).fillna("")
            result.append(entity)

        return result

    def getCanvasesInManifest(self, manifestId: str):
        result = list()
        df_graph = DataFrame()
        df_rel = DataFrame()
        joined_df = DataFrame()

        for processor in self.queryProcessor:
            if isinstance(processor, TriplestoreQueryProcessor):
                df_graph = processor.getCanvasesInCollections(manifestId)

        for processor in self.queryProcessor:
            if isinstance(processor, RelationalQueryProcessor):
                for idx, row in df_graph.iterrows():
                    df_rel.append(processor.getEntitybyId(row[id]))
                    # joined_df.append(df_rel)

        joined_df = (
            merge(df_rel, df_graph, left_on="id", right_on="id")
            .fillna("")
            .drop_duplicates()
        )

        for idx, row in joined_df.iterrows():
            entity = Canvas(
                row["id"], row["label"], row["title"], row["creator"]
            ).fillna("")
            result.append(entity)

        return result

    def getEntityById(self, entityId: str) -> IdentifiableEntity or None:
        result = list()
        df_graph = DataFrame()
        df_rel = DataFrame()

        for processor in self.queryProcessor:
            if isinstance(processor, TriplestoreQueryProcessor):
                df_graph = processor.getEntityById(entityId)
                for idx, row in df_graph.iterrows():
                    entity = IdentifiableEntity(row["id"])
                    result.append(entity)
                return result
            elif isinstance(processor, RelationalQueryProcessor):
                df_rel = processor.getEntityById(entityId)
                for idx, row in df_rel.iterrows():
                    entity = IdentifiableEntity(row["id"])
                    result.append(entity)
                return result
            else:
                return None

    def EntitiesWithCreator(self, creatorName: str) -> list(EntityWithMetadata):
        result = list()
        df_rel = DataFrame()
        df_graph = DataFrame()
        joined_df = DataFrame()

        for processor in self.queryProcessor:
            if isinstance(processor, RelationalQueryProcessor):
                df_rel = processor.getEntitiesWithCreator(creatorName)

        for processor in self.queryProcessor:
            if isinstance(processor, TriplestoreQueryProcessor):
                for idx, row in df_graph.iterrows():
                    df_graph.append(processor.getEntitybyId(row[id]))

        joined_df = merge(df_rel, df_graph, left_on="id", right_on="id").fillna("")

        for idx, row in joined_df.iterrows():
            entity = EntityWithMetadata(
                row["id"], row["label"], row["title"], row["creator"]
            )

        result.append(entity)
        return result

    def getEntitiesWithLabel(self, label: str):
        result = list()
        df_graph = DataFrame()
        df_rel = DataFrame()
        joined_df = DataFrame()

        for processor in self.queryProcessor:
            if isinstance(processor, TriplestoreQueryProcessor):
                df_graph = processor.getEntitiesWithLabel(label)

        for processor in self.queryProcessor:
            if isinstance(processor, RelationalQueryProcessor):
                for idx, row in df_graph.iterrows():
                    df_rel.append(processor.getEntitybyId(row[id]))
                    # joined_df.append(df_rel)

        joined_df = (
            merge(df_rel, df_graph, left_on="id", right_on="id")
            .fillna("")
            .drop_duplicates()
        )

        for idx, row in joined_df.iterrows():
            entity = EntityWithMetadata(
                row["id"], label, row["title"], row["creator"]
            ).fillna("")
            result.append(entity)

        return result

    def getEntitiesWithTitle(self, title):
        result = list()
        df_graph = DataFrame()
        df_rel = DataFrame()
        joined_df = DataFrame()

        for processor in self.queryProcessor:
            if isinstance(processor, RelationalQueryProcessor):
                df_graph = processor.getEntitiesWithTitle(title)

        for processor in self.queryProcessor:
            if isinstance(processor, TriplestoreQueryProcessor):
                for idx, row in df_graph.iterrows():
                    df_rel.append(processor.getEntitybyId(row[id]))
                    # joined_df.append(df_rel)

        joined_df = (
            merge(df_rel, df_graph, left_on="id", right_on="id")
            .fillna("")
            .drop_duplicates()
        )

        for idx, row in joined_df.iterrows():
            entity = EntityWithMetadata(
                row["id"], row["label"], title, row["creator"]
            ).fillna("")
            result.append(entity)

    def getImagesAnnotationgCanvas(self, canvasId):
        result = []
        df_rel = DataFrame()

        for processor in self.queryProcessor:
            if isinstance(processor, RelationalQueryProcessor):
                df_rel = processor.getImagesWithTarget(canvasId)

        for idx, row in df_rel.iterrows():
            entity = Image(row["image_ids"]).fillna("")
            result.append(entity)

        return result

    def getManifestsInCollection(self, collectionId: str) -> list(Manifest):
        result = list()
        df_graph = DataFrame()
        df_rel = DataFrame()
        joined_df = DataFrame()

        for processor in self.queryProcessor:
            if isinstance(processor, TriplestoreQueryProcessor):
                df_graph = processor.getManifestsInCollection(collectionId)

        for processor in self.queryProcessor:
            if isinstance(processor, RelationalQueryProcessor):
                for idx, row in df_graph.iterrows():
                    df_rel.append(processor.getEntitybyId(row[id]))

        joined_df = merge(df_rel, df_graph, left_on="id", right_on="id").fillna("")

        for idx, row in joined_df.iterrows():
            entity = Manifest(
                row["id"], row["label"], row["title"], row["creator"]
            )  # row["items"] CREATE ONLY ONE CELL

        result.append(entity)
        return result
