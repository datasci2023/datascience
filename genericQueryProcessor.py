from pandas import DataFrame, merge, concat
from processor import Processor
from RelationalQueryProcessor import RelationalQueryProcessor
from TriplestoreQueryProcessor import TriplestoreQueryProcessor
from queryProcessor import QueryProcessor
from data_model import *


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

    def addQueryProcessor(self, processor: QueryProcessor) -> bool:  # workssss
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

    def getAllAnnotations(self) -> list[Annotation]:  # updated
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

    def getAllCanvas(self) -> list[Canvas]:  # updated
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
            df_rel.groupby(["id", "title"])["creator_name"].apply(list).reset_index()
        )
        joined_df = df_graph.merge(df_rel, how="left", on="id").fillna("")

        for idx, row in joined_df.iterrows():
            entity = Canvas(row["id"], row["label"], row["title"], row["creator_name"])
            result.append(entity)

        return result

    def getAllCollections(self) -> list[Collection]:  # updated
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

        df_graph = df_graph.groupby(["id", "label"])["items"].apply(list).reset_index()
        df_rel = (
            df_rel.groupby(["id", "title"])["creator_name"].apply(list).reset_index()
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

    def getAllImages(self) -> list[Image]:  # updated
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

    def getAllManifests(self) -> list[Manifest]:  # updated
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

        df_graph = df_graph.groupby(["id", "label"])["items"].apply(list).reset_index()
        df_rel = (
            df_rel.groupby(["id", "title"])["creator_name"].apply(list).reset_index()
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

    def getAnnotationsToCanvas(self, canvasId: str) -> list[Annotation]:  # updated
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
    ) -> list[Annotation]:  # updated
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

    def getAnnotationsToManifest(self, manifestId: str) -> list[Annotation]:  # updated
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

    def getAnnotationsWithBody(self, bodyId) -> list[Annotation]:  # updated
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
    ) -> list[Annotation]:  # updated
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

    def getAnnotationsWithTarget(self, targetId: str) -> list[Annotation]:  # updated
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

    def getCanvasesInCollection(self, collectionId: str) -> list[Canvas]:  # updated
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

    def getCanvasesInManifest(self, manifestId: str) -> list[Canvas]:  # updated
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

    def getEntityById(self, entityId: str) -> IdentifiableEntity or None:  # updated
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
        print(entity.__dict__)

        return entity

    # def getEntityById(
    #     self, entityId: str
    # ) -> IdentifiableEntity or None:  # workkss oleee
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
    ) -> list[EntityWithMetadata]:  # updated - bugs to fix - creators lists
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

        joined_df = df_graph.merge(df_rel, how="left", on="id").fillna("")

        for idx, row in joined_df.iterrows():
            entity = EntityWithMetadata(
                row["id"], row["label"], row["title"], row["new_creator"]
            )
            result.append(entity)

            print(entity.__dict__)

        return result

    def getEntitiesWithLabel(self, label: str) -> list[EntityWithMetadata]:  # updated
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
    ) -> list[EntityWithMetadata]:  # updated - forza chiara!!! spacchi tutto
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
            # print(entity.__dict__)

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

    def getManifestsInCollection(self, collectionId: str) -> list[Manifest]:  # updated
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
