from pandas import DataFrame, merge, concat
from processor import Processor
from RelationalQueryProcessor import RelationalQueryProcessor
from TriplestoreQueryProcessor import TriplestoreQueryProcessor
from queryProcessor import QueryProcessor
from data_model import *


class GenericQueryProcessorDeneme:
    def __init__(self):
        self.queryProcessors = []

    def addQueryProcessor(self, processor: QueryProcessor):  # workssss
        self.queryProcessors.append(processor)
        return True

    def getCol(self) -> list[Collection]:  # braveeeee!!!
        result = list()
        df_graph = DataFrame()
        df_rel = DataFrame()
        joined_df = DataFrame()

        for processor in self.queryProcessors:
            if isinstance(processor, TriplestoreQueryProcessor):
                df_graph = processor.getAllCollections()

        for processor in self.queryProcessors:
            if isinstance(processor, RelationalQueryProcessor):
                for idx, row in df_graph.iterrows():
                    df_rel = concat(
                        [df_rel, processor.getEntityById(row["id"])], ignore_index=True
                    )

        df_graph = df_graph.groupby(["id", "label"])["items"].apply(list).reset_index()
        df_rel = (
            df_rel.groupby(["id", "title"])["creator_name"].apply(list).reset_index()
        )

        joined_df = df_graph.merge(df_rel, how="left", on="id").fillna("")

        # joined_df = (
        #     joined_df.groupby(["id", "label", "title", "creator_name"])["items"]
        #     .apply(list)
        #     .reset_index()
        # )

        # joined_df = joined_df.groupby(["id", "label", "title", "creator_name"])
        # items = joined_df["items"].apply(list)
        # joined_df = items.reset_index()
        # joined_df = joined_df.groupby(["id", "label", "title", "items"])
        # creator_name = joined_df["creator_name"].apply(list)
        # joined_df = creator_name.reset_index()

        # grouped = joined_df.groupby("id")["creator_name"].apply(list).reset_index()

        for idx, row in joined_df.iterrows():
            print(
                row["id"], row["label"], row["title"], row["creator_name"], row["items"]
            )
            entity = Collection(
                row["id"], row["label"], row["title"], row["creator_name"], row["items"]
            )
            # print(entity.__dict__)
            result.append(entity)

        return result
