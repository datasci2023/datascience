from data_model import *
from processor import Processor

from sqlite3 import connect
from pandas import read_csv, Series, DataFrame, merge, read_sql
import pandas as pd


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
