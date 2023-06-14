from pandas import DataFrame
from processor import Processor
from queryProcessor import QueryProcessor
import pandas as pd
from sqlite3 import connect
from pandas import read_sql


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
            query = f"SELECT * FROM EntitiesWithMetadata LEFT JOIN  Creators ON EntitiesWithMetadata.creator == Creators.creator_internal_id WHERE title = '{title}'"
            df_sql = read_sql(query, con)
            return df_sql

    def getEntitiesWithCreator(self, creatorName: str):
        with connect(self.dbPathOrUrl) as con:
            query = f"""
            SELECT * FROM EntitiesWithMetadata
            JOIN Creators ON EntitiesWithMetadata.creator == Creators.creator_internal_id 
            WHERE Creators.creator_name = '{creatorName}'
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
