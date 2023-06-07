from pandas import DataFrame
from processorTemp import Processor
from queryProcessor1 import QueryProcessor
import pandas as pd
from sqlite3 import connect
from pandas import read_sql

class RelationalQueryProcessor(QueryProcessor):
    def __init__(self):
        pass
        # I eliminated entityId as parameter because I don't think it is but check
    
    def getAllAnnotations (self):
        with connect("relationaldatabase.db") as con:
            query = "SELECT * FROM Annotations"
            df_sql = read_sql(query, con)
            return df_sql
    
    def getAllImages (self):
        with connect("relationaldatabase.db") as con:
            query = "SELECT * FROM Images"
            df_sql = read_sql(query, con)
            return df_sql
    
    def getAnnotationsWithBody(self, bodyId: str):
        with connect("relationaldatabase.db") as con:
            query = """
            SELECT annotation_ids FROM Annotations
            JOIN Images ON Annotations.annotation_bodies == Images.images_internal_id
            WHERE image_ids = ?
            """
            cursor = con.cursor()
            cursor.execute(query, (bodyId,))
            df_sql = read_sql(query, con, params=(bodyId,))
            return df_sql
    
    def getAnnotationsWithBodyAndTarget(self, bodyId: str, targetId: str):
        with connect("relationaldatabase.db") as con:
            query = """
            SELECT annotation_ids FROM Annotations
            JOIN Images ON Annotations.annotation_bodies == Images.images_internal_id 
            JOIN EntitiesWithMetadata ON Annotations.annotation_targets == EntitiesWithMetadata.metadata_internal_id
            WHERE image_ids = ? AND id = ?
            """
            cursor = con.cursor()
            cursor.execute(query, (bodyId, targetId))
            df_sql = read_sql(query, con, params=(bodyId, targetId))
            return df_sql

    def getAnnotationsWithTarget(self, targetId: str):
        with connect("relationaldatabase.db") as con:
            query = """
            SELECT annotation_ids FROM Annotations
            JOIN EntitiesWithMetadata ON Annotations.annotation_targets == EntitiesWithMetadata.metadata_internal_id
            WHERE id = ?
            """
            cursor = con.cursor()
            cursor.execute(query, (targetId,))
            df_sql = read_sql(query, con, params=(targetId,))
            return df_sql    
    
    def getEntitiesWithTitle(self, title: str):
        with connect("relationaldatabase.db") as con:
            query = "SELECT * FROM EntitiesWithMetadata WHERE title = ?"
            cursor = con.cursor()
            cursor.execute(query, (title,))
            df_sql = read_sql(query, con, params=(title,))
            return df_sql
    
    def getEntitiesWithCreator(self, creatorName: str):
        with connect("relationaldatabase.db") as con:
            query = """
            SELECT * FROM EntitiesWithMetadata
            JOIN Creators ON EntitiesWithMetadata.creator == Creators.creator_internal_id 
            WHERE Creators.creator = ?
            """
            cursor = con.cursor()
            cursor.execute(query, (creatorName,))
            df_sql = read_sql(query, con, params=(creatorName,))
            return df_sql
        


# print(RelationalQueryProcessor.getEntitiesWithCreator(self=RelationalQueryProcessor, creatorName='Raimondi, Giuseppe'))
# print(RelationalQueryProcessor.getAnnotationsWithBodyAndTarget(self=RelationalQueryProcessor, bodyId="https://dl.ficlit.unibo.it/iiif/2/45500/full/699,800/0/default.jpg", targetId="https://dl.ficlit.unibo.it/iiif/2/28429/canvas/p3"))
# print(RelationalQueryProcessor.getAnnotationsWithBody(self=RelationalQueryProcessor, bodyId="https://dl.ficlit.unibo.it/iiif/2/45500/full/699,800/0/default.jpg"))
# All queries tested, it should work
