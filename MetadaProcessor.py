from sqlite3 import connect
from pandas import read_csv, Series, DataFrame
import pandas as pd
import re

class MetadataProcessor(object): #the object has to be updated to Processor when we'll put everything together
    def __init__(self, dbPathOrUrl):
        super().__init__(dbPathOrUrl)
    
    def uploadData(self, path):

        with connect(self.dbPathOrUrl) as con: # Insert in the parenthesis the name of the file containing the database; the first time, if the database corresponding to that name/path doesn't exist, a database with that name is created; it should be a database separate from the triplestore for now

            metadata = read_csv("E:\\Documents\\GitHub\\datascience\\metadata.csv", 
                            keep_default_na=False,
                            dtype={
                                "id": "string",
                                "title": "string",
                                "creator": "string"
                            })

            # COLLECTIONS TABLE
            collections = pd.DataFrame() # empty DataFrame object
            collections_id = []
            collections_title = []
            for column_name, column in metadata.items():
                for value in column:
                    if "collection" in value:
                        collections_id.append(value) # Iterate over the values of the columns and I append in a list only the ones with the word "collection" in it, which are the IDs of the collections 
                        # (to be improved: find a way to iterate only over column 'id')
                        title = metadata.loc[metadata["id"] == value, "title"].values[0] # Select the rows where the value of the column "id" is equal to my variable "value" (i.e. in this case the id of the collection), then I select the value in the same row but in the column "title" (i.e. the title corresponding to the collection I found)
                        collections_title.append(title)

            collections.insert(0, "id", Series(collections_id, dtype="string")) # Append the items in my list in a column of the DataFrame "collections"
            collections.insert(1, "title", Series(collections_title, dtype="string"))

            collections_internal_id = []
            for idx, row in collections.iterrows():
                collections_internal_id.append("collection-" + str(idx)) # Create a list of internal ids

            collections.insert(0, "collectionId", Series(collections_internal_id, dtype="string")) # Append the internal IDs in a column of the Data Frame "collections"

            # MANIFESTS TABLE
            manifests = pd.DataFrame() # empty DataFrame object
            manifests_id = []
            manifests_title = []
            for column_name, column in metadata.items():
                for value in column:
                    if "manifest" in value:
                        manifests_id.append(value)  
                        title = metadata.loc[metadata["id"] == value, "title"].values[0]
                        manifests_title.append(title)

            manifests.insert(0, "id", Series(manifests_id, dtype="string")) # Append the items in my list in a column of the DataFrame "manifests"
            manifests.insert(1, "title", Series(manifests_title, dtype="string"))

            manifests_internal_id = []
            for idx, row in manifests.iterrows():
                manifests_internal_id.append("manifest-" + str(idx)) # Create a list of internal ids

            manifests.insert(0, "manifestId", Series(manifests_internal_id, dtype="string")) # Append the internal IDs in a column of the Data Frame "manifests"

            # CANVAS TABLE
            canvases = pd.DataFrame() 
            canvases_id = []
            canvases_title = []
            for column_name, column in metadata.items():
                for value in column:
                    if "canvas" in value:
                        canvases_id.append(value)
                        title = metadata.loc[metadata["id"] == value, "title"].values[0] 
                        canvases_title.append(title)

            canvases.insert(0, "id", Series(canvases_id, dtype="string")) 
            canvases.insert(1, "title", Series(canvases_title, dtype="string"))

            canvases_internal_id = []
            for idx, row in canvases.iterrows():
                canvases_internal_id.append("canvas-" + str(idx))

            canvases.insert(0, "canvasesId", Series(canvases_internal_id, dtype="string"))

            # 'ITEMS': RELATION COLLECTION-MANIFEST

            collection_manifest = pd.DataFrame()
            col_id = []
            man_id = [] 
            for id1 in collections_id:
                for id2 in manifests_id:
                    pattern = r"/(\d{2,})/"  # Pattern that looks for a number (two or more digits) inside two slashes "/"
                    match1 = re.search(pattern, id1)
                    match2 = re.search(pattern, id2)

                    if match1 and match2:  # Verify whether the specified pattern is in the two URLs
                        number1 = match1.group(1)  # Number found in the first id
                        number2 = match2.group(1)  # Number found in the second id

                        if number1 == number2:  # Verify whether the two numbers are equal
                            col_id.append(id1)
                            man_id.append(id2)
                        else:
                            col_id.append(None)
                            man_id.append(id2)

            collection_manifest.insert(0, "collection_id", Series(col_id, dtype="string")) 
            collection_manifest.insert(1, "manifest_id", Series(man_id, dtype="string"))


            # 'ITEMS': RELATION MANIFEST-CANVAS

            manifest_canvas = pd.DataFrame()
            man_id_2 = []
            can_id = [] 
            for id1 in manifests_id:
                for id2 in canvases_id:
                    pattern = r"/(\d{2,})/"  # Pattern that looks for a number (two or more digits) inside two slashes "/"
                    match1 = re.search(pattern, id1)
                    match2 = re.search(pattern, id2)

                    if match1 and match2:  # Verify whether the specified pattern is in the two URLs
                        number1 = match1.group(1)  # Number found in the first id
                        number2 = match2.group(1)  # Number found in the second id

                        if number1 == number2:  # Verify whether the two numbers are equal
                            man_id_2.append(id1)
                            can_id.append(id2)
                        else:
                            man_id_2.append(None)
                            can_id.append(id2)

            manifest_canvas.insert(0, "manifest_id", Series(man_id_2, dtype="string")) 
            manifest_canvas.insert(1, "canvas_id", Series(can_id, dtype="string"))



            collections.to_sql("Collections", con, if_exists="replace", index=False)
            manifests.to_sql("Manifests", con, if_exists="replace", index=False)
            canvases.to_sql("Canvases", con, if_exists="replace", index=False)
            collection_manifest.to_sql("Collection-Manifest", con, if_exists="replace", index=False)
            manifest_canvas.to_sql("Manifest-Canvas", con, if_exists="replace", index=False)

            con.commit()
            con.close()




