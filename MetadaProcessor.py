from sqlite3 import connect
from pandas import read_csv, Series, DataFrame
import pandas as pd

metadata = read_csv(
    "C:\\Users\\chiar\\Documents\\Magistrale\\DataScience\\metadata.csv",
    keep_default_na=False,
    dtype={"id": "string", "title": "string", "creator": "string"},
)

# 'ITEMS': RELATION COLLECTION-MANIFEST
collection_manifest = pd.DataFrame()

# 'ITEMS': RELATION MANIFEST-CANVAS
manifest_canvas = pd.DataFrame()

# COLLECTIONS TABLE
collections = pd.DataFrame()  # empty DataFrame object
collections_id = []
collections_title = []
for column_name, column in metadata.items():
    for value in column:
        if "collection" in value:
            collections_id.append(
                value
            )  # Iterate over the values of the columns and I append in a list only the ones with the word "collection" in it, which are the IDs of the collections
            # (to be improved: find a way to iterate only over column 'id')
            title = metadata.loc[metadata["id"] == value, "title"].values[
                0
            ]  # Select the rows where the value of the column "id" is equal to my variable "value" (i.e. in this case the id of the collection), then I select the value in the same row but in the column "title" (i.e. the title corresponding to the collection I found)
            collections_title.append(title)

collections.insert(
    0, "id", Series(collections_id, dtype="string")
)  # Append the items in my list in a column of the DataFrame "collections"
collections.insert(1, "title", Series(collections_title, dtype="string"))

collections_internal_id = []
for idx, row in collections.iterrows():
    collections_internal_id.append(
        "collection-" + str(idx)
    )  # Create a list of internal ids

collections.insert(
    0, "manifestId", Series(collections_internal_id, dtype="string")
)  # Append the internal IDs in a column of the Data Frame "collections"

# MANIFESTS TABLE
manifests = pd.DataFrame()  # empty DataFrame object
manifests_id = []
manifests_title = []
for column_name, column in metadata.items():
    for value in column:
        if "manifest" in value:
            manifests_id.append(value)
            title = metadata.loc[metadata["id"] == value, "title"].values[0]
            manifests_title.append(title)

manifests.insert(
    0, "id", Series(manifests_id, dtype="string")
)  # Append the items in my list in a column of the DataFrame "manifests"
manifests.insert(1, "title", Series(manifests_title, dtype="string"))

manifests_internal_id = []
for idx, row in manifests.iterrows():
    manifests_internal_id.append(
        "manifest-" + str(idx)
    )  # Create a list of internal ids

manifests.insert(
    0, "manifestId", Series(manifests_internal_id, dtype="string")
)  # Append the internal IDs in a column of the Data Frame "manifests"

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


# print(collections)
# print(manifests)
# print(canvases)

# with connect("relationaldatabase.db") as con:


#     con.commit()


# class MetadataProcessor(object): #the superclass should be Processor
#     def super().__init__(dbPathOrUrl)
#     def uploadData(self, path):
