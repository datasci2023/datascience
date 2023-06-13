from collectionProcessor import CollectionProcessor
from TriplestoreQueryProcessor import TriplestoreQueryProcessor

grp_endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
col_dp = CollectionProcessor()
col_dp.setDbPathOrUrl(grp_endpoint)
col_dp.uploadData("collection-1.json")
col_dp.uploadData("collection-2.json")

grp_qp = TriplestoreQueryProcessor()
grp_qp.setDbPathOrUrl(grp_endpoint)

print(grp_qp.getDbPathOrUrl())

# print(grp_qp.getManifestsInCollection("https://dl.ficlit.unibo.it/iiif/28429/collection"))
# print(grp_qp.getAllCollections())
# print(grp_qp.getEntitiesWithLabel("Il Canzoniere"))
