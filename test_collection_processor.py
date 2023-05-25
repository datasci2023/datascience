from collectionProcessor import CollectionProcessor

grp_endpoint = "http://192.168.1.19:9999/blazegraph/sparql"
col_dp = CollectionProcessor()
col_dp.setDbPathOrUrl(grp_endpoint)
col_dp.uploadData("collection-1.json")
col_dp.uploadData("collection-2.json")
