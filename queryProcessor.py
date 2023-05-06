from pandas import DataFrame
from processor import Processor

class queryProcessor(Processor):
    def __init__(self, entityId):
        self.entityId = entityId
        super(Processor).__init__()
        self.dbPathOrUrl = None

    def getEntityById(self, entityId:str) -> DataFrame:
        if entityId:
            return self.entityId