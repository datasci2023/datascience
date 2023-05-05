class Processor(object):
    def __init__(self) -> None:
        self.dbPathOrUrl = None

    def getDbPathOrUrl(self) -> str:
        return self.dbPathOrUrl

    def setDbPathOrUrl(self, dbPathOrUrl:str):
        if self.dbPathOrUrl:
            dbPathOrUrl = self.dbPathOrUrl
            return dbPathOrUrl
        
    #if the self.dbpath exists, then the new dbpath is the one given as a parameter and it is assigned as the new dbpath