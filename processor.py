class Processor(object):
    def __init__(self) -> None:
        self.dbPathOrUrl = ""

    def getDbPathOrUrl(self) -> str:
        return self.dbPathOrUrl

    def setDbPathOrUrl(self, pathOrUrl: str):
        self.dbPathOrUrl = pathOrUrl

    # if the self.dbpath exists, then the new dbpath is the one given as a parameter and it is assigned as the new dbpath
