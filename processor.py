import os


class Processor(object):
    def __init__(self) -> None:
        self.dbPathOrUrl = ""

    def getDbPathOrUrl(self) -> str:
        return self.dbPathOrUrl

    def setDbPathOrUrl(self, pathOrUrl: str) -> bool:
        if os.path.isfile(pathOrUrl) or pathOrUrl.endswith(".db"):
            self.dbPathOrUrl = pathOrUrl
            return True
        elif pathOrUrl.startswith("https:") or pathOrUrl.startswith("http:"):
            self.dbPathOrUrl = pathOrUrl
            return True
        return False
