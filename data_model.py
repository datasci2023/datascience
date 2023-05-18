class IdentifiableEntity:
    def __init__(self, id):
        self.id = id

    def getId(self) -> str:
        return self.id


class Image(IdentifiableEntity):
    pass


class Annotation(IdentifiableEntity):
    def __init__(self, id, motivation: str, target, body):
        self.motivation = motivation
        self.target = target
        self.body = body

        super().__init__(id)

    def getBody(self) -> Image:
        return self.body

    def getMotivation(self) -> str:
        return self.motivation

    def getTarget(self) -> IdentifiableEntity:
        return self.target


class EntityWithMetadata(IdentifiableEntity):
    def __init__(self, id, label: str, title: str, creators: str):
        self.label = label
        self.title = title
        self.creators = set()
        for creator in creators:
            self.creators.add(creator)

        super().__init__(id)

    def getLabel(self) -> str:
        return self.label

    def getTitle(self) -> str:
        return self.title

    def getCreators(self) -> list[str]:
        result = list()
        for creator in self.creators:
            result.append(creator)
        result.sort()
        return result


class Canvas(EntityWithMetadata):
    def __init__(self, id):
        super().__init__(id)


class Manifest(EntityWithMetadata):
    def __init__(self, id, items):
        self.items = set()
        for item in items:
            self.items.add(item)

        super().__init__(id)

    def getItems(self) -> list[Canvas]:
        result = list()
        for item in self.items:
            result.append(item)
        result.sort()
        return result


class Collection(EntityWithMetadata):
    def __init__(self, id, items):
        self.items = set()
        for item in items:
            self.items.add(item)

        super().__init__(id)

    def getItems(self) -> list[Manifest]:
        result = list()
        for item in self.items:
            result.append(item)
        result.sort()
        return result