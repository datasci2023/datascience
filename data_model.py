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
        self.creators = creators
        # self.creators = list()

        # for creator in creators:
        #     self.creators.append(creator)

        super().__init__(id)

    def getLabel(self) -> str:
        return self.label

    def getTitle(self) -> str:
        return self.title

    def getCreators(self) -> list[str]:
        return self.creators


class Canvas(EntityWithMetadata):
    def __init__(self, id, label: str, title: str, creators: list[str]):
        self.label = label
        self.title = title
        self.creators = list()

        for creator in creators:
            self.creators.append(creator)

        super().__init__(id, label, title, creators)


class Manifest(EntityWithMetadata):
    def __init__(
        self, id, label: str, title: str, creators: list[str], items: list[Canvas]
    ):
        self.label = label
        self.title = title
        self.creators = list()
        self.items = list()

        for creator in creators:
            self.creators.append(creator)

        for item in items:
            self.items.append(item)

        super().__init__(id, label, title, creators)

    def getItems(self) -> list[Canvas]:
        result = list()
        for item in self.items:
            result.append(item)

        return result


class Collection(EntityWithMetadata):
    def __init__(
        self, id, label: str, title: str, creators: list[str], items: list[Manifest]
    ):
        self.label = label
        self.title = title
        self.creators = list()
        self.items = list()

        for creator in creators:
            self.creators.append(creator.lstrip(" "))

        for item in items:
            self.items.append(item)

        super().__init__(id, label, title, creators)

    def getItems(self) -> list[Manifest]:
        return self.items
