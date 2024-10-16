class Transformer:
    def __init__(self, content: bytes):
        self.content = content

    def transform(self) -> list[dict]:
        raise Exception("unimplemented; extend class to write a transformer")
