from typing import Callable

ERROR: int = -1
OK: int = 0
SUCCESS: int = 1


class Entry:
    identifier: str
    handler: Callable[[], int]

    def __init__(self, identifier: str, handler: Callable[[], int]):
        self.identifier = identifier
        self.handler = handler


class Menu:
    selection: Entry
    options: list[Entry]

    def __init__(self, options: list[Entry]):
        self.load(options)

    def __init__(self):
        self.load([])

    def load(self, options: list[Entry]):
        self.options = options

    def render(self):
        for i, option in enumerate(self.options):
            print(str(i) + ": " + option.identifier)

    def select(self):
        index: int

        try:
            index = int(input())
            self.selection = self.options[index]
            return OK

        except Exception:
            return ERROR

    def execute(self):
        return self.selection.handler()
