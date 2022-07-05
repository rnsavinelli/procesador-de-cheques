class Runtime:
    status: int

    RUNNING: int = 1
    STOPPED: int = -1

    def __init__(self):
        self.stop()

    def start(self):
        self.status = self.RUNNING

    def stop(self):
        self.status = self.STOPPED
