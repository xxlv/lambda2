
# API
class ContentLessApi():
    def __init__(self, api):
        self.api = api

    def send_to(self, server, func_id, func_body):
        return self.api.send(server, func_id, func_body)
