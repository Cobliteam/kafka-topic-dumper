
class Identity(object):

    def transform(self, msg):
        yield msg

    def get_id(self):
        return "Identity"
