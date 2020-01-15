import datetime

class Auto_Base():
    def __init__(directory_dict=**kwargs,
                 identifier=datetime.datetime.today().strftime("%m.%d.%y")
                 ):
        if not isinstance(directory_dict, dict()):
            raise TypeError()
        self.directory_paths = directory_dict
        self.tag = indentifier
        self.general_source_type

    def _automation_internal(self, source_type=None)

    def static_dimension_tbls(self, source_type=None, update_condition):
        # source_type in ['raw', 'sql']
        # update_condition

    def dynamic_dimension_tbls(self, source_type=None):
        # source_type in ['raw', 'sql']

    def generate_fact_tbls(self, source_type=None):
