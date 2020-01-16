import datetime
import os

class Auto_Base():
    # Accept a dict() if data_source = SQL
    # {}
    def __init__(base_dir=None,
                 identifier=datetime.datetime.today().strftime("%m.%d.%y")
                 ):
        if not isinstance(directory_dict, dict()):
            raise TypeError()
        for file_key, file_path
        self.branch_dict = dict()
        self.directory_base = base_dir
        self.directory_paths = {"Base":self.directory_base}
        self.tag = indentifier
        self.general_source_type



    def _automation_internal(self, source_type=None)

    def static_dimension_tbls(self, source_type=None, update_condition):
        # source_type in ['raw', 'sql']
        # update_condition

    def dynamic_dimension_tbls(self, source_type=None):
        # source_type in ['raw', 'sql']

    def initialize_branch(branch_name, path_extension=None, file_extension=None, table_name=None):
        # Call to add new platform or path
        if path_extension is not None:
            # New branch is a flat file file
            os.path.join(self.directory_base, path_extension)

    def generate_fact_tbls(self, source_type=None):
