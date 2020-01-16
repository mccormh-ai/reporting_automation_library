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
        for file_key, file_path:
            print(file_key)
        self.branch_dict = dict()
        self.directory_base = base_dir
        self.directory_paths = {"Base":self.directory_base}
        self.dimension_tbls = dict()
        self.tag = indentifier
        self.general_source_type



    def _automation_internal(self, source_type=None)

    def static_dimension_tbls(self, source_type=None, update_condition):
        # source_type in ['raw', 'sql']
        # update_condition

    def dynamic_dimension_tbls(self, source_type=None):
        # source_type in ['raw', 'sql']

    def initialize_dimension_tbl(self, name, data_source="flat file",
                                path_extension=None, all_in_folder=False, file_extension=None, 
                                table_name=None
                                return_type="DataFrame"):
        dimension_payload = dict()
        dimension_payload['Name'] = name
        if

        if return_type is "DataFrame":
            dimension_tbls.append(dimension_payload)

    def initialize_branch(self, branch_name, path_extension=None, file_extension=None, table_name=None):
        # Call to add new platform or path
        if path_extension is not None:
            # New branch is a flat file file
            os.path.join(self.directory_base, path_extension)

    def generate_fact_tbls(self, source_type=None):
