import importlib.resources
import json

class NameMappingUtils:
    def __init__(self):
        with importlib.resources.open_text("gumbo_client", "name_mapping.json") as file:
            self.name_mapping = json.load(file) # ex. { "table_name": {"snake_case_col_name" -> "CustomColNAME"}}

    def rename_columns(self, table_name, df, convert_to_custom_names=True, inplace=False):
        column_name_mapping = self.name_mapping[table_name]
        # invert the mapping if moving from custom names to snake case
        if not convert_to_custom_names:
            column_name_mapping = {v: k for k, v in column_name_mapping.items()}

        return df.rename(columns=column_name_mapping, inplace=inplace)
