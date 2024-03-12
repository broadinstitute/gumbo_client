from gumbo_rest_client import utils
import pandas as pd


def test_name_mapping():
    mapping_utils = utils.NameMappingUtils()
    assert mapping_utils.name_mapping is not None

    # override the mapping (for test simplicity)
    test_name_mapping = {"test_table": {"a": "A", "b": "B", "f": "F"}}
    mapping_utils.name_mapping = test_name_mapping

    test_df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": [4, 5, 6]})

    # convert to custom names (uppercase letters) where the mapping exists
    test_df = mapping_utils.rename_columns("test_table", test_df)
    assert test_df.columns.tolist() == ["A", "B", "c"]

    # convert back to snake case (lowercase letters) where the mapping exists
    mapping_utils.rename_columns(
        "test_table", test_df, convert_to_custom_names=False, inplace=True
    )
    assert test_df.columns.tolist() == ["a", "b", "c"]
