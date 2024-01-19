from dataframe_json_packing import pack, unpack
import pandas as pd
import datetime
import json


def test_df_to_dict():
    df = pd.DataFrame(
        {
            "string": ["a", "b", None],
            "int": [1, 2, None],
            "float": [1.1, 2.2, None],
            "d": [datetime.date(2000, 1, 1), datetime.date(2001, 2, 2), None],
        }
    ).convert_dtypes()

    packed = pack(df)
    # make sure missing values are represented with "null" and not NaN
    "NaN" not in json.dumps(packed)
    assert packed == {
        "columns": [
            {"name": "string", "type": "string", "values": ["a", "b", None]},
            {"name": "int", "type": "int", "values": [1, 2, None]},
            {"name": "float", "type": "float", "values": [1.1, 2.2, None]},
            {"name": "d", "type": "date", "values": [730120, 730518, None]},
        ]
    }

    unpacked_df = unpack(packed)

    assert df.equals(unpacked_df)
