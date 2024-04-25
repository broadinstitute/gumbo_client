import datetime
import json
import re

import pandas as pd

from dataframe_json_packing import pack, unpack


def test_df_to_dict():
    df = pd.DataFrame(
        {
            "string": ["a", "b", None],
            "int": [1, 2, None],
            "float": [1.1, 2.2, None],
            "d": [datetime.date(2000, 1, 1), datetime.date(2001, 2, 2), None],
            "dt": [
                pd.to_datetime(datetime.datetime(2000, 1, 1, hour=1)),
                pd.to_datetime(datetime.datetime(2001, 2, 2, hour=2)),
                None,
            ],
            "dt_tz": [
                pd.to_datetime(datetime.datetime(2000, 1, 1, hour=1), utc=True),
                pd.to_datetime(datetime.datetime(2001, 2, 2, hour=2), utc=True),
                None,
            ],
            "json": [
                "[1, 2, 3]",
                '{"s": "foo", "a": [-5.5], "o": {"x": "y"}}',
                "{}"
            ]
        }
    ).convert_dtypes()

    df['json'] = df['json'].apply(json.loads)

    packed = pack(df)

    # make sure missing values are represented with "null" and not NaN
    assert re.search(r"NaN|NaT", json.dumps(packed)) is None

    assert packed == {
        "columns": [
            {"name": "string", "type": "string", "values": ["a", "b", None]},
            {"name": "int", "type": "int", "values": [1, 2, None]},
            {"name": "float", "type": "float", "values": [1.1, 2.2, None]},
            {"name": "d", "type": "date", "values": [730120, 730518, None]},
            {
                "name": "dt",
                "type": "datetime64",
                "values": ["2000-01-01 01:00:00", "2001-02-02 02:00:00", None],
            },
            {
                "name": "dt_tz",
                "type": "datetime64",
                "values": [
                    "2000-01-01 01:00:00+00:00",
                    "2001-02-02 02:00:00+00:00",
                    None,
                ],
            },
            {
                "name": "json",
                "type": "json",
                "values": [
                    "[1, 2, 3]",
                    '{"s": "foo", "a": [-5.5], "o": {"x": "y"}}',
                    "{}"
                ]
            }
        ]
    }

    unpacked_df = unpack(packed)

    assert df.equals(unpacked_df)
