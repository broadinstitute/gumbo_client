import pandas as pd
import datetime

# created an adhoc serialization/deserialization 
# format because panda's existing to_json does not 
# retain data types

def _date_column_from_ordinal(values):
    date_values = []
    for value in values:
        if value is not None:
            value = datetime.date.fromordinal(value)
        date_values.append(value)
    return pd.Series(date_values, dtype="object")

series_constructor_by_name = {'string': lambda values: pd.Series(data=values, dtype="string"), 
 'int':lambda values: pd.Series(data=values, dtype="Int64"), 
 'float':lambda values: pd.Series(data=values, dtype="Float64"), 
 'date': _date_column_from_ordinal}


def _ordinals_from_date_column(values):
    oridnal_values = []
    for value in values:
        if value is not None:
            value = value.toordinal()
        oridnal_values.append(value)
    return oridnal_values

def _all_are_dates(values):
    for value in values:
        if not (isinstance(value, datetime.date) or (value is None)):
            return False
    return True

def _replace_na_with_none(values, coerce):
    # replace NA with None. I suspect there must be 
    # a better way then making this loop, but not sure what
    result = []
    for value in values:
        if pd.isna(value):
            result.append(None)
        else:
            result.append(coerce(value))
    return result


def pack(df):
    df = df.convert_dtypes()
    columns = []
    for column_name, type in df.dtypes.items():
        if type == "Int64":
            coerce = int
        elif type == "Float64":
            coerce = float
        else:
            coerce = lambda x: x
        values = _replace_na_with_none(df[column_name], coerce)

        # special handling of "object" series because these
        # could be anything, but really they should only be 
        # used for dates
        if type.name == "object":
            if _all_are_dates(values):
                type_name = "date"
                values = _ordinals_from_date_column(values)
            else:
                raise Exception(f"Column {column_name} was unhandled type: {values}...")
        elif type.name == "Int64":
            type_name = "int"
        elif type.name == "Float64":
            type_name = "float"
        else:
            type_name = type.name

        columns.append({"name": column_name, "type": type_name, "values": values})
    result = {"columns": columns}
    return result


def unpack(d):
    columns_dict = {}
    for column in d['columns']:
        column_name = column['name']
        type_name = column['type']
        if type_name not in series_constructor_by_name:
            raise Exception(f"Column {column_name} was unknown type: {type_name}")
        constructor = series_constructor_by_name[type_name]
        values = column['values']
        columns_dict[column_name] = constructor(values)
    return pd.DataFrame(columns_dict)
