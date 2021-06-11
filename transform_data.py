from datetime import datetime, timedelta
from os import walk
import pandas as pd
data_input_path = "D:/DS/Academi/Week_3/Johan/data_input"
data_output_path = "D:/DS/Academi/Week_3/Johan/data_output"

f=[]
for (dirpath, dirnames, filenames) in walk(data_input_path):
    f.extend(filenames)
    break
for i in f:
    df = pd.read_json(f'{data_input_path}/{i}', lines=True)
    filename = (i.split('.')[0])
    filename = datetime.strptime(filename, "%Y-%m-%d")
    filename_edit = filename + timedelta(days=724)
    flightdate = filename + timedelta(days=723)
    filename_edit = filename_edit.strftime("%Y-%m-%d")
    flightdate = flightdate.strftime("%Y-%m-%d")
    df['flight_date'] = flightdate
    df.to_csv(f'{data_output_path}/{filename_edit}.csv', index=False)
