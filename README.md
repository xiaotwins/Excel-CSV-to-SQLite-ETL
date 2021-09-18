# Excel-CSV-to-SQLite

## Introduction
Writes all .csv and .xlsx files in the specified directory all with transformation of datetime columns to format (yyyy-mm-dd hh:mm:ss)/(yyyy-mm-dd),conversion of float columns to int,replace values in columns into sqlite.db file

## Functionality
1. Read all csv and xlsx files in the specified folder recursively.
2. For each files
   -read data in pandas dataframe ( for xlsx: consider each sheet has 1 data table, will read data from all sheets)  
   -identify the first row with 3 non null values as columns/header and remove all rows above that
   -Replace whitespaces,tabs,newlines from the all column names with a single space and then replace single space with _
   -If entry exists in transformation.json (key in transformation file is found in file name) then :
    -Perform datetime format conversion on columns specified
    -Perform date format conversion on columns specified
    -Perform float to int conversion on columns specified
    -Replace in specific value with another one in specified column
   -Load into sqlfile


## Libraries
- python > 2.7
- openpyxl 
- pandas
- sqlalchemy
- psutil

## How to Use
- Run using:  
```python main.py </path/to/data/> </path/to/transformations> </path/to/output/dir>```  
```python main.py ./data ./transformation.json .```
  
