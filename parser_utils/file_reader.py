import glob
import os
import pandas as pd
from os import path
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.types import NVARCHAR
import time
import sys
import csv 
def remove_unnecessary_rows_and_cols(df):

	# Supposing that the last column is df's last column
	# Drop all starting rows where last column is nan
	
	last_column=len(df.columns)-1
	second_last_column=last_column-1 if (last_column-1)>=0 else 0
	third_last_column=last_column-2 if (last_column-2)>=0 else 0

	rows_drop=[]	
	for x in range(len(df.index)):

		if (pd.isnull(df.iloc[x,last_column]) or pd.isnull(df.iloc[x,second_last_column]) or pd.isnull(df.iloc[x,third_last_column])):
			# When a non nan col occurs we suppose it to be header 
			# And drop all rows before it that were nan's
			# df.drop(df.index[rows_drop],inplace=True)
			rows_drop.append(x)
		else:
			break
#	rows_drop.append(x)

	df.drop(df.index[rows_drop],inplace=True)
	print("Dropped Rows {}".format(rows_drop))

	print("Current Column Names {}".format(df.columns))
	
	# After drop reset the index's
	df=df.reset_index(drop=True)
	
	if len(df)>0:
		# First row of the modified df is the header which will be the new col names
		df.columns=list(df.loc[0].fillna('Unnamed'))
		df.columns = df.columns.astype(str)
		print("Replacing Column Names {}".format(df.columns))
		# Drop the first row
		df.drop(0,inplace=True)
		
		# Drop all columns that are unnamed
		df.drop(columns=[x for x in df.columns if 'Unnamed' in x],inplace=True)
		print("New Columns Names after Dropping Unnamed Column{}".format(df.columns))

	
	return df


def read_csv_files(p,file_mapping):
	print('File mapping',file_mapping,'\n')
	# Get file_name from path
	#file_name=os.path.splitext(p)[0].split("/")[-1]
	file_name=os.path.basename(os.path.splitext(p)[0])
	
	#file_name=''.join([c for c in file_name if c.isalpha()])
	print("--------->>Reading CSV File: ",file_name)

	# Remove numeric digits
	file_name=''.join([i for i in file_name if not i.isdigit()]).lower()
	print("Formatted CSV Filename: ",file_name)
	df_dict={}
	# Read csv file, first row is header (row 0)

	#df=pd.read_csv(p,dtype=object,header=None,encoding="latin1")
	df=pd.read_csv(p,encoding='latin1',header=None,low_memory=False,quoting=csv.QUOTE_NONE, error_bad_lines=False,dtype=object)
	df=remove_unnecessary_rows_and_cols(df)
	
	print("Dataframe Read:\n(Rows: {}\n Columns: {} \n Types: {}\n Dict Key: {}\n)".format(len(df),df.columns,df.dtypes,file_name))
	# Changed 3
	#	if len(df)==0:
	#		print("df ignored, no records")
	#		return df_dict

	for f_m in file_mapping:
		if 	f_m in file_name:
			file_name=file_mapping[f_m]["table_name"]
			break

	print("New Dict Key",file_name)
	df_dict[file_name]=df
	return df_dict

def read_excel_files(p,file_mapping):
	# Get file_name from path
	#file_name=os.path.splitext(p)[0].split("/")[-1]
	file_name=os.path.basename(os.path.splitext(p)[0])

	
	#file_name=''.join([c for c in file_name if c.isalpha()])
	print("--------->>Reading Excel File: ",file_name)

	# Remove numeric digits
	file_name=''.join([i for i in file_name if not i.isdigit()]).lower()
	print("Formatted Excel Filename: ",file_name)

	# Read xl file
	xls = pd.ExcelFile(p)

	df_dict={}
	

	if len(xls.sheet_names)==1 or (len(xls.sheet_names)==2 and xls.sheet_names[1]=='XDO_METADATA'):
		print("File has 1 sheet")
		# Read dataframe 
		df=pd.read_excel(xls,dtype=object,header=None)
		print("Dataframe Read:\n(Rows: {}\n Columns: {} \n Types: {}\n)".format(len(df),df.columns,df.dtypes))
		# Remove unnecessary rows and cols
		df=remove_unnecessary_rows_and_cols(df)
		print("New DF after removing_unnecessary_rows_and_cols():\n( {}\n)".format(df))

		print("Dict Key:",file_name)
		# Changed 3
		#	if len(df)==0:
		#		print("df ignored, no records")
		#		return df_dict
		for f_m in file_mapping:
			if 	f_m in file_name:
				file_name=file_mapping[f_m]["table_name"]
				break
		print("New Dict Key",file_name)

		df_dict[file_name]=df

	elif len(xls.sheet_names)>1:
		print("File has {} sheets".format(len(xls.sheet_names)))
		# For all sheets in xls file

		for x in xls.sheet_names:
			
			print("Reading sheet {}".format(x))
			# Read dataframe
			df=pd.read_excel(xls,sheet_name=x,dtype=object,header=None)	
			print("Dataframe Read:\n(Rows: {}\n Columns: {} \n Types: {}\n)".format(len(df),df.columns,df.dtypes))
			# Remove unnecessary rows and cols
			df=remove_unnecessary_rows_and_cols(df)
			print("New DF after removing_unnecessary_rows_and_cols():\n( {}\n)".format(df))

			print("Dict Key:",x)
			# Changed 3
			# if len(df)==0:
			# 	print("df ignored, no records")
			# 	continue
			lower_case_sheet_name=x.lower()
			for f_m in file_mapping:
				if 	f_m in lower_case_sheet_name:
					lower_case_sheet_name=file_mapping[f_m]["table_name"]
					break
			print("New Dict Key",file_name)

			df_dict[lower_case_sheet_name]=df.drop_duplicates()

	return df_dict







"""select "ll" name,date(ll.date_time) as dt,count(*) c from login_logs ll group by date(ll.date_time)
UNION
select "cdr" name,date(cl.date_time) as dt,count(*) c from cdr_logs cl  group by date(cl.date_time)
UNION
select "ba" name,date(bl.date) as dt,count(*) c from business_action_logs bl  group by date(bl.date)
UNION
select "si" name,date(sl.date_time) as dt,count(*) c from subscrib_integrate_logs sl group by date(sl.date_time)
UNION
select "pl" name,date(pl.date_time) as dt,count(*) c from payment_logs pl group by date(pl.date_time);
"""