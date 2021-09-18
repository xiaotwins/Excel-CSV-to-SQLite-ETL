import glob
import os
import pandas as pd
from os import path
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.types import NVARCHAR
import time
import sys
import subprocess
import json
from .file_reader import *
import gc
import psutil
from datetime import datetime

class Parser:
	extensions={'.csv':read_csv_files,'.xls':read_excel_files,'.xlsx':read_excel_files}
	character_to_remove=["$",".","*","#"]
	
	def __init__(self,path,trans,out):

		self.search_directory=path
		self.transformations=trans
		self.output_file=out
		#self.file_mapping=fm

		self.files=[]
		self.dataframes={}

		self.engine = create_engine('sqlite:///{}/parser_db.db'.format(out), echo=False)
		self.sqlite_connection = self.engine.connect()
		

	def clean_columns(self,df_dict):
		
		#For each dataframe in dict
		for i in df_dict.keys():
			print("--------->>Cleaning Df",i)
			
			#Get columns list
			columns=df_dict[i].columns
			
			print("Current Columns names: ",columns)

			# Replace whitespaces,tabs,newlines from the str with a single space and replace it with _
			columns=[" ".join(col.split()).lower().replace(" ","_") for col in columns]
			
			# For each `remove_symbol` in symbols to remove 
			for replace_symbol in self.character_to_remove:
				
				# For each col in columns remove the `remove_symbol`
				columns=[col.replace(replace_symbol,"") for col in columns]
			
			# Assign new cols to the dataframe accessed through df_dict[i] where i is the key.
			print(len(columns),"\n",columns)
			counts_column={}
			idx=0
			while idx<len(columns):
				try:
					counts_column[columns[idx]]=counts_column[columns[idx]]+1
					columns[idx]=columns[idx]+str(counts_column[columns[idx]])
				except:
					counts_column[columns[idx]]=1
				idx+=1

			print("\nRemoved Dups",columns)
			df_dict[i].columns=columns
			
			print("\nNew Columns names: ",columns)

		return df_dict

	def find_files(self):

		for ext in self.extensions.keys():
			for x in os.walk(self.search_directory):
				for y in glob.glob(os.path.join(x[0], '*'+ext)):
					if path.isfile(y):
						self.files.append(y)
		return len(self.files)

	def convert_datetime_columns(self):
		for df_key in self.dataframes.keys():
			transformationidx=''
			for i in self.transformations:
				if self.transformations[i]['name']==df_key:
					transformationidx=i
			if transformationidx=='':
				continue
			try:
				if len(self.transformations[transformationidx]['datetime'])>0:
					print("Datetime Transformations",self.transformations[transformationidx]['datetime']," on df",transformationidx)
					date_time_columns=self.transformations[transformationidx]['datetime']
					for column in date_time_columns:
						try:
							self.dataframes[df_key][column]=self.dataframes[df_key][column].apply(pd.to_datetime,errors='ignore')
						except:
							print("\tCannot Convert Col {}\n\tE".format(column),sys.exc_info())
			except:
				print("\tDatetime Entry in transformation.json not found")
			try:
				if len(self.transformations[transformationidx]['date'])>0:
					print("Date Transformations",self.transformations[transformationidx]['date']," on df",transformationidx)
					date_time_columns=self.transformations[transformationidx]['date']
					for column in date_time_columns:
						try:
							self.dataframes[df_key][column]=self.dataframes[df_key][column].apply(pd.to_datetime,errors='ignore').dt.date
						except:
							print("\tCannot Convert Col {}\n\tE".format(column),sys.exc_info())
			except:
				print("\tDate Entry in transformation.json not found")
			try:
				if len(self.transformations[transformationidx]['float'])>0:
					print("Float Transformations",self.transformations[transformationidx]['float']," on df",transformationidx)
					float_columns=self.transformations[transformationidx]['float']
					for column in float_columns:
						try:
							self.dataframes[df_key][column]=self.dataframes[df_key][column].astype('float').apply(np.ceil).astype("int").astype("str")
						except:
							print("\tCannot Convert Col {}\n\tE".format(column),sys.exc_info())
			except:
				print("\tFloat Entry in transformation.json not found")
			try:
				if len(self.transformations[transformationidx]['replace'])>0:
					print("Replace Transformations",self.transformations[transformationidx]['replace']," on df",transformationidx)
					replace_column_array=self.transformations[transformationidx]['replace']
					for col_dict in replace_column_array:
						for column in col_dict:
							print("\tTransformating col{}".format(column))
							if len(col_dict[column])==2:
								try:
									print("\tReplace {} by {}".format(col_dict[column][0],col_dict[column][1]))
									self.dataframes[df_key][column]=self.dataframes[df_key][column].str.replace(col_dict[column][0],col_dict[column][1])
									print("Done")
								except:
									print("\tCannot Convert Col {}\n\tE".format(column),sys.exc_info())							
							else:
								print("\tIllegal Values Didnt Replace for ",column," ",col_dict[column])
								
			except:
				print("\tReplace Entry in transformation.json not found")
			

	def load_into_to_sql(self):
		
		# For each df read and transformed for the currently read file
		for i in self.dataframes.keys():
			
			# Write df to sqlite table with dict key as table name
			self.dataframes[i].to_sql(i, self.sqlite_connection, if_exists='append',index=False,dtype={col_name: NVARCHAR for col_name in self.dataframes[i]})
			print("\nAdded to table",i," len",len(self.dataframes[i]))
			gc.collect()
		print("\nAdded to table,Ended\n")

	
	# Changed 2
	def truncate_tables(self,df_dict):
		for key in df_dict:
			print("Truncating table {}".format(key))
			try:
				self.sqlite_connection.execution_options(autocommit=True).execute("DELETE FROM {};".format(key))
			except:
				print("Cannot truncate table {}".format(sys.exc_info()))

		dict_keys=list(df_dict.keys())

		for key in dict_keys:
			if len(df_dict[key])==0:
				print("Dropping, Df {} contains 0 items".format(key))
				df_dict.pop(key,None)
		
		return df_dict


	def process_files(self):
		
		for x in self.files:
			print("!"*100)
			print("\nFile_Path",x,"\n")
			extension=os.path.splitext(x)
			print("\nAvailable Memory %")
			print(psutil.virtual_memory().available * 100 / psutil.virtual_memory().total)
			print("\nUsed Memory %")
			print(psutil.virtual_memory().percent)
			print("-----------STARTING READING DF(s) FROM FILE-----------")
			
			dataframe_dict=self.extensions[extension[-1]](x,self.transformations)
			
			print("-----------TRUNCATING PREVIOUS TABLES AND DROPPING 0 LENGTH DFS-----------")
			
			#dataframe_dict=self.truncate_tables(dataframe_dict)
			
			if len(dataframe_dict)>0:
				
				print("-----------CLEANING COLUMNS-----------")
				self.dataframes=self.clean_columns(dataframe_dict)
				print("-----------CONVERTING STRING TO DATETIME AND FLOAT TO INT COLUMNS-----------")
				self.convert_datetime_columns()
				print("-----------LOADING DATA INTO SQL-----------")
				self.load_into_to_sql()

				#os.remove(x)
			else:
				print("deleting file at",x)
				os.remove(x)
				print("###No df in file found###")
