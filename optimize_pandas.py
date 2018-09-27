"""
These functions make it easy to find optimize 
dataFrames in pandas and find the righ parameters when 
using chunks to work with larger files
"""
import pandas as pd
import numpy as np

def optimize(df):
	"""
	Returns an optimized version of the dataFrame
	Downcasts numeric columns into the lowest 
	numeric value possible.

	Changes object columns into the 'category' dtype 
	where applicable
	"""
	orig_mem_usage = df.memory_usage(deep=True)/1024**2

	df_len = len(df)
	float_cols = df.select_dtypes(include='float').columns
	int_cols = df.select_dtypes(include='integer').columns
	object_cols = df.select_dtypes(include='object').columns

	for col in float_cols:
		df[col] = pd.to_numeric(df[col], downcast='float')

	for col in int_cols:
		df[col] = pd.to_numeric(df[col], downcast='integer')

	for col in object_cols:
		if df[col].nunique()/df_len < 0.5:
			df[col] = df[col].astype('category')

	new_mem_usage = df.memory_usage(deep=True)/1024**2
	saved_memory = orig_mem_usage - new_mem_usage

	print("AMOUNT OF MEMORY USED ORIGINALLY (MB): ", orig_mem_usage.sum(), '\n')
	print("AMOUNT OF MEMORY USED NOW (MB): ", new_mem_usage.sum(), '\n')
	print(saved_memory)

	return df

def get_chunk_size(mb_limit, filename, increment=100):
	"""
	Finds the optiman number of rows for chunks in
	a pandas dataFrame.

	mb_limit: Amount of memory available
	file_name: csv file
	"""
	nrows = 0
	mb_chunk = 0

	while mb_chunk < mb_limit:
		nrows += increment
		df = pd.read_csv(filename, nrows=nrows)
		mb_chunk = loans_2007.memory_usage(deep=True).sum()/1024**2

	return nrows - increment

