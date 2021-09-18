
import time
import sys
from parser_utils.parser import *
# python main.py ./data ./transformation.json . ./file_mapping.json
if __name__ == "__main__":
	t0 = time.time()

	if len(sys.argv)==5:
		if path.isdir(sys.argv[1]) and path.isfile(sys.argv[2]) and path.isdir(sys.argv[3]) and path.isfile(sys.argv[4]):

#			Sample paths,
#			directory='/root/data/yest'
#			transformation_file='/root/data/transformation.json'
#			output='/root/data/yest'

			search_directory=sys.argv[1]
			transformation_file=sys.argv[2]
			output_directory=sys.argv[3]
			file_mapping_file=sys.argv[4]

			transformations=None
			with open(transformation_file) as f:
				try:
					transformations=json.load(f)
				except:
					print("Invalid Transformation JSON, exiting")
					exit()
			file_mapping=None
			with open(file_mapping_file) as f:
				try:
					file_mapping=json.load(f)
				except:
					print("Invalid File Mapping JSON, exiting")
					exit()

			p=Parser(search_directory,transformations,output_directory,file_mapping)
			no_of_files_found=p.find_files()
			
			# If some files found then process them else Exit
			if no_of_files_found>0:
				print(no_of_files_found," Files Found")
				p.process_files()	
			else:
				print("No files found Exiting")

			# Time taken to process the folder
			print("\nTime taken to run the script:",time.time()-t0)

		else:
			print("Bad path")
			exit()
	else:
		print("Illegal Arguements")


	
