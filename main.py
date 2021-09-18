
import time
import sys
from parser_utils.parser import *
# python main.py ./data ./transformation.json . ./file_mapping.json
if __name__ == "__main__":
	t0 = time.time()

	if len(sys.argv)==4:
		if path.isdir(sys.argv[1]) and path.isfile(sys.argv[2]) and path.isdir(sys.argv[3]):


			search_directory=sys.argv[1]
			transformation_file=sys.argv[2]
			output_directory=sys.argv[3]
			

			transformations=None
			with open(transformation_file) as f:
				try:
					transformations=json.load(f)
				except:
					print("Invalid Transformation JSON, exiting")
					exit()
			
			p=Parser(search_directory,transformations,output_directory)
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


	
