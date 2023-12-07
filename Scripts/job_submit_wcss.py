import subprocess
import os
import glob
import csv
from datetime import datetime
# NOTE: the name of the files has to be writen as name.extension (the point is very important and only can appear once)
# '/home/exlonk/submit.csv' name of the file that controls submission,

# python 3.6

# This algorithm runs continuosly calculations in the wcss cluster

def save_as_csv(file_path, data):
    # Extract column headers from the keys of the first dictionary
    headers = data[0].keys()

    # Write data to CSV file
    with open(file_path, 'w', newline='') as file:
        csv_writer = csv.DictWriter(file, fieldnames=headers)
        
        # Write headers
        csv_writer.writeheader()
        
        # Write rows
        csv_writer.writerows(data)
    
    return None

# -------------------  FOLDERS AND FILES ----------------------------- #

directory_calcs = '/home/exlonk/in_progress/' # where are the calculations to check
directory_error = '/home/exlonk/errors/' # where to move the calculations with errors
directory_results = '/home/exlonk/results/' # where to move the calculations with errors
submit_file = '/home/exlonk/submit.csv'

# creation storage_copy_error
if not os.path.exists(os.path.join(directory_results,"storage_copy_error")):
# Si el archivo no existe, se crea
    with open(os.path.join(directory_results,"storage_copy_error"), 'w') as file:
        file.write("")

# # # GAUSSIAN 09 / 16 # # #

# ------------------------------- ERROR HANDLING ----------------------------------- # 

# Check if the last 10 lines of the output file contain the string "Error termination" 
# If so, move the calculation to the error directory and add the comment "ERROR" to the dataframe 
# submit
      
for i in glob.glob(os.path.join(directory_calcs,'*.log')):
    linux_command = subprocess.Popen('tail -n 10 {0} | grep "Error termination"'.format(i),shell=True,
                                     stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    output, error = linux_command.communicate()
    if output.decode('utf-8'):
        subprocess.Popen('mv '+i[:-3]+'* '+directory_error,shell=True)
        
# ----------------------------- NORMAL TERMINATION --------------------------------- #

# Check if the last 10 lines of the output file contain the string "Normal termination"
# If so, move the calculation to the results directory

for i in glob.glob(os.path.join(directory_calcs,'*.log')):
    linux_command = subprocess.Popen('tail -n 10 {0} | grep "Normal termination"'.format(i),shell=True,
                                     stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    output, error = linux_command.communicate()
    if output.decode('utf-8'):
        subprocess.Popen('mv '+i[:-3]+'* '+directory_results,shell=True)

# # # GAUSSIAN FINAL CHECK # # #

# # # SUBMIT FILE MODIFICATION ###

# ERROR TERMINATION

# enocunter the unique files in the directory error
list_errors = []
for i in glob.glob(os.path.join(directory_error,'*')):
    file_error = i.replace(directory_error,'')
    file_error = file_error.split('.')[0]
    list_errors.append(file_error)
list_errors = list(set(list_errors))  # name of the error files without extension

# add ERROR to the status column
file_lines_error = []
with open(submit_file,'r') as submit:
    submit_lines = csv.DictReader(submit)
    for row in submit_lines:
        name = str(row['FILE']).split('.')[0]
        if name in list_errors:
            row['STATUS'] = 'ERROR'
            file_lines_error.append(row)
        else:
            file_lines_error.append(row)

save_as_csv(submit_file, file_lines_error)

# NORMAL TERMINATION 

# enocunter the unique files in the directory results
list_results = []
for i in glob.glob(os.path.join(directory_results,'*')):
    file = i.replace(directory_results,'')
    file = file.split('.')[0]
    list_results.append(file)
list_results = list(set(list_results)) 
list_results.remove('storage_copy_error') # files in the directory results without extension

# add COMPLETE to the status and the number of submission
file_lines_normal = []
with open(submit_file,'r') as submit:
    submit_lines = csv.DictReader(submit)
    for row in submit_lines:
        name = str(row['FILE']).split('.')[0]
        if name in list_results:
            row['STATUS'] = 'COMPLETE'
            file_lines_normal.append(row)
        else:
            file_lines_normal.append(row)

# Save the list of dictionaries as a CSV file
save_as_csv(submit_file, file_lines_normal)

# IN PROGRESS

# Execute the command and capture its output
process = subprocess.Popen('squeue -u exlonk -o "%100j"', shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

# Wait for the process to complete and capture output/errors
output, error = process.communicate()

# Decode output to string (assumes output is text)
output_str = output.decode('utf-8')

# Display the command output
list_in_progress = [x.replace(" ","") for x in list(output_str.split('\n'))[1:-1]]

# check files to submit again that are not either in the error folder or normal folder
list_in_progress_csv = []
with open(submit_file,'r') as submit:
    submit_lines = csv.DictReader(submit)
    for row in submit_lines:
        if str(row['STATUS']) == 'IN PROGRESS' or str(row['STATUS']) == 'SUBMITED':
            list_in_progress_csv.append(row['FILE'].split('.')[0])
if len(list_in_progress_csv) != 0: 
    for i in list_in_progress_csv:
        file_lines_progress = []
        if i not in list_in_progress:
            with open(submit_file,'r') as submit:
                submit_lines = csv.DictReader(submit)
                for row in submit_lines:
                    if str(row['FILE']).split('.')[0] == i:
                        row['STATUS'] = 'TO SUBMIT'
                        file_lines_progress.append(row)
                    else:
                        file_lines_progress.append(row)
                save_as_csv(submit_file, file_lines_progress)

# add IN PROGRESS to the status
file_lines_progress = []
with open(submit_file,'r') as submit:
    submit_lines = csv.DictReader(submit)
    for row in submit_lines:
        name = str(row['FILE']).split('.')[0]
        if name in list_in_progress:
            process = subprocess.Popen('squeue -n {0}'.format(name), shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            output, error = process.communicate()
            output_str = output.decode('utf-8')
            run = output_str.split('\n')[1:-1][0].split()[4]
            if run == 'R':
                row['STATUS'] = 'IN PROGRESS'
            if run == 'PD':
                row['STATUS'] = 'SUBMITED'            
            if row['JOB_#'] == '':
                row['JOB_#'] = 1                   
            file_lines_progress.append(row)
        else:
            file_lines_progress.append(row)

# Save the list of dictionaries as a CSV file
save_as_csv(submit_file, file_lines_progress)

# # # SUBMIT NEW JOBS # # #
def submit_job(name,algorithm,directory_calcs):
    if algorithm == 'GAUSSIAN':
        linux_command = subprocess.Popen(
            '/usr/local/bin/bem2/sub-gaussian-2016-C.01 {0} -c 26 -m 16 -t 168'.format(os.path.join(directory_calcs,name))
            ,shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        output, error = linux_command.communicate()
        print(output.decode('utf-8'))
        if output.decode('utf-8'):
            file_lines_progress = []
            with open(submit_file,'r') as submit:
                submit_lines = csv.DictReader(submit)
                for row in submit_lines:
                    name_file = str(row['FILE']).split('.')[0]
                    if name_file == name.split('.')[0]:            
                        row['DATE'] = str(datetime.now().date())
                        row['TIME'] = str(datetime.now().time())[:-4]
                        row['COMMENT'] = str(output.decode('utf-8')).split('\n')[0]
                        if row['STATUS'] == 'TO SUBMIT' and row['JOB_#'] != '':
                            row['JOB_#'] = int(row['JOB_#'])+1 
                        if row['JOB_#'] == '':
                            row['JOB_#'] = 1 
                        row['STATUS'] = 'SUBMITED'
                        file_lines_progress.append(row)
                    else:
                        file_lines_progress.append(row)
            save_as_csv(submit_file, file_lines_progress)
        if error.decode('utf-8'):
            file_lines_progress = []
            with open(submit_file,'r') as submit:
                submit_lines = csv.DictReader(submit)
                for row in submit_lines:
                    name_file = str(row['FILE']).split('.')[0]
                    if name_file == name:            
                        row['COMMENT'] = str(error.decode('utf-8')).split('\n')[0]
                        file_lines_progress.append(row)
                    else:
                        file_lines_progress.append(row)

            # Save the list of dictionaries as a CSV file
            save_as_csv(submit_file, file_lines_progress)
    return None
    
if len(list_in_progress) <= 495:
    with open(submit_file,'r') as submit:
        submit_lines = csv.DictReader(submit)
        counter = 0
        for row in submit_lines:
            if str(row['STATUS']) == 'TO SUBMIT':
                submit_job(row['FILE'],row['ALGORITHM'],directory_calcs)
                counter+=1
            if (counter+len(list_in_progress)) == 495:
                break

# ---------------------- Store the data in the cloud ------------------------------- #

linux_command = subprocess.Popen('aws s3 cp {0} s3://pqyl-storage/results/ --output text --only-show-errors --recursive'.format(directory_results),shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
output, error = linux_command.communicate()
if output.decode('utf-8') == '':

    # CHECK SYSTEM AVAILABLE SPACE #
    space_command = subprocess.Popen('quota -s -f ~', shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # Wait for the process to complete and capture output/errors
    output_space, error_space = space_command.communicate()

    # Decode output to string (assumes output is text)
    output_space= output_space.decode('utf-8')
    output_values = output_space.split('\n')[3:-1][0].split()
    used = int(output_values[0][:-1])
    total = int(output_values[2][:-1])
    percentage_used = used/total*100
    if percentage_used > 70:
        for i in glob.glob(os.path.join(directory_results,'*')):        
            if i != os.path.join(directory_results,"storage_copy_error"):
                subprocess.Popen('rm {0}*'.format(i), shell=True)

else:
    with open(directory_results+'storage_copy_error','a') as f:
        f.write(error.decode('utf-8'))
        f.write('\n')
        f.close()

              
        
                
                

