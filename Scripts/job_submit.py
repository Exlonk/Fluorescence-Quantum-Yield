import subprocess
import os
import glob
import csv
from datetime import datetime
import pandas as pd
import json
import time

# COMMANDS TO USE

id_command = 'squeue -h -o %i -u exlonk_'
name_command = 'squeue -h -o "%j" --job={0}'
status_command = 'squeue -h -o "%t" --job={0}'
directory_calcs = '/users/kdm/exlonk_/calc/' # remember the / at the end, here the calcs are running
directory_submit = '/users/kdm/exlonk_/' # remember the / at the end, here is the submit file
directory_orca_sh = '/users/kdm/exlonk_/software/orca.sh'

def linux_command_queue(command):
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # Wait for the process to complete and capture output/errors
    # Capture the output and error, if any
    output, error = process.communicate()

    # Decode the output and error from bytes to strings
    output_str = output.decode('utf-8').strip()
    error_str = error.decode('utf-8').strip()

    return output_str,error_str

# Cluster status
queue = {}
jobs_ids = linux_command_queue(id_command)[0].split('\n')
queue['NAME'] = []
queue['ID'] = []
queue['STATUS'] = []
if jobs_ids[0] != '':
    for i in jobs_ids:
        queue['NAME'].append(linux_command_queue(name_command.format(i))[0][:-4])
        queue['ID'].append(i)
        queue['STATUS'].append(linux_command_queue(status_command.format(i))[0])

status_dataframe = pd.DataFrame(queue)

# submit file

submit_file = pd.read_csv(directory_submit+'submit.csv')
submit_file

# ID and names of the ended jobs

id_jobs_ended = []

for i in submit_file[submit_file['STATUS']=='IN PROGRESS']['ID']:
    if str(i) not in status_dataframe['ID'].values :
        id_jobs_ended.append(i)

names_jobs_ended = []
for i in id_jobs_ended:
    names_jobs_ended.append(submit_file[submit_file['ID'] == i]['NAME'].values.tolist()[0])

## -------------------- ERROR MODULES ---------------------------- ##

def opt(name,directory_calcs,submit_file):
    submit_file = submit_file
    file = directory_calcs+name+'.out'
    
    normal_termination1, normal_termination2  = linux_command_queue('tail -n 10 {0} | grep -c "****ORCA TERMINATED NORMALLY****" '.format(file))
    
    if normal_termination2 == '':    
        normal_termination = int(normal_termination1[0])    
    else:
        submit_file.loc[submit_file['NAME'] == name, 'COMMENT'] = "ERROR APPLYING GREP"
        normal_termination = 2

    # NORMAL TERMINATION 
    if normal_termination == 1:
        imaginary_frequencies = int(linux_command_queue('grep -c "***imaginary mode***" {0}'.format(file))[0].split('\n')[0])
        
        if imaginary_frequencies > 0: 
            # Get the imaginary values
            imaginary_information = linux_command_queue('grep -n "***imaginary mode***" {0}'.format(file))[0].split('\n')
            imaginary_values = []
            
            for i in imaginary_information:
                imaginary_values.append(float(i.split()[2]))

            #-- IMAGINARY VALUES SMALLER THAN 100  --#

            if all(abs(img) < 100 and img != 0 for img in imaginary_values) == True:
                # Creation of the OPT dictionary
                if 'OPT' not in submit_file[submit_file['NAME']==name]['ERROR_HANDLING'].values.tolist()[0]:
                    submit_file.loc[submit_file['NAME'] == name, 'ERROR_HANDLING'] = '{"OPT":[]}'
                    add_solution = json.loads(submit_file[submit_file['NAME']==name]['ERROR_HANDLING'].values.tolist()[0]) 
                else:
                   
                    add_solution = json.loads(submit_file[submit_file['NAME']==name]['ERROR_HANDLING'].values.tolist()[0]) # dictionary of errors
                    
                # 1ST SOLUTION TO IMPLEMENT 
                
                tightopt = "!TightOpt | used to lower the thresholds for the geometry optimization"
               
                # 2TH SOLUTION TO IMPLEMENT
                verytight = "!Verytightopt defgrid3 | used to lower the threshold for the geometry optimization with tightening the numerical integration grid"

                if tightopt not in add_solution['OPT']:
                    # CHHANGE THE FILE TO APPLIED THE SOLUTION,
                    # ADD THE SOLUTION APPLIED TO THE HANDLING ERROR, INCREASE THE JOB, CHANGE THE STATUS

                    # COPY OF THE OUTPUT WITH THE ERROR
                    file_copy = file[:-4]+'_error_'+str(submit_file[submit_file['NAME']==name]['TRIAL_NUMBER'].values[0])+'.out'
                    cp1,cp2 = linux_command_queue("cp {0} {1}".format(file,file_copy))
                    if cp2 != '':
                        print("Error cp old file: {0}".format(file))
                    else:
                        print("Error file: {0}, successfully copied".format(file))          
                    # APPLIED THE SOLUTION
                    sed1, sed2 =linux_command_queue("sed -i '0,/OPT/{{s/OPT/TIGHTOPT/i}}' {}".format(file[:-4]+'.inp')) 
                    if sed2 != '':
                        print("Error appling solution to file: {0}".format(file))  
                    else:
                        print("Sed successfully appliedto file: {0}".format(file))
                    add_solution["OPT"].append(tightopt)
                    # adding the solution to submit
                    submit_file.loc[submit_file['NAME'] == name, 'ERROR_HANDLING'] = json.dumps(add_solution) 
                    # Increment 'TRIAL_NUMBER' and set 'STATUS' to 'in progress'
                    submit_file.loc[submit_file['NAME'] == name, 'TRIAL_NUMBER'] =  submit_file[submit_file['NAME']==name]['TRIAL_NUMBER'] + 1
                    submit_file.loc[submit_file['NAME'] == name, 'STATUS'] = 'TO SUBMIT'
                    linux_command_queue('rm {0}'.format(directory_calcs+name+'.out'))              
              
                elif verytight not in add_solution['OPT']:
                    print('here')
                    # CHHANGE THE FILE TO APPLIED THE SOLUTION,
                    # ADD THE SOLUTION APPLIED TO THE HANDLING ERROR, INCREASE THE JOB, CHANGE THE STATUS

                    # COPY OF THE OUTPUT WITH THE ERROR
                    file_copy = file[:-4]+'_error_'+str(submit_file[submit_file['NAME']==name]['TRIAL_NUMBER'].values[0])+'.out'
                    cp1,cp2 = linux_command_queue("cp {0} {1}".format(file,file_copy))
                    if cp2 != '':
                        print("Error cp old file: {0}".format(file))
                    else:
                        print("Error file: {0}, successfully copied".format(file))          
                    # APPLIED THE SOLUTION
                    sed1, sed2 =linux_command_queue("sed -i '0,/TIGHTOPT/{{s/TIGHTOPT/VERYTIGHTOPT DEFGRID3/i}}' {}".format(file[:-4]+'.inp')) 
                    if sed2 != '':
                        print("Error appling solution to file: {0}".format(file))  
                    else:
                        print("Sed successfully appliedto file: {0}".format(file))
                    add_solution["OPT"].append(verytight)
                    # adding the solution to submit
                    submit_file.loc[submit_file['NAME'] == name, 'ERROR_HANDLING'] = json.dumps(add_solution) 
                    # Increment 'TRIAL_NUMBER' and set 'STATUS' to 'in progress'
                    submit_file.loc[submit_file['NAME'] == name, 'TRIAL_NUMBER'] =  submit_file[submit_file['NAME']==name]['TRIAL_NUMBER'] + 1
                    submit_file.loc[submit_file['NAME'] == name, 'STATUS'] = 'TO SUBMIT'
                    linux_command_queue('rm {0}'.format(directory_calcs+name+'.out'))

                else:
                    # Set 'STATUS' to 'FAIL' and add a comment
                    submit_file.loc[submit_file['NAME'] == name, 'STATUS'] = 'FAIL'
                    submit_file.loc[submit_file['NAME'] == name, 'COMMENT'] = 'All frequencies below 100 and applied solutions didn\'t work'

            #-- 1ST SOLUTION FOR IMAGINARY VALUES GREATER THAN 100  --#        

            else:
                
                # ADD THE COMMENT IN THE COMMENT SECTION TO CHANGE THE GEOMETRY
                if 'OPT' not in submit_file[submit_file['NAME']==name]['ERROR_HANDLING'].values.tolist()[0]:
                    submit_file.loc[submit_file['NAME'] == name, 'ERROR_HANDLING'] = '{"OPT":[]}'
                    add_solution = json.loads(submit_file[submit_file['NAME']==name]['ERROR_HANDLING'].values.tolist()[0]) 
                else:
                    add_solution = json.loads(submit_file[submit_file['NAME']==name]['ERROR_HANDLING'].values.tolist()[0]) # dictionary of errors
                
                submit_file.loc[submit_file['NAME'] == name, 'STATUS'] = 'FAIL'
                submit_file.loc[submit_file['NAME'] == name, 'COMMENT'] =  'Modify the initial geometry'

        else:
            # NORMAL TERMINATION WITHOUT IMAGINARY FREQUENCIES
            submit_file.loc[submit_file['NAME'] == name, 'STATUS'] = 'COMPLETE'
    # NOT NORMAL TERMINATION
    elif normal_termination == 0:
        if 'OPT' not in submit_file[submit_file['NAME']==name]['ERROR_HANDLING'].values.tolist()[0]:
            submit_file.loc[submit_file['NAME'] == name, 'ERROR_HANDLING'] = '{"OPT":[]}'
            add_solution = json.loads(submit_file[submit_file['NAME']==name]['ERROR_HANDLING'].values.tolist()[0]) 
        else:
            add_solution = json.loads(submit_file[submit_file['NAME']==name]['ERROR_HANDLING'].values.tolist()[0]) # dictionary of errors

        submit_file.loc[submit_file['NAME'] == name, 'STATUS'] = 'FAIL'
        submit_file.loc[submit_file['NAME'] == name, 'COMMENT'] =  'ERROR SIN SOLUCION PREDETERMINADA'
    # PROBLEMS READING THE FILE
    elif normal_termination == 2:
        submit_file.loc[submit_file['NAME']==name,'ID'] = 0
        submit_file.loc[submit_file['NAME']==name,'STATUS'] = 'TO SUBMIT'
        submit_file.loc[submit_file['NAME']==name,'ERROR_HANDLING'] = '{}'
        submit_file.loc[submit_file['NAME']==name,'TRIAL_NUMBER'] = 1
        submit_file.loc[submit_file['NAME']==name,'DATE'] = ''
        submit_file.loc[submit_file['NAME']==name,'TIME'] = ''
        submit_file.loc[submit_file['NAME']==name,'COMMENT'] = 'THE OUTPUT CANNOT BE ANALYSE, RESENDING AGAIN'

def scf(name,directory_calcs,submit_file):
    pass

# RUN THE MODULES

for i in names_jobs_ended:
    # i = name of the file
    for  k in submit_file[submit_file['NAME']==i]['ERROR_MODULES'].values.tolist()[0].strip().split(','):
        print(i)
        print(k)
        globals()[k.lower()](i,directory_calcs,submit_file)

## --------------------- END MODULES -------------------------------- ##

## ----------------- SUBMIT MODULE ---------------------------------- ##
number_jobs_in_progress = status_dataframe.shape[0]
number_jobs_in_progress

j = 0

number_to_submit_jobs = 20 # VERY IMPORTANT PARAMETER!! HOW MANY JOBS I WANT TO RUN

for i in submit_file['NAME']:
    if ("TO SUBMIT" == submit_file[submit_file["NAME"]==i]["STATUS"].values[0]) == True:
            if j < (number_to_submit_jobs - number_jobs_in_progress) and (number_to_submit_jobs - number_jobs_in_progress) > 0:
                j+=1
                if "ORCA" == submit_file[submit_file["NAME"]==i]["ALGORITHM"].values[0]:
                    os.chdir(directory_calcs)
                    command = "sbatch -J {0} {1}".format(i,directory_orca_sh)
                    orca1, orca2 = linux_command_queue(command)
                    time.sleep(1)
                    if orca2 == '':
                        submit_file.loc[submit_file['NAME'] == i,"ID"] = orca1.split()[-1] # orca1 is its id
                        submit_file.loc[submit_file['NAME'] == i,"STATUS"] = "IN PROGRESS"
                        submit_file.loc[submit_file['NAME'] == i,"DATE"] = str(datetime.now().date())
                        submit_file.loc[submit_file['NAME'] == i,"TIME"] = str(datetime.now().time())[:-4]
                    else:
                        submit_file.loc[submit_file['NAME'] == i,"STATUS"] = "CLUSTER ERROR"
                        submit_file.loc[submit_file['NAME'] == i,"COMMENT"] = orca2

submit_file.to_csv(directory_submit+'submit.csv',index=False)
