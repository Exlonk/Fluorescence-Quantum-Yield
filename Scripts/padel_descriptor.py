import glob
import subprocess
import pandas as pd

# Calculate chemical descriptors using Padel algorithm

# directory with .mol files
directory = "/mnt/Particion_E/Proyectos/Doctorade/Thesis/calcs/databases/database_1/output/mol_extension/"

for index,files in enumerate(glob.glob(directory + "*.mol")):
    print(f"File {files} in process {index} of {len(glob.glob(directory + '*.mol'))}")
    try:
        subprocess.run(["java", "-jar", 
                            "/mnt/Particion_E/Proyectos/Doctorade/Tools/Chemical_Descriptors/PaDEL-Descriptor/PaDEL-Descriptor.jar", 
                            "-2d", "-3d", "-dir", f"{files}","-file",directory+"descriptors.csv"],timeout=300,
                            stdout=subprocess.PIPE,stderr=subprocess.PIPE, text=True)
        new = pd.read_csv(directory + "descriptors.csv")
        if index >0:
            descriptors_df= pd.concat([descriptors_df,new])
        else:
            descriptors_df = new.copy()
    except subprocess.TimeoutExpired:
        print(f"File {files} in process {index} of {len(glob.glob(directory + '*.mol'))} has not been processed")
        continue
    except subprocess.CalledProcessError:
        print(f"Execution error in file {files} in process {index} of {len(glob.glob(directory + '*.mol'))}")
        continue

descriptors_df.to_csv(directory + "descriptors.csv",index=False)
