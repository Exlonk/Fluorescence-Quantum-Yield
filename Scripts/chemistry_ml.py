import pandas as pd
from rdkit import Chem
from rdkit.Chem import AllChem
import py3Dmol
import os
import glob
import subprocess
import pandas as pd
import pubchempy as pcp

"""
Module to do chemistry with machine learning and quantum chemistry software.

ModuleS and its version used:
{'pandas': '2.1.1', 'rdkit': '2023.9.1', 'py3Dmol': '2.0.4'}

Exlonk G. P.
"""

def smiles_to_3d_rdkit_object(smiles):
    """
    Converts a SMILES string to a 3D molecular structure.

    Args:
    - smiles (str): SMILES string.

    Returns:
    - mol (rdkit.Chem.rdchem.Mol): 3D molecular structure.

    """
    mol = Chem.MolFromSmiles(smiles)
    mol = Chem.AddHs(mol)
    AllChem.EmbedMolecule(mol)
    AllChem.MMFFOptimizeMolecule(mol)
    return mol

def rdkit_mol_to_xyz_input(mol):
    """
    Converts a 3D molecular structure .mol rdkit to an XYZ input file format.

    Args:
    - mol (rdkit.Chem.rdchem.Mol): 3D molecular structure.

    Returns:
    - atom_block (str): XYZ input file format.

    """
    atom_block = ""
    for atom in mol.GetAtoms():
        symbol = atom.GetSymbol()
        x, y, z = mol.GetConformer().GetAtomPosition(atom.GetIdx())
        atom_block += f"{symbol} {x:.6f} {y:.6f} {z:.6f}\n"
    return atom_block

def smiles_3d_ff(df_file,column_name="SMILES",directory='mol3d'):
    """
        Converts SMILES strings to 3D molecular structures using a force field (RDKit) and saves them as input .xyz files 
        for quantum chemistry software.

        Arguments:
        - df_file (str): Path to the input file containing SMILES strings in .csv format with a column call 'ID' that identify 
                         each smiles to name the file.
        - column_name (str, optional): Name of the column containing the SMILES strings. Defaults to 'SMILES'.
        - directory (str, optional): Path of the directory where the input files will be saved (e.g., /home/usr/my_struct).
                                    The algorithm automatically creates the directory "my_struct" to save the structures.
                                    Defaults to 'mol3d'; in this mode, the folder is created where the module is saved.

        Returns:
        - None

        Example: smiles_3d_ff(df_file="/home/usr/documents/my_file.csv",column_name="smiles",directory='/home/usr/project/mol3d')
    """
    
    molecules_database = pd.read_csv(df_file)
    c = 0

    for index, i in enumerate(molecules_database[column_name]):
        print('Molecule in process: ',molecules_database['ID'][index])
        try:
            # molecule_name = str(index) 
            molecule_name = str(molecules_database['ID'][index]) # por borrar   <-----------------------
            mol = smiles_to_3d_rdkit_object(i) # i represent each molecule
            xyz_input = rdkit_mol_to_xyz_input(mol)
            if directory == 'mol3d':
                # Create output directories
                os.makedirs(directory, exist_ok=True)
                actual_directory = os.getcwd()
                directory = os.path.join(actual_directory, directory)
            else:
                os.makedirs(directory, exist_ok=True)
                directory = directory
            file = os.path.join(directory,molecule_name+".xyz")
            with open(file, "w") as f:
                f.write(xyz_input)

            with open(file, "r") as f:
                original_contents = f.read()
            with open(file, "r") as f:
                atoms_number = f.readlines()
                atoms_number = len(atoms_number)

            new_lines = "{0}\n"\
            "\n".format(atoms_number)

            new_contents = new_lines + original_contents

            with open(file, "w") as f:
                f.write(new_contents)

        except:
            # Molecules that can not be optimized with force field has to be shown
            c+=1 #por borrar
            print('molecule {0} can not be obtimized with the force field'.format(molecule_name))
            print('no optimized total number: ',c) # por borrar

def gaussian_files_generator(files_name=None):

    """
        Generates Gaussian input files (.inp) from .xyz files.

        Arguments:
        - None

        Returns:
        - None

        Example: gaussian_files_generator()
    """
    # Solicitando información al usuario
    num_processors = input("Número de procesadores: ")
    memory = input("Memoria (en GB): ")
    theory_level = input("Nivel de teoría (por ejemplo, B3LYP/6-31G(d)): ")
    charge = input("Carga: ")
    multiplicity = input("Multiplicidad: ")
    path = input("Directorio de los .xyz: ")
    prefijo = input("Prefijo output (por ejemplo, td_): ")
    chk_folder = input("chk folder: ")
    # Obtener una lista de todos los archivos .xyz en el directorio actual

    xyz_files = [f for f in os.listdir(path) if f.endswith('.xyz')]
    
    for index, xyz_file_path in enumerate(xyz_files):
        # Leyendo las coordenadas del archivo .xyz
        with open(os.path.join(path,xyz_file_path), 'r') as xyz_file:
            xyz_data = xyz_file.readlines()[2:]  # Ignorando las primeras dos líneas

        # Creando el contenido del archivo de entrada de Gaussian
        if files_name != None:
            name = files_name[index]
        else:
            name = xyz_file_path[:-4]

        input_content = (
                            "%nproc={0}\n"
                            "%mem={1}GB\n"
                            "%chk={8}{2}{7}.chk\n"
                            "{4}\n\n"
                            "name file: {3}\n\n"
                            "{5} {6}\n"
                        ).format(
                            num_processors,
                            memory,
                            prefijo,
                            name,
                            theory_level,
                            charge,
                            multiplicity,
                            xyz_file_path[:-4],
                            chk_folder
                        )
        input_content += ''.join(xyz_data)  # Añadiendo las coordenadas
        input_content += '\n'  # Añadiendo una línea vacía al final

        # Creando el nombre del archivo de salida basado en el nombre del archivo .xyz
        output_file_name = f'{prefijo}{os.path.splitext(xyz_file_path)[0]}.inp'

        # Guardando el archivo de entrada de Gaussian
        with open(os.path.join(path,output_file_name), 'w') as g_input_file:
            g_input_file.write(input_content)

        print(f'Archivo de entrada de Gaussian generado como {output_file_name}')

    return None
    
def padel_descriptors(java_path,padel_path,threads,mol_path):

    """
    Calculate the Padel descriptors for a given set of molecules with .mol extension.

    Parameters:
    java_path (str): The path to the Java executable.
    padel_path (str): The path to the Padel software.
    threads (int): The number of threads to use for the calculation. Default is 1.
    mol_path (str): The path to the file containing the molecules.

    Returns:
    None
    """

    """
    /home/exlonk/wcss_scripts/jre1.8.0_391/bin/java
    /home/exlonk/thesis/database/mol_extension/
    /home/exlonk/wcss_scripts/PaDel-Descriptors/PaDEL-Descriptor.jar
    """

    for index,files in enumerate(glob.glob(mol_path+ "*.mol")):
        print(f"File {files} in process {index} of {len(glob.glob(mol_path+ '*.mol'))}")
        try:
            result = subprocess.run([f"{java_path}", "-jar", f"{padel_path}",f" -threads {threads}","-2d", "-3d", "-dir", f"{files}","-file", mol_path + "descriptors.csv"],timeout=100,
                            stdout=subprocess.PIPE,stderr=subprocess.PIPE, text=True)       
            print("Error (stderr) del subproceso:")
            print(result.stderr)
            new = pd.read_csv(mol_path+ "descriptors.csv")
            if index >0:
                descriptors_df= pd.concat([descriptors_df,new])
            else:
                descriptors_df = new.copy()
        except subprocess.TimeoutExpired:
            print(f"File {files} in process {index} of {len(glob.glob(mol_path + '*.mol'))} has not been processed")
            continue
        except subprocess.CalledProcessError:
            print(f"Execution error in file {files} in process {index} of {len(glob.glob(mol_path + '*.mol'))}")
            continue

    descriptors_df.to_csv(mol_path + "descriptors.csv",index=False)

def get_smiles_from_common(common_name):
    """
    Retrieves the SMILES representation of a compound based on its common name.

    Args:
        common_name (str): The common name of the compound.

    Returns:
        str: The SMILES representation of the compound if found, or a message indicating no compound was found.
        If an error occurs during the retrieval process, an error message is returned.
    """
    try:
        # Search for the compound by its common name
        compound = pcp.get_compounds(common_name, 'name')
        
        # Assuming the first match is the desired one, get its SMILES representation
        if compound:
            return compound[0].isomeric_smiles
        else:
            return "No compound found with that name."
    except Exception as e:
        return f"An error occurred: {e}"
    
    return None
