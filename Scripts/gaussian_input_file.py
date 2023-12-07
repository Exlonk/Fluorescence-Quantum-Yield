# Toma todos los archivos con extension .xyz en un directorio en especifico
# y genera archivos de gaussian de entrada con los parametros introducidos
import os

def gaussian_input_generator(num_processors, memory, theory_level, charge, multiplicity,path,prefijo):
    # Obtener una lista de todos los archivos .xyz en el directorio actual
    xyz_files = [f for f in os.listdir(path) if f.endswith('.xyz')]
    
    for xyz_file_path in xyz_files:
        # Leyendo las coordenadas del archivo .xyz
        with open(os.path.join(path,xyz_file_path), 'r') as xyz_file:
            xyz_data = xyz_file.readlines()[2:]  # Ignorando las primeras dos líneas

        # Creando el contenido del archivo de entrada de Gaussian
        input_content = f"""%nproc={num_processors}
%mem={memory}
%chk={prefijo}{xyz_file_path[:-4]}.chk
#p {theory_level}

Title Card Required

{charge} {multiplicity}
"""
        input_content += ''.join(xyz_data)  # Añadiendo las coordenadas
        input_content += '\n'  # Añadiendo una línea vacía al final

        # Creando el nombre del archivo de salida basado en el nombre del archivo .xyz
        output_file_name = f'{prefijo}{os.path.splitext(xyz_file_path)[0]}.inp'

        # Guardando el archivo de entrada de Gaussian
        with open(os.path.join(path,output_file_name), 'w') as g_input_file:
            g_input_file.write(input_content)

        print(f'Archivo de entrada de Gaussian generado como {output_file_name}')

# Solicitando información al usuario
num_processors = input("Número de procesadores: ")
memory = input("Memoria (en GB, por ejemplo, 4GB): ")
theory_level = input("Nivel de teoría (por ejemplo, B3LYP/6-31G(d)): ")
charge = input("Carga: ")
multiplicity = input("Multiplicidad: ")
path = input("Directorio: ")
prefijo = input("Prefijo output (por ejemplo, td_): ")
# Llamada a la función para ejecutar el script
gaussian_input_generator(num_processors, memory, theory_level, charge, multiplicity,path,prefijo)

