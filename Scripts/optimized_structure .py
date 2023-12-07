#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 17 14:35:07 2020

@author: exlonk gil p.
"""
# Find the optimized structure of an optimization performed in Gaussian (09 and 16),
# and return a .xyz file

import re,os

entrada_teclado_2=False
archivos_malos=[]

#----------------------------------------------------------------------------#
# Entrada de datos

while entrada_teclado_2 != True:
    opcion=input('1) Nombre archivo\n2) Todos los archivos en esta carpeta\n3) Rutas separas por coma\nR: ')
    if opcion == '0' or opcion == '1' or opcion == '2' or opcion=='3':
        entrada_teclado_2=True
    else:
        print('\nIngrese un valor correcto')
        
if opcion=='1' :
    rutas=os.getcwd()
    rutas=rutas.split(',')
    archivos=input('Introduzca el nombre de los archivos separados por coma:\n')
    archivos+=','
    lista_archivos=[]
    nombre=''
    for cadena in range(len(archivos)):
        if archivos[cadena] != ',':
            nombre+=archivos[cadena]
        else:
            lista_archivos.append(nombre)
            nombre=''

if opcion=='2':
    rutas=os.getcwd()
    rutas=rutas.split(',')

if opcion=='3':
    rutas=input('Rutas: ')
    rutas=rutas.split(',')

#----------------------------------------------------------------------------#
import subprocess

def find_line_number(filename, string):
  """Returns the line number where the given string is found in the given file, or None if the string is not found."""

  command = ["grep", "-n", string, filename]
  output = subprocess.check_output(command).decode("utf-8")

  # Split the output into lines.
  lines = output.splitlines()

  # If the output is empty, then the string was not found.
  if not lines:
    return None

  # Return the line number.
  line_number = int(lines[0].split(":")[0])

  return line_number

# Busqueda de información
    
for x in range(0,len(rutas)):
    
    if opcion=='3' or opcion=='2':
            lista_archivos=os.listdir(rutas[x])
            
    # Buqueda en los archivos
    for recorrido in range(len(lista_archivos)):
        entrada_1=False 
        entrada_2=False
        
        # Busca archivos .log
        busqueda=re.compile(r'.*\.log')
        objeto=busqueda.search(lista_archivos[recorrido])
        if str(type(objeto)) != '<class \'NoneType\'>':
            entrada_1=True
        
        # Determina si terminaron adecuadamente y si existen
        if entrada_1==True:
            if os.path.exists(os.path.join(rutas[x],objeto.group())):
                for i in range(1,20):
                   archivo=open(os.path.join(rutas[x],objeto.group()))
                   busqueda_normal=re.compile(r'Normal termination')
                   resultado=busqueda_normal.search(archivo.readlines()[-i])
                   if str(type(resultado)) != '<class \'NoneType\'>':
                       entrada_2=True     
                       archivo.close()
                       break
                   archivo.close()
                if entrada_2==False:
                    archivos_malos.append(objeto.group())
          
        # Busqueda en el archivo  
        if entrada_2==True:
            coordenadas = []
            nombre=objeto.group().split('.')
            nombre=nombre[0]+'.xyz'
            
            if os.path.exists(os.path.join(rutas[x],nombre)):
                os.remove(os.path.join(rutas[x],nombre))
                
            # Numero de átomos
            archivo=open(os.path.join(rutas[x],objeto.group()))
            start_line_1 = find_line_number(os.path.join(rutas[x],objeto.group()), "NAtoms=")
            start_line_2 = find_line_number(os.path.join(rutas[x],objeto.group()), " Charge")
            for k in range(start_line_1-5,len(archivo.readlines())):
                atomos_completos=False
                archivo.close()
                archivo=open(os.path.join(rutas[x],objeto.group()))
                busqueda_numero_atomos=re.compile(r'NAtoms=')
                if busqueda_numero_atomos.search(archivo.readlines()[k]) != None:
                    archivo.close()
                    archivo=open(os.path.join(rutas[x],objeto.group()))
                    numero_atomos=archivo.readlines()[k].split()
                    numero_atomos=int(numero_atomos[1])
                    archivo.close()
                    break
                
            archivo=open(os.path.join(rutas[x],objeto.group()))
            for k in range(start_line_2-5,len(archivo.readlines())):
                archivo=open(os.path.join(rutas[x],objeto.group()))
                busqueda_atomos=re.compile(r'^ Charge')
                if busqueda_atomos.search(archivo.readlines()[k]) != None:
                    archivo.close()
                    for z in range(1,numero_atomos+1):
                        archivo=open(os.path.join(rutas[x],objeto.group()))
                        atomo=archivo.readlines()[k+z].split()
                        coordenadas.append(atomo[0])
                        archivo.close()
                    break
             
            # Busqueda estructuras
            archivo=open(os.path.join(rutas[x],objeto.group()))
            start_line_3 = find_line_number(os.path.join(rutas[x],objeto.group()), " Optimization completed.")
            archivo=open(os.path.join(rutas[x],objeto.group()))
            for k in range(start_line_3-5,len(archivo.readlines())):
                archivo.close()
                archivo=open(os.path.join(rutas[x],objeto.group()))
                busqueda_optimizacion=re.compile(r'^ Optimization completed.')
                if busqueda_optimizacion.search(archivo.readlines()[k]) != None:
                    archivo.close()
                    archivo=open(os.path.join(rutas[x],objeto.group()))
                    for g in range(k,len(archivo.readlines())):
                        busqueda_coordenadas=re.compile(r'                         Standard orientation:')
                        archivo.close()
                        archivo=open(os.path.join(rutas[x],objeto.group()))
                        if busqueda_coordenadas.search(archivo.readlines()[g]) != None:
                          for p in range(0,len(coordenadas)):
                              archivo.close()
                              archivo=open(os.path.join(rutas[x],objeto.group()))
                              cadena=archivo.readlines()[g+5+p]
                              cadena=cadena.split()
                              coordenadas[p]=coordenadas[p]+' '+cadena[3]+' '+cadena[4]+' '+cadena[5]+'\n'  
                              archivo.close()
                          break
                    break
            # Mostrar en pantalla archivos que van finalizando
            print(objeto.group())
            # Guardar resultados
            Resultado=open(os.path.join(rutas[x],nombre),'a')
            Resultado.write(str(len(coordenadas))+'\n\n')
            Resultado.close()
            for r in range(0,len(coordenadas)):
                Resultado=open(os.path.join(rutas[x],nombre),'a')
                Resultado.write(coordenadas[r])
                Resultado.close()
 
    # Mostrar archivos que no terminarón correctamente           
    if len(archivos_malos) != 0:
        if os.path.exists(os.path.join(rutas[x],'Resultados_malos.txt')):
                os.remove(os.path.join(rutas[x],'Resultados_malos.txt'))
        print('\nArchivos que no terminaron correctamente: ',archivos_malos)
        archivo_salida=open(os.path.join(rutas[x],'Resultados_malos.txt'),'w')
        archivo_salida.write('Archivos que no terminaron correctamente: \n')
        archivo_salida.close()
        for z in range(0,len(archivos_malos)):
            archivo_salida=open(os.path.join(rutas[x],'Resultados_malos.txt'),'a')
            archivo_salida.write('\n'+archivos_malos[z])
            archivo_salida.close()
        
#----------------------------------------------------------------------------#
