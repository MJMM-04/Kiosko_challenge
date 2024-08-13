from enum import unique
import pandas as pd
import os
import numpy as np
from datetime import datetime
import psycopg2

#Variables globales

#Definir rutas
path_folder = os.path.dirname(os.path.abspath(__file__))
path_file_master = os.path.join(path_folder, 'input/individual_stocks_5yr')
path_folder_output_qa = os.path.join(path_folder, 'output/data_quality')
path_folder_output_load = os.path.join(path_folder, 'output/load')
today = datetime.today().now()

#Nombre del archivos
master_file_name = 'all_stocks_5yr.csv'
null_file_name= 'null_data.csv'
dim_name_file = 'dim_name_data.csv'
fact_stockshare_file = 'data_fact_stockShare.csv'

#Función para leer archivos utilizando pandas
def read_csv_file(path_file):
    try:
        df = pd.read_csv(path_file)
        return df
    except Exception as e:
        print(f"Error: {e}")

#Función para abrir archivos utilizando pandas
def open_csv_file(path_file):
    try:
        f = open(path_file,'r')
        return f
    except Exception as e:
        print(f"Error: {e}")        

##Funcíon ejemplo QA
def check_file(df):
    datos_nulos = df[df.isnull().any(axis=1)]
    if not datos_nulos.empty:
        # Guardar df con nulos
        datos_nulos.to_csv(path_folder_output_qa+'/'+null_file_name, index=False)
    else:
        print("No se encontraron datos nulos en el archivo")


#Archivo de dim
def extract_dim(path,nombre):
    df = pd.read_csv(path)
    #Solo los valores unicos de name 
    df = df[nombre].unique().tolist()
    df_unique = pd.DataFrame(df,columns =[nombre])
    df_unique[['created_at','updated_at']] = today
    df_unique.to_csv(path_folder_output_load+'/dim_name_data.csv', index=False, header=False)


# Función para crear CSV
def create_csv(df,file_name):
    df.to_csv(path_folder_output_load+'/'+file_name, index=False, header=False)
  
    



if __name__ == "__main__":

    df_master = read_csv_file(path_file_master+'/'+master_file_name)
    df_nullos = check_file(df_master)    
    extract_dim(path_file_master+'/'+master_file_name,"Name")
    #Conexión a la base de datos, recomendación leer las credenciales de un archivo
    conn = psycopg2.connect(
        host="localhost",
        database="kiosko",
        user="postgres",
        password="123456")
    cursor = conn.cursor()
    #Carga de dim_name
    cursor.execute('truncate table dim_name')
    f= open_csv_file(path_folder_output_load+'/'+dim_name_file)
    cursor.copy_from(f, 'dim_name', sep=',', columns=['name','created_at','updated_at'])
    conn.commit()
    f.close()

    #Obtenemos los registros de dim_name
    cur = conn.cursor()
    cur.execute('SELECT name_id,name FROM dim_name;')
    db_dim_name = cur.fetchall()
    db_dim_name = pd.DataFrame(db_dim_name,columns =['ID','Name'])

    df_historico = read_csv_file(path_file_master+'/'+master_file_name)
    # Se hace proceso de transformación
    df_historico[['created_at']] = today
    df_historico = df_historico.merge(db_dim_name, left_on='Name', right_on='Name',how='inner')
    df_historico = df_historico[['date','open','high','low','close','volume','ID','created_at']]
    #Borramos los registros nullos
    df_historico = df_historico.dropna()
    # Creamos archivo CSV solo con las columnas de la tabla en la bdd se simula del DL
    create_csv(df_historico,fact_stockshare_file)
    #Carga de fact_stockShare
    cursor.execute('truncate table fact_stockshare')
    f = open_csv_file(path_folder_output_load+'/'+fact_stockshare_file)
    cursor.copy_from(f, 'fact_stockshare', sep=',', columns=['date','open','high','low','close','volume','name_id','created_at'])
    conn.commit()
    f.close()
    cursor.close()


