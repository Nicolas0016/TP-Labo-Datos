#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Feb  8 07:40:51 2026

@author: nicolas
"""

# %%

import pandas as pd
import duckdb as dd
#%%
carpeta = './Archivos-TP/' 
censo2010 = pd.read_excel(carpeta + 'censo2010.xlsX') 
censo2022 = pd.read_excel(carpeta + 'censo2022.xlsX')
defunciones = pd.read_csv(carpeta + 'defunciones.csv')
establecimientos = pd.read_excel(carpeta + 'instituciones_de_salud.xlsx')
#%% CENSOS
def obtener_index_provincias(anio=0):
    celdas = ([],[])
    
    for (index_celdas, censo) in enumerate([censo2010, censo2022]):
        
        cosas_de_interes = censo.iloc[:, 1]
        
        for (index, celda) in enumerate(cosas_de_interes):
            if "AREA #" in str(celda):
                celdas[index_celdas].append(index)
    
    if (anio==2010): return celdas[0]
    if (anio==2022): return celdas[1]

def obtener_dataFrameProvincias(censo):
    provincias_filas = obtener_index_provincias(2010)
    provincias = []
    for i in provincias_filas:
        id_provincia = int(censo.iloc[i, 1].split()[2])
        nombre_provincia = censo.iloc[i, 2]
        
        if(nombre_provincia == 'Caba'): 
            provincias.append((id_provincia,'Ciudad Autónoma de Buenos Aires'))
        else:
            provincias.append((id_provincia,nombre_provincia))
    
    df_provincias = pd.DataFrame(data=provincias, columns=['id', 'nombre'])  # CORREGIDO
    return df_provincias

    
def recolectar_datos(censo, anio):
    if(anio == 2010):
        cobertura_filas = [17, 130, 239, 349, 453]
    else:
        cobertura_filas = [17, 130, 238]
    
    provincias_filas = obtener_index_provincias(anio)
    
    datos = {
        'anio': [],
        'provincia': [],
        'sexo': [],
        'edad': [],
        'cobertura_medica': [],
        'cantidad': []
    }

    # --- provincias ---
    provincias = []
    for i in provincias_filas:
        id_provincia = int(censo.iloc[i, 1].split()[2])
        provincias.append(id_provincia)


    # --- coberturas ---
    coberturas = []
    for i in cobertura_filas:
        coberturas.append(censo.iloc[i, 1])

    # --- cosas de interes ---
    df = censo.iloc[18:, 2:5].copy()
    df.columns = ['edad', 'varon', 'mujer']

    ix = 0
    provincia_idx = 0
    cobertura_idx = 0
    i = 0

    while True:

        fila = df.iloc[ix]

        # detectar total
        if str(fila['edad']).strip().lower() == "total":
            cobertura_idx += 1
            ix += 2
            continue

        # cambio de provincia
        if cobertura_idx == len(coberturas):
            cobertura_idx = 0
            provincia_idx += 1

            if provincia_idx >= len(provincias):
                break

            i += 1
            ix = provincias_filas[i] + 4
            continue

        provincia = provincias[provincia_idx]
        cobertura = coberturas[cobertura_idx]

        edad = fila['edad']
        varon = 0 if fila['varon'] == '-' else fila['varon']
        mujer = 0 if fila['mujer'] == '-' else fila['mujer']
        
        # Hombres GOD
        datos['anio'].append(anio)
        datos['provincia'].append(provincia)
        datos['sexo'].append("Varón")
        datos['edad'].append(edad)
        datos['cobertura_medica'].append(cobertura)
        datos['cantidad'].append(varon)
        
        # Muejeres ZZZ
        datos['anio'].append(anio)
        datos['provincia'].append(provincia)
        datos['sexo'].append("Mujer")
        datos['edad'].append(edad)
        datos['cobertura_medica'].append(cobertura)
        datos['cantidad'].append(mujer)

        ix += 1

    return pd.DataFrame(datos)

df2010 = recolectar_datos(censo2010, 2010)
df2022 = recolectar_datos(censo2022, 2022)

df_final = pd.concat([df2010, df2022], ignore_index=True)

# Reemplazar las posibles coberturas medicas con los otros.

df_final['cobertura_medica'] = df_final['cobertura_medica'].replace(
   {'Obra social (incluye PAMI)': 'Obra social o prepaga (incluye PAMI)', 
    'Prepaga a través de obra social': 'Obra social o prepaga (incluye PAMI)', 
    'Prepaga sólo por contratación voluntaria': 'Obra social o prepaga (incluye PAMI)'}
)
consulta = """
        SELECT anio, provincia, sexo, edad, cobertura_medica, sum(cantidad) as cantidad
        FROM df_final
        GROUP BY anio, provincia, sexo, edad, cobertura_medica
        ORDER BY anio, provincia, edad, cobertura_medica
"""

resultado = dd.query(consulta).df()
resultado.to_csv('Archivos_Propios/censo2010-2022.csv', index=False, encoding='utf-8')

df_provincias = obtener_dataFrameProvincias(censo2010)
df_provincias.to_csv('Archivos_Propios/provincias.csv', index=False, encoding='utf-8')

# %% LIMPIEZA DEL DATAFRAME 'ESTABLECIMIENTOS'

def limpieza_establecimientos():

    # ver que hacer con 'obra social' y 'otros'
    origenes_publicos = ['FFAA/Seguridad','Mixta','Municipal',
                         'Servicio Penitenciario Federal',
                         'Servicio Penitenciario Provincia',
                         'Universitario público']
    
    tienen_terapia_intensiva = ['Alto riesgo con terapia intensiva',
                                'Alto riesgo con terapia intensiva especializada']
    
    establecimientos_datos = {
                        'id': [],
                        'nombre': [],
                        'id_departamento': [],
                        'es_publico': [],
                        'terapia_intensiva': []
                        }
    
    ids_establecimientos = establecimientos['establecimiento_id'].tolist()
    nombres = establecimientos['establecimiento_nombre'].tolist()
    ids_departamentos = (establecimientos['provincia_id'].astype(str) + '_' + 
                        establecimientos['departamento_id'].astype(str)).tolist()
    
    establecimientos_datos['id'].extend(ids_establecimientos)
    establecimientos_datos['nombre'].extend(nombres)
    establecimientos_datos['id_departamento'].extend(ids_departamentos)
    
    i = 0
    while i < len(establecimientos):
        
        # veo si tiene origen público
        if establecimientos.loc[i, 'origen_financiamiento'] in origenes_publicos:
            establecimientos_datos['es_publico'].append(True)
        else:
            establecimientos_datos['es_publico'].append(False)
        
        # veo si tiene terapia intensiva
        if establecimientos.loc[i, 'tipologia_nombre'] in tienen_terapia_intensiva:
            establecimientos_datos['terapia_intensiva'].append(True)
        else:
            establecimientos_datos['terapia_intensiva'].append(False)
            
        i += 1
    
    return pd.DataFrame(establecimientos_datos)

df_establecimientos = limpieza_establecimientos()
df_establecimientos.to_csv('Archivos_Propios/establecimiento.csv', index= False, encoding='utf-8')

# %% CREACIÓN DEL DATAFRAME 'DEPARTAMENTOS'

def crear_departamento():
    consultaSQL = """
            SELECT DISTINCT 
                
                CONCAT(provincia_id, '_' , departamento_id) AS id, 
                provincia_id,
                departamento_nombre AS nombre
            
            FROM establecimientos
            GROUP BY id, nombre, provincia_id
            ORDER BY provincia_id, id;
            """
    return dd.query(consultaSQL).df()
    
df_departamentos = crear_departamento()
df_departamentos.to_csv('Archivos_Propios/departamentos.csv', index= False, encoding='utf-8')

#%% DEFUNCIONES
#Creacion del DataFrame principal de 'defunciones'
#
consulta = """
        SELECT 
            anio, 
            CASE 
                WHEN jurisdiccion_de_residencia_id = 98 THEN 99
                ELSE jurisdiccion_de_residencia_id
                END as provincia_id,
            cie10_causa_id AS codigo_defuncion, 
            Sexo AS sexo, 
            grupo_edad, 
            cantidad,
            
        FROM defunciones
            """
defunciones_tuneado = dd.query(consulta).df()


#Creacion del Dataframe 'clasificacion_de_defunciones'
consulta = """
        SELECT DISTINCT cie10_causa_id AS codigo_defuncion, cie10_clasificacion AS clasificacion_defuncion
        FROM defunciones
"""
clasificacion_de_defunciones = dd.query(consulta).df()

#Creacion del DataFrame 'provincias_defunciones'
consulta = """
        SELECT DISTINCT jurisdiccion_de_residencia_id AS id, jurisdicion_residencia_nombre AS nombre
        FROM defunciones
        WHERE id != 98
        ORDER BY id
"""
provincias_defunciones = dd.query(consulta).df()




#ARCHIVOS
defunciones_tuneado.to_csv('Archivos_Propios/defunciones.csv', index=False, encoding='utf-8')

clasificacion_de_defunciones.to_csv('Archivos_Propios/clasificacion_de_defunciones.csv', index=False, encoding='utf-8')

provincias_defunciones.to_csv('Archivos_Propios/provincias.csv', index=False, encoding='utf-8')
# %% INICIALIZACION DE DATAFRAMES:
nuestra_carpeta = 'Archivos_Propios/'
censos = pd.read_csv(nuestra_carpeta + 'censo2010-2022.csv')
defunciones = pd.read_csv(nuestra_carpeta + 'defunciones.csv')
clasificacion_de_defunciones = pd.read_csv(nuestra_carpeta + 'clasificacion_de_defunciones.csv')
departamentos = pd.read_csv(nuestra_carpeta + 'departamentos.csv')
establecimientos = pd.read_csv(nuestra_carpeta + 'establecimiento.csv')
provincias = pd.read_csv(nuestra_carpeta + 'provincias.csv')
# %% PUNTO 2: Establecimientos de salud con terapia intensiva

establecientos_con_terapia_intensiva = dd.query(
    """
        SELECT 
            provincias.nombre AS provincia, 
            IF(es_publico, 'estatal', 'privado') AS financiamiento,
            count(*) as cantidad,
        FROM establecimientos
        INNER JOIN departamentos 
            ON departamentos.id = establecimientos.id_departamento
        INNER JOIN provincias
            ON provincias.id = departamentos.provincia_id
        WHERE terapia_intensiva
        GROUP BY provincias.nombre, establecimientos.es_publico
        ORDER BY provincias.nombre, financiamiento
    """).df()

res = dd.query("""
        SELECT *
        FROM establecientos_con_terapia_intensiva
        WHERE provincia = 'Buenos Aires' OR provincia = 'Ciudad Autónoma de Buenos Aires' OR provincia = 'Santa Fe' 
        ORDER BY provincia 

""").df()
    
# %% PUNTO 5: Cambios en las causas de defunción
cantidad_defunciones_2010_2022 = dd.query(
    """
        SELECT 
            clasificacion_de_defunciones.clasificacion_defuncion,
            SUM(CASE 
                WHEN anio = 2010 THEN cantidad 
                ELSE 0 END
                ) AS def_2010,
            SUM(
                CASE WHEN anio = 2022 THEN cantidad 
                ELSE 0 END
                ) AS def_2022
        FROM defunciones
        INNER JOIN clasificacion_de_defunciones
            ON defunciones.codigo_defuncion = clasificacion_de_defunciones.codigo
        WHERE anio = 2010 OR anio = 2022
        GROUP BY clasificacion_de_defunciones.clasificacion_defuncion
    """).df()


diferencia_entre_2010_2022 = dd.query(
    """
        SELECT 
            clasificacion_defuncion,
            def_2010,
            def_2022,
            def_2022 - def_2010 AS diferencia
        FROM cantidad_defunciones_2010_2022
        ORDER BY diferencia DESC
    """).df()
