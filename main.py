#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Feb  8 07:40:51 2026

@author: nicolas
"""

# %%

import pandas as pd
import duckdb as dd

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

    return celdas

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
    ids_departamentos = establecimientos['departamento_id'].tolist()
    
    establecimientos_datos['id'].extend(ids_establecimientos)
    establecimientos_datos['nombre'].extend(nombres)
    establecimientos_datos['id_departamento'].extend(ids_departamentos)
    
    i = 0
    while i < len(establecimientos):
        
        # veo si tiene origen público
        if establecimientos.loc[i, 'origen_financiamiento'] in origenes_publicos:
            establecimientos_datos['es_publico'].append('SI')
        else:
            establecimientos_datos['es_publico'].append('NO')
        
        # veo si tiene terapia intensiva
        if establecimientos.loc[i, 'tipologia_nombre'] in 'tienen_terapia_intensiva':
            establecimientos_datos['terapia_intensiva'].append('SI')
        else:
            establecimientos_datos['terapia_intensiva'].append('NO')
            
        i += 1
    
    return pd.DataFrame(establecimientos_datos)

df_establecimientos = limpieza_establecimientos()
    

# %% 
