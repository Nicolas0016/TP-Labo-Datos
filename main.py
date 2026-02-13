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
#%%
def recolectar_datos(censo, anio):
    if(anio == 2010):
        provincias_filas = [14, 674, 1341, 1961, 2608, 3237, 3851,
                            4459, 5084, 5689, 6305, 6910, 7512, 8144,
                            8777, 9402, 10023, 10653, 11267, 11878,
                            12471, 13123, 13764, 14399]

        cobertura_filas = [17, 130, 239, 349, 453]
    else:
        provincias_filas = [14, 461, 913, 1343, 1791, 2240, 2685,
                            3107, 3550, 3984, 4424, 4851, 5288, 5727,
                            6172, 6605, 7047, 7494, 7928, 8359,
                            8779, 9230, 9676, 10122]

        cobertura_filas = [17, 130, 238]
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
        nombre_provincia = censo.iloc[i, 2]
        if(nombre_provincia == 'Caba'): 
            provincias.append('Ciudad Autónoma de Buenos Aires')
        else:
            provincias.append(nombre_provincia)


    # --- coberturas ---
    coberturas = []
    for i in cobertura_filas:
        coberturas.append(censo.iloc[i, 1])

    # --- datos principales ---
    df = censo.iloc[:, 2:5].copy()
    df.columns = ['edad', 'varon', 'mujer']

    ix = 18
    provincia_idx = 0
    cobertura_idx = 0
    i = 0

    while ix < len(df):

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

        datos['anio'].append(anio)
        datos['provincia'].append(provincia)
        datos['sexo'].append("Varón")
        datos['edad'].append(edad)
        datos['cobertura_medica'].append(cobertura)
        datos['cantidad'].append(varon)

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
df_final

