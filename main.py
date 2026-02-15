#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Feb  8 07:40:51 2026

@author: nicolas
"""

# %%

import numpy as np
import pandas as pd
import duckdb as dd
import matplotlib.pyplot as plt
from matplotlib import ticker
import seaborn as sns
#%%
carpeta = './Archivos-TP/' 
censo2010 = pd.read_excel(carpeta + 'censo2010.xlsX') 
censo2022 = pd.read_excel(carpeta + 'censo2022.xlsX')
defunciones = pd.read_csv(carpeta + 'defunciones.csv')
establecimientos = pd.read_excel(carpeta + 'instituciones_de_salud.xlsx')
clasificacion_defunciones = pd.read_csv(carpeta + 'categoriasDefunciones.csv')

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
    ids_departamentos = (establecimientos['provincia_id'].astype(str) + 
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
                
                CONCAT(provincia_id, departamento_id) AS id, 
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
#cambio los id de 98 a 99 (de null a 'Sin Informacion')
consulta = """
        SELECT 
            defunciones.anio, 
            CASE 
                WHEN jurisdiccion_de_residencia_id = 98 
                THEN 99
                ELSE jurisdiccion_de_residencia_id
                END as provincia_id,
            clasificacion_defunciones.categorias AS categoria_defuncion, 
            Sexo AS sexo, 
            grupo_edad, 
            cantidad,
        FROM defunciones
        INNER JOIN clasificacion_defunciones
        ON clasificacion_defunciones.codigo_def = defunciones.cie10_causa_id
        ORDER BY clasificacion_defunciones.codigo_def 
            """
defunciones_tuneado = dd.query(consulta).df()


#Creacion del Dataframe 'clasificacion_de_defunciones'

consulta = """
        SELECT DISTINCT cie10_causa_id AS codigo, cie10_clasificacion AS clasificacion
        FROM defunciones
        WHERE clasificacion IS NOT NULL
"""
clasificacion_de_defunciones = dd.query(consulta).df()

#Ahora voy a renombrar los nulls de defunciones por 'sin informacion' y su codigo por A00
#obtengo los codigos cuya clasificacion es null
consulta = """
        SELECT DISTINCT cie10_causa_id AS codigo
        FROM defunciones
        WHERE cie10_clasificacion IS NULL
"""
codigos_null = (dd.query(consulta).df())["codigo"]
dicc_nulls = {}

#creo el diccionario que se va a usar para reemplazar los codigos por A00
for codigo in codigos_null:
    dicc_nulls[codigo] = "A00"
    
defunciones_tuneado['categoria_defuncion'].replace(dicc_nulls,inplace=True)


#defunciones_tuneado.loc[defunciones_tuneado['codigo_defuncion'] == "A00",'clasificacion'] = "Sin Información"
nueva_fila = pd.DataFrame({'codigo':'A00','clasificacion':["Sin Información"]})
clasificacion_de_defunciones = pd.concat([clasificacion_de_defunciones,nueva_fila],ignore_index=True)


#Creacion del DataFrame 'provincias_defunciones'
#Ignoro el id 98 porque es null
consulta = """
        SELECT DISTINCT jurisdiccion_de_residencia_id AS id, jurisdicion_residencia_nombre AS nombre
        FROM defunciones
        WHERE id != 98
        ORDER BY id
"""
provincias_defunciones = dd.query(consulta).df()

#dejo los datos un poco mas lindos
def quitar_comillas(lista):
    res = lista.copy()

    for i in range(len(lista)):
        elem = lista[i]
        if elem[0] == '"' and elem[len(elem)] == '"':
            reemplazo = elem[1:len(elem)-1]
            res[i] = reemplazo

    return res

provincias_defunciones["nombre"] = quitar_comillas(provincias_defunciones["nombre"])
clasificacion_de_defunciones["clasificacion"] = quitar_comillas(clasificacion_de_defunciones['clasificacion'])

clasificacion_de_defunciones = dd.query(
    """
    SELECT * 
    FROM clasificacion_de_defunciones
    ORDER BY codigo
    """).df()


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

# %% PUNTO 1: Cobertura de salud

obras_sociales = ('Obra social o prepaga (incluye PAMI)', 
                 'Programas o planes estatales de salud')

cobertura_de_salud = dd.query(
        f"""
    WITH tabla_intermedia AS (
        SELECT p.nombre AS Provincia,
        CASE WHEN c.edad < 15 THEN '0 a 14'
             WHEN c.edad < 35 THEN '15 a 34'
             WHEN c.edad < 55 THEN '35 a 54'
             WHEN c.edad < 75 THEN '55 a 74'
             WHEN c.edad > 74 THEN '75 o más'
        END AS Rango_etario,
        c.anio AS Año,
        c.cantidad AS Cantidad,
        CASE WHEN c.cobertura_medica IN {obras_sociales} THEN 1
            ELSE 0 
        END AS Tiene_cobertura
        
        FROM censos AS c
        INNER JOIN provincias AS p
            ON c.provincia = p.id
    )
    
    SELECT Provincia,
    Rango_etario,
    SUM(CASE WHEN
        Año = 2010 AND tiene_cobertura = 1 THEN cantidad ELSE 0 END)
        AS Habitantes_con_cobertura_en_2010,
    SUM(CASE WHEN
        Año = 2010 AND tiene_cobertura = 0 THEN cantidad ELSE 0 END)
        AS Habitantes_sin_cobertura_en_2010,  
    SUM(CASE WHEN
        Año = 2022 AND tiene_cobertura = 1 THEN cantidad ELSE 0 END)
        AS Habitantes_con_cobertura_en_2022,
    SUM(CASE WHEN
        Año = 2022 AND tiene_cobertura = 0 THEN cantidad ELSE 0 END)
        AS Habitantes_sin_cobertura_en_2022
    
    FROM tabla_intermedia
    GROUP BY Provincia, Rango_etario
    ORDER BY Provincia, Rango_etario
    """).df()


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
            ON defunciones.codigo_defuncion = clasificacion_de_defunciones.codigo_defuncion
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

# %% VISUALIZACIÓN PUNTO 1: Cantidad de habitantes por provincia

habitantes_por_provincia = dd.query(
    """
        SELECT c.anio, p.nombre, SUM(c.cantidad) AS cantidad_habitantes
        FROM censos AS c
        INNER JOIN provincias AS p
            ON c.provincia = p.id
        GROUP BY c.anio, p.nombre
        ORDER BY cantidad_habitantes, p.nombre, c.anio
    """).df()

habitantes_por_provincia = habitantes_por_provincia.pivot(index='nombre', columns='anio', values='cantidad_habitantes')

fig, ax = plt.subplots()

x = habitantes_por_provincia['provincia']
datos_2010 = habitantes_por_provincia['anio'] == 2010

x = np.arange(len(habitantes_por_provincia.index))
habitantes_2010 = habitantes_por_provincia[2010]
habitantes_2022 = habitantes_por_provincia[2022]

width = 0.4

ax.bar(x - width/2, habitantes_2010, width=width, label='Habitantes 2010')
ax.bar(x + width/2, habitantes_2022, width=width, label='Habitantes 2022')

ax.set_title('Población Argentina por provincia 2010 vs 2022')
ax.set_xlabel('Provincias')
ax.set_xticks(x, labels=habitantes_por_provincia.index, rotation=45, ha='right')
ax.legend()

plt.show()
# %% VISUALIZACION PUNTO 2


cantidad_defunciones_por_tiempo = dd.query(
    """
        SELECT anio, categoria_defuncion, sum(cantidad) as cantidad
        FROM defunciones_tuneado
        GROUP BY anio, categoria_defuncion
        ORDER BY cantidad DESC
    """    
    
).df()
# %%
categorias_df = dd.query(
    """
        SELECT DISTINCT categoria_defuncion
        FROM cantidad_defunciones_por_tiempo
    """    
).df()
n_categorias = len(categorias.index.tolist())
mitad = n_categorias // 2
categorias_lista = categorias_df['categoria_defuncion'].tolist()
primer_grupo = categorias_lista[:mitad]
segundo_grupo = categorias_lista[mitad:]
# %%
# PRIMER GÁFICO
fig1, ax1 = plt.subplots(figsize=(20, 8))
sns.lineplot(data=cantidad_defunciones_por_tiempo[cantidad_defunciones_por_tiempo['categoria_defuncion'].isin(primer_grupo)], 
             x='anio', y='cantidad', hue='categoria_defuncion', marker='o', ax=ax1)
ax1.set_xlabel('Año', fontsize=12)
ax1.set_ylabel('Cantidad de defunciones', fontsize=12)
ax1.set_title(f'Categorías 1 a {mitad} (mayor cantidad)', fontsize=14, fontweight='bold')
ax1.legend(title='Categoría', bbox_to_anchor=(1.05, 1), loc='upper left')
plt.tight_layout()
plt.show()

# SEGUNDO GRÁFICO
fig2, ax2 = plt.subplots(figsize=(20, 8))
sns.lineplot(data=cantidad_defunciones_por_tiempo[cantidad_defunciones_por_tiempo['categoria_defuncion'].isin(segundo_grupo)], 
             x='anio', y='cantidad', hue='categoria_defuncion', marker='o', ax=ax2)
ax2.set_xlabel('Año', fontsize=12)
ax2.set_ylabel('Cantidad de defunciones', fontsize=12)
ax2.set_title(f'Categorías {mitad+1} a {len(categorias)} (menor cantidad)', fontsize=14, fontweight='bold')
ax2.legend(title='Categoría', bbox_to_anchor=(1.05, 1), loc='upper left')
plt.tight_layout()
plt.show()
