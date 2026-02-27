#%% Importar librerias y archivos
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# GRUPO CRUD: Nicolás Argañaraz, Axel Gabriel Frontera Castillo y Emiliano Rojas.
# En este archivo se encuentra todo lo correspondiente a la visualización de los datos.

import numpy as np
import pandas as pd
import duckdb as dd
import matplotlib.pyplot as plt
import seaborn as sns

nuestra_carpeta = 'Archivos_Propios/'
censos = pd.read_csv(nuestra_carpeta + 'censo2010-2022.csv')
defunciones = pd.read_csv(nuestra_carpeta + 'defunciones.csv')
departamentos = pd.read_csv(nuestra_carpeta + 'departamentos.csv')
establecimientos = pd.read_csv(nuestra_carpeta + 'establecimiento.csv')
provincias = pd.read_csv(nuestra_carpeta + 'provincias.csv')
#%% VISUALIZACION PUNTO 1 - Cantidad de habitantes por provincia
habitantes_por_provincia = dd.query(
    """
        SELECT c.anio, p.nombre, SUM(c.cantidad) AS cantidad_habitantes
        FROM censos AS c
        INNER JOIN provincias AS p
            ON c.provincia = p.id
        GROUP BY c.anio, p.nombre
        ORDER BY c.anio, cantidad_habitantes 
    """).df()
habitantes_por_provincia = habitantes_por_provincia.pivot(index='nombre', columns='anio', values='cantidad_habitantes')

habitantes_por_provincia = habitantes_por_provincia.sort_values(2022, ascending=True)
habitantes_por_provincia.head()
fig, ax = plt.subplots(figsize=(7, 8))


y = np.arange(len(habitantes_por_provincia.index))
habitantes_2010 = habitantes_por_provincia[2010]
habitantes_2022 = habitantes_por_provincia[2022]
habitantes_2010 = habitantes_2010 / 1_000_000
habitantes_2022 = habitantes_2022 / 1_000_000

width = 0.4
ax.barh(y - width/2, habitantes_2010, height=width, label='Habitantes 2010')
ax.barh(y + width/2, habitantes_2022, height=width, label='Habitantes 2022')

ax.set_title('Población Argentina por provincia 2010 vs 2022')
ax.set_ylabel('Provincias')
ax.set_xlabel('Cantidad de habitantes (en millones)')
ax.set_yticks(y, labels=habitantes_por_provincia.index, ha='right')
ax.legend()
plt.figtext(.5, 0, "FIGURA 1",fontweight="bold")
ax.grid(axis='x', alpha=0.3)
plt.tight_layout(rect=[0, 0.03, 1, 0.98])
plt.show()

# %% VISUALIZACION PUNTO 2 -     

def percentil(datos, percentil):
    datos_ordenados = sorted(datos)
    posicion = (len(datos_ordenados) - 1) * (percentil / 100)
    i = int(posicion)  # posicion entera
        
    return datos_ordenados[i] + (posicion-i) * (datos_ordenados[i + 1] - datos_ordenados[i])

def calcular_percentiles_de_corte(medianas_df):
    valores = medianas_df['mediana'].tolist()
    
    q1 = percentil(valores, 25)
    q2 = percentil(valores, 50)
    q3 = percentil(valores, 75)
    
    return (
        int(round(q1)),
        int(round(q2)),
        int(round(q3))
    )

cantidad_defunciones_por_tiempo = dd.query(
    """
        SELECT anio, categorias, sum(cantidad) as cantidad
        FROM defunciones
        WHERE categorias <> 'Sin Información'
        GROUP BY anio, categorias
        ORDER BY cantidad DESC
    """    
).df()

categorias_df = dd.query(
    """
        SELECT DISTINCT categorias
        FROM cantidad_defunciones_por_tiempo
    """    
).df()

medianas_de_categorias = []
for _, row in categorias_df.iterrows():
    categoria = row['categorias']
    df = dd.query(
        f"""
            SELECT sum(cantidad) as cantidad
            FROM defunciones
            WHERE categorias = '{categoria}'
            GROUP BY anio
        """
    ).df()
    mediana = df['cantidad'].median()
    medianas_de_categorias.append({'categoria': categoria, 'mediana': mediana})

medianas_df = pd.DataFrame(medianas_de_categorias)

(corte_grupo1,corte_grupo2,corte_grupo3) = calcular_percentiles_de_corte(medianas_df)

grupo1 = medianas_df[medianas_df['mediana'] <= corte_grupo1]['categoria'].tolist()  
grupo2 = medianas_df[(medianas_df['mediana'] > corte_grupo1) & (medianas_df['mediana'] <= corte_grupo2)]['categoria'].tolist()
grupo3 = medianas_df[(medianas_df['mediana'] > corte_grupo2) & (medianas_df['mediana'] <= corte_grupo3)]['categoria'].tolist()
grupo4 = medianas_df[medianas_df['mediana'] > corte_grupo3]['categoria'].tolist()

def graficar_grupo(grupo, titulo):
    
    datos_grupo = cantidad_defunciones_por_tiempo[cantidad_defunciones_por_tiempo['categorias'].isin(grupo)]
    
    fig, ax = plt.subplots(figsize=(15, 8))
    
    sns.lineplot(data=datos_grupo, 
                 x='anio', y='cantidad', 
                 hue='categorias', 
                 marker='o', legend=False)
    
    años_unicos = sorted(datos_grupo['anio'])
    
    ax.set_xticks(años_unicos)
    ax.set_xticklabels(años_unicos)
    
    ax.set_xlabel('Año', fontsize=12)
    ax.set_ylabel('Cantidad de defunciones', fontsize=12)
    
    ax.set_title(f'{titulo}', fontsize=14, fontweight='bold')
    plt.figtext(.5, 0, f"FIGURA {int(titulo.split(' ')[1]) + 1}",fontweight="bold")
    plt.show()

graficar_grupo(grupo1, "GRUPO 1")
graficar_grupo(grupo2, "GRUPO 2")
graficar_grupo(grupo3, "GRUPO 3")
graficar_grupo(grupo4, "GRUPO 4")


# %% VISUALIZACION PUNTO 3 - Tasa de mortalidad por provincia y gráfico a elección
#Parte 1: Tasa de mortalidad por provincia
muertes_totales_vis = dd.query(
    """
        SELECT p.nombre AS provincia, sum(d.cantidad) AS muertes
        FROM defunciones d
        LEFT OUTER JOIN provincias p
        ON d.provincia_id = p.id
        WHERE anio = 2022
        GROUP BY provincia        
        ORDER BY muertes DESC
        
    """).df()

#calculo la cantidad de habitantes por provincia
habitantes_por_provincia_vis = dd.query(
    """
        SELECT p.nombre AS provincia, sum(c.cantidad) AS habitantes
        FROM censos c
        LEFT OUTER JOIN provincias p
            ON c.provincia = p.id
        WHERE c.anio = 2022
        GROUP BY p.nombre
        ORDER BY habitantes DESC
""").df()

#calculo la tasa de mortalidad
tasa_de_mortalidad_vis = dd.query("""
        SELECT h.provincia, ROUND((m.muertes/h.habitantes)*10000,2) AS tasa
        FROM habitantes_por_provincia_vis h
        LEFT OUTER JOIN muertes_totales_vis m                              
            ON h.provincia = m.provincia                              
        ORDER BY tasa DESC
""").df()


#Parte 2: Tasa de mortalidad por accidentes y causas externas

muertes_totales_vis2 = dd.query(
    """
        SELECT p.nombre AS provincia, sum(d.cantidad) AS muertes, sexo
        FROM defunciones d
        LEFT OUTER JOIN provincias p
        ON d.provincia_id = p.id
        WHERE anio = 2022
        AND d.categorias = 'Accidentes y causas externas'
        GROUP BY provincia, sexo        
        ORDER BY muertes DESC
        
    """).df()

#calculo la cantidad de habitantes por provincia
habitantes_por_provincia_vis2 = dd.query(
    """
        SELECT p.nombre AS provincia, sum(c.cantidad) AS habitantes, 
        CASE
            WHEN c.sexo = 'Varón' THEN 'masculino'
            WHEN c.sexo = 'Mujer' THEN 'femenino'
            ELSE 'desconocido'
        END AS sexo
        FROM censos c
        LEFT OUTER JOIN provincias p
            ON c.provincia = p.id
        WHERE c.anio = 2022
        GROUP BY p.nombre, sexo
        ORDER BY habitantes DESC
""").df()

#calculo la tasa de mortalidad
tasa_de_mortalidad_vis2 = dd.query("""
        SELECT h.provincia, ROUND((m.muertes/h.habitantes)*10000,2) AS tasa, m.sexo
        FROM habitantes_por_provincia_vis2 h
        LEFT OUTER JOIN muertes_totales_vis2 m                              
            ON h.provincia = m.provincia AND h.sexo = m.sexo                              
        ORDER BY tasa ASC
""").df()

#----------------------
#GRAFICO 1
fig, ax = plt.subplots(figsize=(10,9))
ax.barh(data=tasa_de_mortalidad_vis, y='provincia', width='tasa')

ax.set_title('Tasa de mortalidad por provincia')
ax.set_xlabel('Muertes (cada 10000 habitantes)', fontsize='medium')                       
ax.set_ylabel('Provincia', fontsize='medium')

#achico los nombres de las provincias
ax.tick_params(axis = 'y',labelsize = 8)

plt.figtext(.5, 0, "FIGURA 6",fontweight="bold")

#GRAFICO 2
fig, ax = plt.subplots(figsize = (8,6))
sns.barplot(data = tasa_de_mortalidad_vis2, y='provincia', x = 'tasa',hue = 'sexo',orient='h',ax=ax,
            palette={'femenino':'#e851cc', 'masculino':'#026cb8'})


ax.set_title('Tasa de mortalidad por accidentes y causas externas')
ax.set_xlabel('Muertes (cada 10000 habitantes)', fontsize='medium')                       
ax.set_ylabel('Provincia', fontsize='medium')
#achico los nombres de las provincias
ax.tick_params(axis = 'y',labelsize = 8)
ax.legend(title = 'Sexo')
plt.figtext(.40, -0, "FIGURA 7",fontweight="bold") 

# %% PUNTO 4 - Defunciones por grupo etario y sexo en 2022

#voy a ignorar los sexos que son desconocidos porque son muy pocos
#tambien voy a ignorar los grupos etarios desconocidos
muertes_totales_grupo_etario = dd.query(
    """
        SELECT grupo_edad, sexo, sum(d.cantidad) AS muertes
        FROM defunciones d
        WHERE anio = 2022 
            AND sexo != 'desconocido'
            AND grupo_edad != 'Sin Información' 
        GROUP BY grupo_edad, sexo        
        ORDER BY grupo_edad, sexo,muertes DESC
        
    """).df()

#las defunciones las normalizo a muerte cada 1000 habitantes de ese grupo etario
defuncion_por_grupo_etario_normalizado_por_grupo = dd.query(
    """
        SELECT c2.grupo_edad, c2.sexo,(m.muertes/sum(c2.cantidad))*1000 AS defunciones 
        FROM(SELECT
            CASE 
                WHEN c.edad >= 0 AND c.edad <= 14 THEN '0-14'
                WHEN c.edad >= 15 AND c.edad <= 34 THEN '15-34'
                WHEN c.edad >= 35 AND c.edad <= 54 THEN '35-54'
                WHEN c.edad >= 55 AND c.edad <= 74 THEN '55-74'
                ELSE '75 o mas'
                END AS grupo_edad,
            CASE
                WHEN c.sexo = 'Varón' THEN 'masculino'
                WHEN c.sexo = 'Mujer' THEN 'femenino'
                ELSE c.sexo
            END AS sexo,
            c.cantidad
            FROM censos c
            WHERE c.anio = 2022
        ) AS c2
        
        LEFT OUTER JOIN muertes_totales_grupo_etario m
            ON m.grupo_edad = c2.grupo_edad AND m.sexo = c2.sexo
        
        GROUP BY c2.grupo_edad,c2.sexo,muertes
        ORDER BY c2.grupo_edad,c2.sexo,muertes
""").df()

#las defunciones las normalizo a muerte cada 1000 habitantes de TODA la poblacion
defuncion_por_grupo_etario_normalizado_total = dd.query(
    """
        SELECT c2.grupo_edad, c2.sexo,(m.muertes/(SELECT sum(cantidad) FROM censos))*1000 AS defunciones 
        FROM(SELECT
            CASE 
                WHEN c.edad >= 0 AND c.edad <= 14 THEN '0-14'
                WHEN c.edad >= 15 AND c.edad <= 34 THEN '15-34'
                WHEN c.edad >= 35 AND c.edad <= 54 THEN '35-54'
                WHEN c.edad >= 55 AND c.edad <= 74 THEN '55-74'
                ELSE '75 o mas'
                END AS grupo_edad,
            CASE
                WHEN c.sexo = 'Varón' THEN 'masculino'
                WHEN c.sexo = 'Mujer' THEN 'femenino'
                ELSE c.sexo
            END AS sexo,
            c.cantidad
            FROM censos c
            WHERE c.anio = 2022
        ) AS c2
        
        LEFT OUTER JOIN muertes_totales_grupo_etario m
            ON m.grupo_edad = c2.grupo_edad AND m.sexo = c2.sexo
        
        GROUP BY c2.grupo_edad,c2.sexo,muertes
        ORDER BY c2.grupo_edad,c2.sexo,muertes
""").df()

#grafico normalizado por grupo etario
fig, ax = plt.subplots(1, 2, figsize=(14, 5))

sns.barplot(data = defuncion_por_grupo_etario_normalizado_por_grupo, y='grupo_edad', x = 'defunciones',hue = 'sexo',orient='h',ax=ax[0],
            palette={'femenino':'#e851cc', 'masculino':'#026cb8'})


ax[0].set_title('Defunciones por grupo etario y sexo en 2022')
ax[0].set_xlabel('muertes (cada 1000 habitantes de grupo etario)', fontsize='medium')                       
ax[0].set_ylabel('Grupo etario', fontsize='medium')

#achico los nombres de las provincias
ax[0].tick_params(axis = 'y',labelsize = 8)
ax[0].legend(title = 'Sexo')

#grafico normalizado por población total
sns.barplot(data = defuncion_por_grupo_etario_normalizado_total, y='grupo_edad', x = 'defunciones',hue = 'sexo',orient='h',ax=ax[1],
            palette={'femenino':'#e851cc', 'masculino':'#026cb8'})


ax[1].set_title('Defunciones por grupo etario y sexo en 2022')
ax[1].set_xlabel('muertes (cada 1000 habitantes totales)', fontsize='medium')                       
ax[1].set_ylabel('Grupo etario', fontsize='medium')

#achico los nombres de las provincias
ax[1].tick_params(axis = 'y',labelsize = 8)
ax[1].legend(title = 'Sexo')

plt.suptitle('Comparativa de Defunciones en Argentina (2022)', fontsize=14, fontweight='bold')
plt.figtext(0.5, -0.03, "Comparativa de tasas por grupo etario vs. total poblacional", 
            ha="center", fontweight="bold", fontsize=12)

plt.tight_layout(rect=[0, 0.03, 1, 0.95])
plt.figtext(.5, -.1, "FIGURA 8",fontweight="bold")

plt.show()

# %% VISUALIZACION PUNTO 5 - Distribucion de departamentos de salud

establecimientos_por_depto = dd.query("""
    SELECT id_departamento, COUNT(*) as cantidad
    FROM establecimientos
    GROUP BY id_departamento
    ORDER BY id_departamento
""").df()

establecimientos_por_departamento = dd.query("""
    SELECT  e.id_departamento, d.nombre,p.nombre as provincia,e.cantidad
    FROM establecimientos_por_depto AS e
    INNER JOIN departamentos as d
    ON e.id_departamento = d.id
    INNER JOIN provincias AS p
    ON d.provincia_id = p.id
    ORDER BY d.provincia_id, e.cantidad DESC
""").df()


grupo_axel1 = ['CABA','Formosa','La Rioja', 'Catamarca','Santa Cruz','Buenos Aires', 'Tucumán', 'Neuquén','Córdoba', 'Entre Ríos','Santa Fe', 'Río negro']
grupo_axel2 = ['Jujuy', 'Misiones', 'Chubut', 'La Pampa','Corrientes', 'San Luis', 'Chaco', 'Santiago del Estero','San Juan', 'Salta', 'Mendoza', 'Tierra del Fuego']

for i, grupo in enumerate([grupo_axel1,grupo_axel2], 1):
    # Filtrar datos para este grupo
    datos_grupo = establecimientos_por_departamento[
        establecimientos_por_departamento['provincia'].isin(grupo)
    ]
    
    plt.figure(figsize=(8.27, 6.27))

    
    sns.boxplot(data=datos_grupo, 
                x='provincia', 
                y='cantidad',
                gap=0,
                showfliers = False) #IGNORO LOS OUTLIERS (los puntos alejados)
    
    plt.xlabel('Provincia')
    if(i==0):
        plt.ylabel('Cantidad de establecimientos por departamento')
    plt.xticks(rotation=45, ha='right')
    
    plt.figtext(.43, -.1, f"FIGURA {i + 8}",fontweight="bold")
    plt.tight_layout()
    plt.show()


# %% VISUALIZACION PUNTO 6 - Grafico a eleccion

establecimientos_por_provincia = dd.query(
    """
        SELECT p.nombre AS provincia, COUNT(*) AS cant_establecimientos
        FROM establecimientos e
        INNER JOIN departamentos d ON e.id_departamento = d.id
        INNER JOIN provincias p ON d.provincia_id = p.id
        GROUP BY p.nombre
        ORDER BY cant_establecimientos DESC
    """).df()
    
tasas_por_provincia = dd.query(
    """
        SELECT m.provincia, m.tasa AS tasa_mortalidad,
            ROUND((e.cant_establecimientos/h.habitantes)*10000,2) AS tasa_establecimientos
        FROM tasa_de_mortalidad_vis m
        INNER JOIN establecimientos_por_provincia e
            ON m.provincia = e.provincia
        INNER JOIN habitantes_por_provincia_vis h
            ON m.provincia = h.provincia
    """).df()


# agrego columna 'region'

regiones = dict(zip(
    ['CABA', 'Buenos Aires', 'Córdoba',
     'Entre Ríos', 'Santa Fe', 'La Pampa', 'Mendoza', 'San Juan',
     'San Luis', 'Corrientes', 'Chaco', 'Formosa', 'Misiones',
     'Catamarca', 'Jujuy', 'La Rioja', 'Salta', 'Santiago del Estero',
     'Tucumán', 'Chubut', 'Neuquén', 'Río negro', 'Santa Cruz',
     'Tierra del Fuego'],
    ['Pampeana', 'Pampeana', 'Pampeana', 'Pampeana', 'Pampeana', 'Pampeana',
     'Cuyo', 'Cuyo', 'Cuyo', 'Noreste (NEA)', 'Noreste (NEA)',
     'Noreste (NEA)', 'Noreste (NEA)', 'Noroeste (NOA)',
     'Noroeste (NOA)', 'Noroeste (NOA)', 'Noroeste (NOA)',
     'Noroeste (NOA)', 'Noroeste (NOA)', 'Patagonia',
     'Patagonia', 'Patagonia', 'Patagonia', 'Patagonia']))

tasas_por_provincia['Regiones'] = tasas_por_provincia['provincia'].map(regiones)

fig, ax = plt.subplots(figsize=(10.5,6))

sns.scatterplot(data=tasas_por_provincia,
           x='tasa_establecimientos',
           y='tasa_mortalidad',
           hue='Regiones',
           s=170,
           ax=ax)

indices_a_etiquetar = []

ax.legend(title='Región')

# busco los índices de las provincias límites de cada región
for region, datos_grupo in tasas_por_provincia.groupby('Regiones'):
    id_max = datos_grupo['tasa_establecimientos'].idxmax()
    id_min = datos_grupo['tasa_establecimientos'].idxmin()
    indices_a_etiquetar.append(id_max)
    indices_a_etiquetar.append(id_min)  

df_etiquetas = tasas_por_provincia.loc[indices_a_etiquetar]

for i, row in df_etiquetas.iterrows():
    ax.text(x = row['tasa_establecimientos'] - 1.2,
            y = row['tasa_mortalidad'] - 0.3,             
            s = row['provincia'],                   
            fontsize = 12)  

ax.set_title('Relación entre la oferta sanitaria Argentina y la mortalidad general (2022)', fontsize = 'xx-large')
ax.set_xlabel('Establecimientos de salud (cada 10.000 hab.)', fontsize = 'x-large')
ax.set_ylabel('Tasa de mortalidad (%)', fontsize = 'x-large')
plt.figtext(.50, -0, "FIGURA 11",fontweight="bold")                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         
plt.show()

