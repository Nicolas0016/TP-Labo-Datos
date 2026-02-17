#%% Importar librerias y archivos
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import duckdb as dd

# INICIALIZACION DE DATAFRAMES:
nuestra_carpeta = 'Archivos_Propios/'
censos = pd.read_csv(nuestra_carpeta + 'censo2010-2022.csv')
defunciones = pd.read_csv(nuestra_carpeta + 'defunciones.csv')
departamentos = pd.read_csv(nuestra_carpeta + 'departamentos.csv')
establecimientos = pd.read_csv(nuestra_carpeta + 'establecimiento.csv')
provincias = pd.read_csv(nuestra_carpeta + 'provincias.csv')

# %% PUNTO 1: Cobertura de salud

obras_sociales = ('Obra social o prepaga (incluye PAMI)', 
                 'Programas o planes estatales de salud')

tabla_intermedia = dd.query(
    f"""
        SELECT 
            p.nombre AS Provincia,
            CASE WHEN c.edad < 15 THEN '0 a 14'
                 WHEN c.edad < 35 THEN '15 a 34'
                 WHEN c.edad < 55 THEN '35 a 54'
                 WHEN c.edad < 75 THEN '55 a 74'
                 WHEN c.edad > 74 THEN '75 o más'
            END AS Rango_etario,
            c.anio AS Año,
            c.cantidad AS Cantidad,
            CASE WHEN c.cobertura_medica IN {obras_sociales} 
                THEN 1
                ELSE 0 
            END AS Tiene_cobertura

        FROM censos AS c
        INNER JOIN provincias AS p
            ON c.provincia = p.id                            
    """).df()
    
cobertura_de_salud = dd.query(
    """
        SELECT 
        Provincia,
        Rango_etario,
        SUM(CASE WHEN (Año = 2010 AND tiene_cobertura = 1) 
            THEN cantidad 
            ELSE 0 END)
            AS Habitantes_con_cobertura_en_2010,
        SUM(CASE WHEN (Año = 2010 AND tiene_cobertura = 0) 
            THEN cantidad 
            ELSE 0 END)
            AS Habitantes_sin_cobertura_en_2010,  
        SUM(CASE WHEN (Año = 2022 AND tiene_cobertura = 1)
            THEN cantidad 
            ELSE 0 END)
            AS Habitantes_con_cobertura_en_2022,
        SUM(CASE WHEN Año = 2022 AND tiene_cobertura = 0 
            THEN cantidad 
            ELSE 0 END)
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

    


# %% PUNTO 3 CAUSAS MUERTE

# Agrupo la cantidad de funciones dependiendo su categoria, edad y sexo
defunciones_agrupadas = dd.query(
    """
        SELECT categorias, grupo_edad, sexo, SUM(cantidad) as total
        FROM defunciones
        WHERE sexo IN ('masculino', 'femenino') AND categorias != 'Sin Información'
        GROUP BY categorias, grupo_edad, sexo
    """    
).df()
# Busco el top 5 defunciones más comunes
defunciones_mas_frecuentes = dd.query("""
    SELECT d1.grupo_edad, d1.sexo, d1.categorias, d1.total
    FROM jutanda_defunciones d1
    WHERE  5 >= ( 
        SELECT COUNT(*) 
        FROM jutanda_defunciones d2 
        WHERE d2.grupo_edad = d1.grupo_edad 
          AND d2.sexo = d1.sexo 
          AND d2.total >= d1.total
    ) 
    ORDER BY d1.grupo_edad, d1.sexo, d1.total DESC
""").df()

# Busco el top 5 defunciones menos comunes
defunciones_menos_frecuentes = dd.query(
    """
        SELECT d1.grupo_edad, d1.sexo, d1.categorias, d1.total
        FROM jutanda_defunciones d1
        WHERE  5 >= ( 
            SELECT COUNT(*) 
            FROM jutanda_defunciones d2 
            WHERE d2.grupo_edad = d1.grupo_edad 
              AND d2.sexo = d1.sexo 
              AND d2.total <= d1.total
        ) 
        ORDER BY d1.grupo_edad, d1.sexo, d1.total ASC
    """    
).df()

extremos_def_grupo = dd.query(
    """
        SELECT *
        FROM defunciones_mas_frecuentes
        UNION ALL
        SELECT *
        FROM defunciones_menos_frecuentes
        ORDER BY grupo_edad, sexo, total ASC
    """    
).df()

#%% PUNTO 4: TASA DE MORTALIDAD POR PROVINCIA

muertes_totales = dd.query(
    """
        SELECT p.nombre AS provincia, d.grupo_edad,sum(d.cantidad) AS muertes
        FROM defunciones d
        LEFT OUTER JOIN provincias p
        ON d.provincia_id = p.id
        WHERE anio = 2022
        GROUP BY grupo_edad, provincia        
        ORDER BY grupo_edad,muertes DESC
        
    """).df()

#Ahora calculo la cantidad de habitantes por provincia y grupo etario
habitantes_por_provincia = dd.query(
    """
        SELECT p.nombre AS provincia, CASE 
            WHEN c.edad >= 0 AND c.edad <= 14 THEN '0-14'
            WHEN c.edad >= 15 AND c.edad <= 34 THEN '15-34'
            WHEN c.edad >= 35 AND c.edad <= 54 THEN '35-54'
            WHEN c.edad >= 55 AND c.edad <= 74 THEN '55-74'
            ELSE '75 o mas'
            END AS grupo_edad, 
        sum(c.cantidad) AS habitantes
        FROM censos c
        LEFT OUTER JOIN provincias p
            ON c.provincia = p.id
        WHERE c.anio = 2022
        GROUP BY p.nombre,grupo_edad
        ORDER BY grupo_edad, habitantes DESC
""").df()

#Finalmente calculo la tasa de mortalidad
#AVISO: estamos ignorando los datos en los que no sabemos el grupo etario
tasa_de_mortalidad = dd.query("""
        SELECT h.provincia, h.grupo_edad, ROUND((m.muertes/h.habitantes)*1000,2) AS tasa
        FROM habitantes_por_provincia h
        LEFT OUTER JOIN muertes_totales m                              
            ON h.grupo_edad = m.grupo_edad AND h.provincia = m.provincia                              
        ORDER BY h.grupo_edad,tasa
""").df()

# %% PUNTO 5: Cambios en las causas de defunción
cantidad_defunciones_2010_2022 = dd.query(
    """
        SELECT 
            categorias,
            SUM(CASE 
                WHEN anio = 2010 THEN cantidad 
                ELSE 0 END
                ) AS def_2010,
            SUM(
                CASE WHEN anio = 2022 THEN cantidad 
                ELSE 0 END
                ) AS def_2022
        FROM defunciones
        WHERE categorias != 'Sin Información'
        GROUP BY categorias
    """).df()


diferencia_entre_2010_2022 = dd.query(
    """
        SELECT 
            *,
            def_2022 - def_2010 AS diferencia
        FROM cantidad_defunciones_2010_2022
        ORDER BY diferencia DESC
    """).df()
