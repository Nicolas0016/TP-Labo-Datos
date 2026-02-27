#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# GRUPO CRUD: Nicolás Argañaraz, Axel Gabriel Frontera Castillo y Emiliano Rojas.
# En este archivo se encuentra todo lo correspondiente a la limpieza de los datos.

import pandas as pd
import duckdb as dd

carpeta = './Archivos-TP/' 
censo2010 = pd.read_excel(carpeta + 'censo2010.xlsX') 
censo2022 = pd.read_excel(carpeta + 'censo2022.xlsX')
defunciones = pd.read_csv(carpeta + 'defunciones.csv')
establecimientos = pd.read_excel(carpeta + 'instituciones_de_salud.xlsx')
clasificacion_defunciones = pd.read_csv(carpeta + 'categoriasDefunciones.csv')
clasificacion_defunciones = clasificacion_defunciones.iloc[:, 1:]

#%% CENSOS

def recolectar_datos(censo, anio):
    if(anio == 2010):
        provincias_filas = [14, 674, 1341, 1961, 2608, 3237, 3851,
                            4459, 5084, 5689, 6305, 6910, 7512, 8144,
                            8777, 9402, 10023, 10653, 11267, 11878,
                            12471, 13123, 13764, 14399]

        cobertura_filas = [17, 130, 239, 349, 453]
    else:
        provincias_filas = [14, 461, 913, 1343, 1791, 2240, 2683,
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
        id_provincia = int(censo.iloc[i, 1].split()[2])
        provincias.append(id_provincia)


    # --- coberturas ---
    coberturas = []
    for i in cobertura_filas:
        coberturas.append(censo.iloc[i, 1])

    # --- datos principales ---
    df = censo.iloc[:, 2:5].copy()
    
    if(anio == 2010):
        df.columns = ['edad', 'varon', 'mujer']
    else: 
        df.columns = ['edad', 'mujer', 'varon']
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

df_censos = pd.concat([df2010, df2022], ignore_index=True)
df_censos.to_csv('borrar/prubve.csv', index=False, encoding='utf-8')

# como no nos interesa todas lascoberturas médicas, reemplazamo por las interes de estudio.
df_censos['cobertura_medica'] = df_censos['cobertura_medica'].replace(
   {'Obra social (incluye PAMI)': 'Obra social o prepaga (incluye PAMI)', 
    'Prepaga a través de obra social': 'Obra social o prepaga (incluye PAMI)', 
    'Prepaga sólo por contratación voluntaria': 'Obra social o prepaga (incluye PAMI)',
    'Programas o planes estatales de salud':'Obra social o prepaga (incluye PAMI)',
    'No tiene obra social, prepaga ni plan estatal':'No tiene obra social, prepaga o plan estatal'}
)
df_censos.to_csv('Archivos_Propios/censo2010-2022.csv', index=False, encoding='utf-8')
#%% Provincias

def obtener_dataFrameProvincias(censo):
    """
    A partir de censos creamos una tabla con id_provincia -> nombre
    """
    provincias_filas = [14, 674, 1341, 1961, 2608, 3237, 3851,
                        4459, 5084, 5689, 6305, 6910, 7512, 8144,
                        8777, 9402, 10023, 10653, 11267, 11878,
                        12471, 13123, 13764, 14399]
    provincias = []
    for i in provincias_filas:
        id_provincia = int(censo.iloc[i, 1].split()[2])
        nombre_provincia = censo.iloc[i, 2]
        
        if(nombre_provincia == 'Ciudad Autónoma de Buenos Aires'): 
            provincias.append((id_provincia,'CABA'))
        elif(nombre_provincia == 'Tierra del Fuego, Antártida e Islas del Atlántico Sur'): 
            provincias.append((id_provincia,'Tierra  del Fuego'))
        else:
            provincias.append((id_provincia,nombre_provincia))

    provincias.append((99,'Sin Información')) #Agrego el id faltante que usa defunciones
    
    df_provincias = pd.DataFrame(data=provincias, columns=['id', 'nombre'])  # CORREGIDO
    return df_provincias

df_provincia = obtener_dataFrameProvincias(censo2010)
df_provincia.to_csv('Archivos_Propios/provincias.csv', index= False, encoding='utf-8')

# %% LIMPIEZA DEL DATAFRAME 'ESTABLECIMIENTOS'
def limpieza_establecimientos():
    """
    Obtiene los datos de relvancia del dataFrame establecimientos
    {id}->{nombre:str, id_departamento:int, es_publico:bool, terapia_intensiva:bool}
    """
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
                -- Como el departamento_id no es unico, le concatenamos la provincia.
                CONCAT(provincia_id, '_',departamento_id) AS id, 
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

# Cambiamos el jurisdiccion_de_residencia_id = 98 a 99 para tener un mejor menejo de nulls.
# Formateamos grupo_edad.
defunciones_tuneado = dd.query(
    """
        SELECT 
            defunciones.anio, 
            --- RENAME A 98 -> 99
            CASE 
                WHEN jurisdiccion_de_residencia_id = 98 
                THEN 99
                ELSE jurisdiccion_de_residencia_id
                END as provincia_id,
            defunciones.cie10_causa_id AS cie10_causa_id, 
            Sexo AS sexo, 
            
            --- FORMATEO DE GRUPO_EDAD
            CASE
                WHEN grupo_edad = '01.De a 0  a 14 anios' THEN '0-14'
                WHEN grupo_edad = '02.De 15 a 34 anios' THEN '15-34'
                WHEN grupo_edad = '03.De 35 a 54 anios' THEN '35-54'
                WHEN grupo_edad = '04.De 55 a 74 anios' THEN '55-74'
                WHEN grupo_edad = '05.De 75 anios y mas' THEN '75 o mas'
                ELSE 'Sin Información'
            END AS grupo_edad, 
            
            cantidad,
        FROM defunciones
        ORDER BY cie10_causa_id
            """).df()

# INICIANDO PROCESO PARA LAS TUPLAS QUE NO SE RELACIONAN CON NADA

codigos_null = (dd.query("""
        SELECT DISTINCT 
            d.cie10_causa_id AS codigo
        FROM defunciones_tuneado AS d
        LEFT JOIN clasificacion_defunciones AS c 
            ON d.cie10_causa_id = c.codigo_def
        WHERE c.codigo_def IS NULL
""").df())["codigo"].tolist()

# Renombramos los codigos que no machean con nada a un código nuevo: A00
for codigo in codigos_null:
    defunciones_tuneado.loc[defunciones_tuneado['cie10_causa_id'] == codigo, 'cie10_causa_id'] = 'A00'

# Añadimos el nuevo código a clasificacion defunciones.
nueva_fila = pd.DataFrame({'codigo_def':['A00'],'clasificacion':["Sin Información"]})
clasificacion_defunciones = pd.concat([clasificacion_defunciones, nueva_fila])

# Con la seguridad de que todos los codigos le corresponde una categoria hacemos un INNER JOIN.
defunciones_tuneado = dd.query(
    """
        SELECT 
            d.anio, 
            d.provincia_id, 
            c.categorias, 
            d.sexo, 
            d.grupo_edad, 
            d.cantidad
        FROM defunciones_tuneado AS d
        INNER JOIN clasificacion_defunciones AS c
            ON d.cie10_causa_id = c.codigo_def
    """).df()
# SUMAMOS filas semejantes.
defunciones_tuneado = dd.query(
    """
        SELECT 
            anio	, 
            provincia_id, 
            categorias, 
            sexo, 
            grupo_edad, 
            SUM(cantidad) as cantidad
        FROM defunciones_tuneado 
        GROUP BY  anio,provincia_id, categorias,sexo, grupo_edad 
    """).df()

defunciones_tuneado.to_csv('Archivos_Propios/defunciones.csv', index=False, encoding='utf-8')
