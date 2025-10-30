import pandas as pd
from unidecode import unidecode

# Ruta del archivo original
archivo_entrada = "actuaciones_julio-septiembre_202510300628.csv"
archivo_salida = "actuaciones_ofuscado.csv"

# Leer el CSV
df = pd.read_csv(archivo_entrada, encoding='utf-8')

# Quitar tildes de todas las columnas tipo texto
df = df.applymap(lambda x: unidecode(str(x)) if isinstance(x, str) else x)

# Ofuscar 'oficina_territorial'
if 'oficina_territorial' in df.columns:
    oficinas_unicas = df['oficina_territorial'].dropna().unique()
    mapa_oficinas = {nombre: f"Oficina {i+1}" for i, nombre in enumerate(oficinas_unicas)}
    df['oficina_territorial'] = df['oficina_territorial'].map(mapa_oficinas)

# Ofuscar 'abogado_responsable'
if 'abogado_responsable' in df.columns:
    abogados_unicos = df['abogado_responsable'].dropna().unique()
    mapa_abogados = {nombre: f"Usuario {i+1}" for i, nombre in enumerate(abogados_unicos)}
    df['abogado_responsable'] = df['abogado_responsable'].map(mapa_abogados)

# Guardar el CSV resultante
df.to_csv(archivo_salida, index=False, encoding='utf-8')

print("Archivo procesado y guardado como:", archivo_salida)
