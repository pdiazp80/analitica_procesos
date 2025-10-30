import pandas as pd
from datetime import timedelta

# === 1. Cargar archivo original ===
df = pd.read_csv("actuaciones_ofuscado.csv")

# === 2. Crear DataFrame base ===
df_log = pd.DataFrame()
df_log["case_id"] = df["radicado_demanda"]
df_log["activity"] = df["actuacion"] if "actuacion" in df.columns else "Actuación registrada"
df_log["resource"] = df["abogado_responsable"]
df_log["start_timestamp"] = pd.to_datetime(df["Start_Time"])
df_log["end_timestamp"] = pd.to_datetime(df["End_Time"])

# === 3. Flujo estándar (según diagrama) ===
flujo_base = [
    ("Presentación de demanda", "Interesado"),
    ("Registro de demanda", "Dirección Jurídica"),
    ("Vinculación de documentos adjuntos", "Dirección Jurídica"),
    ("Actualización de estado de la demanda", "CSJ"),
    ("Seguimiento y control de tiempos procesales", "Repositorio documental institucional"),
]

# === 4. Generar eventos simulados faltantes ===
rows = []
for case_id, group in df_log.groupby("case_id"):
    actividades_existentes = group["activity"].unique().tolist()
    fecha_base = group["start_timestamp"].min() - timedelta(minutes=30)

    for i, (actividad, recurso) in enumerate(flujo_base):
        if actividad not in actividades_existentes:
            rows.append({
                "case_id": case_id,
                "activity": actividad,
                "resource": recurso,
                "start_timestamp": fecha_base + timedelta(minutes=i*5),
                "end_timestamp": fecha_base + timedelta(minutes=i*5 + 2)
            })

df_simulados = pd.DataFrame(rows)

# === 5. Combinar ambos conjuntos ===
df_final = pd.concat([df_log, df_simulados], ignore_index=True)

# === 6. Ordenar por case_id y tiempo ===
df_final = df_final.sort_values(by=["case_id", "start_timestamp"])

# === 7. Exportar a CSV ===
df_final.to_csv("log_eventos_apromore.csv", index=False)

print("✅ Archivo 'log_eventos_apromore.csv' generado correctamente.")
