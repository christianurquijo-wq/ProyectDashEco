import os
import json
import pandas as pd
from google.oauth2.service_account import Credentials
from google.cloud import bigquery

import dash
from dash import dcc, html, Input, Output
import plotly.express as px

# ── Configuración ──────────────────────────────────────────────
BQ_PROJECT = "sustained-edge-465417-m3"
BQ_DATASET = "EFE_2026"
BQ_TABLE   = "ECOLOMBIA_2026"

SCOPES = ["https://www.googleapis.com/auth/bigquery"]

# ── Conexión con credenciales desde variable de entorno ────────
creds_info = json.loads(os.environ.get("GOOGLE_CREDENTIALS"))
creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
bq    = bigquery.Client(credentials=creds, project=BQ_PROJECT)

# ── Consulta ───────────────────────────────────────────────────
query = f"""
SELECT
    cohorte, grupo, programa, ciudad, etapa,
    novedad, estado_academico, estado_contratacion,
    nota_final, nota_modulo_0, nota_modulo_1,
    nota_modulo_2, nota_modulo_3, nota_modulo_4,
    nota_modulo_5, nota_modulo_6, nota_modulo_7,
    asistencias_modulo_1, asistencias_modulo_2,
    asistencias_modulo_3, asistencias_modulo_4,
    asistencias_modulo_5, asistencias_modulo_6,
    asistencias_modulo_7, asistencias_modulo_8
FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}`
"""

df = bq.query(query).to_dataframe()
print(f"Filas obtenidas: {len(df)}")

# ── Limpiar columnas numéricas ─────────────────────────────────
cols_notas = [c for c in df.columns if "nota" in c or "asistencia" in c]
for col in cols_notas:
    df[col] = pd.to_numeric(
        df[col].astype(str).str.replace(",", ".").str.strip(),
        errors="coerce"
    )

for col in ["cohorte", "grupo", "programa", "ciudad", "etapa", "novedad", "estado_contratacion"]:
    df[col] = df[col].fillna("Sin dato").astype(str).str.strip()

# ── App Dash ───────────────────────────────────────────────────
app = dash.Dash(__name__)
server = app.server  # necesario para Railway

app.layout = html.Div([
    html.Header([
        html.H1("Dashboard · EFE Colombia 2026", style={"color": "white", "fontWeight": "400"}),
    ], style={"background": "#1a1a2e", "padding": "1.2rem 2rem"}),

    html.Div([
        html.Label(["Cohorte", dcc.Dropdown(["Todos"] + sorted(df["cohorte"].unique()), "Todos", id="f-cohorte", clearable=False)]),
        html.Label(["Grupo",   dcc.Dropdown(["Todos"] + sorted(df["grupo"].unique()),   "Todos", id="f-grupo",   clearable=False)]),
        html.Label(["Programa",dcc.Dropdown(["Todos"] + sorted(df["programa"].unique()),"Todos", id="f-programa",clearable=False)]),
        html.Label(["Ciudad",  dcc.Dropdown(["Todos"] + sorted(df["ciudad"].unique()),  "Todos", id="f-ciudad",  clearable=False)]),
        html.Label(["Etapa",   dcc.Dropdown(["Todos"] + sorted(df["etapa"].unique()),   "Todos", id="f-etapa",   clearable=False)]),
    ], style={"display": "flex", "gap": "1rem", "flexWrap": "wrap", "padding": "1rem 2rem", "background": "white", "borderBottom": "1px solid #e2e4e9"}),

    html.Div(id="kpis", style={"display": "grid", "gridTemplateColumns": "repeat(5, 1fr)", "gap": "1rem", "padding": "1.5rem 2rem 0.5rem"}),

    html.Div([
        dcc.Graph(id="g-estado"),
        dcc.Graph(id="g-ciudad"),
        dcc.Graph(id="g-notas",      style={"gridColumn": "span 2"}),
        dcc.Graph(id="g-asistencia", style={"gridColumn": "span 2"}),
        dcc.Graph(id="g-empleo",     style={"gridColumn": "span 2"}),
    ], style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "1rem", "padding": "1rem 2rem"}),

], style={"fontFamily": "system-ui, sans-serif", "background": "#f4f5f7", "minHeight": "100vh"})


@app.callback(
    Output("kpis",        "children"),
    Output("g-estado",    "figure"),
    Output("g-ciudad",    "figure"),
    Output("g-notas",     "figure"),
    Output("g-asistencia","figure"),
    Output("g-empleo",    "figure"),
    Input("f-cohorte",  "value"),
    Input("f-grupo",    "value"),
    Input("f-programa", "value"),
    Input("f-ciudad",   "value"),
    Input("f-etapa",    "value"),
)
def actualizar(cohorte, grupo, programa, ciudad, etapa):
    d = df.copy()
    if cohorte  != "Todos": d = d[d["cohorte"]  == cohorte]
    if grupo    != "Todos": d = d[d["grupo"]    == grupo]
    if programa != "Todos": d = d[d["programa"] == programa]
    if ciudad   != "Todos": d = d[d["ciudad"]   == ciudad]
    if etapa    != "Todos": d = d[d["etapa"]    == etapa]

    total       = len(d)
    activos     = int((d["novedad"] == "Activo").sum())
    desertores  = int(d["novedad"].str.contains("eser", case=False, na=False).sum())
    contratados = int(d["estado_contratacion"].str.contains("ontrat", case=False, na=False).sum())
    retencion   = f"{round(activos/total*100) if total else 0}%"

    def kpi_card(label, valor, color="#1a1a2e"):
        return html.Div([
            html.Div(label, style={"fontSize": "0.72rem", "color": "#888", "textTransform": "uppercase"}),
            html.Div(str(valor), style={"fontSize": "2rem", "fontWeight": "600", "color": color}),
        ], style={"background": "white", "borderRadius": "10px", "padding": "1.2rem", "border": "1px solid #e2e4e9"})

    kpis = [
        kpi_card("Total beneficiarios", total),
        kpi_card("Activos",     activos,     "#1d9e75"),
        kpi_card("Desertores",  desertores,  "#d85a30"),
        kpi_card("Contratados", contratados, "#185fa5"),
        kpi_card("Tasa retención", retencion),
    ]

    # Gráficas
    fig_estado = px.pie(d, names="novedad", hole=0.6, title="Estado de beneficiarios",
                        color_discrete_sequence=px.colors.qualitative.Bold)

    top_ciudades = d["ciudad"].value_counts().head(10).reset_index()
    top_ciudades.columns = ["ciudad", "count"]
    fig_ciudad = px.bar(top_ciudades, x="count", y="ciudad", orientation="h",
                        title="Distribución por ciudad", color_discrete_sequence=["#185fa5"])

    modulos = [f"nota_modulo_{i}" for i in range(8)]
    notas_prom = [d[c].mean() for c in modulos if c in d.columns]
    fig_notas = px.bar(x=[f"M{i}" for i in range(len(notas_prom))], y=notas_prom,
                       title="Nota promedio por módulo", color_discrete_sequence=["#1d9e75"],
                       labels={"x": "Módulo", "y": "Nota promedio"})
    fig_notas.update_yaxes(range=[0, 5])

    asist_cols = [f"asistencias_modulo_{i}" for i in range(1, 9)]
    asist_prom = [d[c].mean() for c in asist_cols if c in d.columns]
    fig_asist = px.line(x=[f"M{i}" for i in range(1, len(asist_prom)+1)], y=asist_prom,
                        title="Asistencia promedio por módulo",
                        labels={"x": "Módulo", "y": "Asistencia promedio"},
                        markers=True)
    fig_asist.update_traces(line_color="#534ab7", fill="tozeroy", fillcolor="rgba(83,74,183,0.08)")

    empleo = d["estado_contratacion"].value_counts().reset_index()
    empleo.columns = ["estado", "count"]
    fig_empleo = px.bar(empleo, x="estado", y="count",
                        title="Estado de contratación / empleabilidad",
                        color_discrete_sequence=px.colors.qualitative.Bold)

    return kpis, fig_estado, fig_ciudad, fig_notas, fig_asist, fig_empleo


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8050))
    app.run(host="0.0.0.0", port=port, debug=False)