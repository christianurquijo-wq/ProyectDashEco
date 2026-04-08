import json
import webbrowser
import pandas as pd
from google.oauth2.service_account import Credentials
from google.cloud import bigquery

# ── Configuración ──────────────────────────────────────────────
CREDENCIALES_JSON = "credenciales.json"
BQ_PROJECT        = "sustained-edge-465417-m3"
BQ_DATASET        = "EFE_2026"
BQ_TABLE          = "ECOLOMBIA_2026"
OUTPUT_HTML       = "dashboard.html"

SCOPES = [
    "https://www.googleapis.com/auth/bigquery",
]

# ── Conexión ───────────────────────────────────────────────────
creds = Credentials.from_service_account_file(CREDENCIALES_JSON, scopes=SCOPES)
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

# ── Limpiar columnas numéricas (comas → puntos) ────────────────
cols_notas = [c for c in df.columns if "nota" in c or "asistencia" in c]
for col in cols_notas:
    df[col] = pd.to_numeric(
        df[col].astype(str).str.replace(",", ".").str.strip(),
        errors="coerce"
    )

df["cohorte"]  = df["cohorte"].fillna("Sin dato").astype(str).str.strip()
df["grupo"]    = df["grupo"].fillna("Sin dato").astype(str).str.strip()
df["programa"] = df["programa"].fillna("Sin dato").astype(str).str.strip()
df["ciudad"]   = df["ciudad"].fillna("Sin dato").astype(str).str.strip()
df["etapa"]    = df["etapa"].fillna("Sin dato").astype(str).str.strip()
df["novedad"]  = df["novedad"].fillna("Sin dato").astype(str).str.strip()
df["estado_contratacion"] = df["estado_contratacion"].fillna("Sin dato").astype(str).str.strip()

# ── Calcular KPIs ──────────────────────────────────────────────
total        = len(df)
activos      = int((df["novedad"] == "Activo").sum())
desertores   = int(df["novedad"].str.contains("eser", case=False, na=False).sum())
contratados  = int(df["estado_contratacion"].str.contains("ontrat", case=False, na=False).sum())

# Distribución por ciudad
por_ciudad = df["ciudad"].value_counts().head(10)

# Notas promedio por módulo
modulos = [f"nota_modulo_{i}" for i in range(8)]
notas_prom = {f"M{i}": round(df[f"nota_modulo_{i}"].mean(), 2)
              for i in range(8) if f"nota_modulo_{i}" in df.columns}
notas_prom = {k: v for k, v in notas_prom.items() if not pd.isna(v)}

# Asistencias promedio por módulo
asist_prom = {f"M{i}": round(df[f"asistencias_modulo_{i}"].mean(), 2)
              for i in range(1, 9) if f"asistencias_modulo_{i}" in df.columns}
asist_prom = {k: v for k, v in asist_prom.items() if not pd.isna(v)}

# Datos para filtros
filtros = {
    "cohortes":  sorted(df["cohorte"].unique().tolist()),
    "grupos":    sorted(df["grupo"].unique().tolist()),
    "programas": sorted(df["programa"].unique().tolist()),
    "ciudades":  sorted(df["ciudad"].unique().tolist()),
    "etapas":    sorted(df["etapa"].unique().tolist()),
}

# Dataset completo para filtrado dinámico en JS
data_js = df[[
    "cohorte","grupo","programa","ciudad","etapa","novedad",
    "estado_contratacion"
] + [c for c in cols_notas if c in df.columns]].copy()

for col in [c for c in cols_notas if c in data_js.columns]:
    data_js[col] = data_js[col].where(data_js[col].notna(), None)

registros = json.loads(data_js.to_json(orient="records"))

# ── Generar HTML ───────────────────────────────────────────────
html = f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Dashboard EFE Colombia 2026</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: system-ui, sans-serif; background: #f4f5f7; color: #1a1a2e; }}
  header {{ background: #1a1a2e; color: #fff; padding: 1.2rem 2rem; display: flex; align-items: center; gap: 1rem; }}
  header h1 {{ font-size: 1.2rem; font-weight: 500; }}
  header span {{ font-size: 0.8rem; opacity: 0.6; }}
  .filtros {{ background: #fff; border-bottom: 1px solid #e2e4e9; padding: 0.8rem 2rem; display: flex; gap: 1rem; flex-wrap: wrap; align-items: center; }}
  .filtros label {{ font-size: 0.75rem; color: #666; display: flex; flex-direction: column; gap: 3px; }}
  .filtros select {{ font-size: 0.82rem; padding: 5px 8px; border: 1px solid #d0d3da; border-radius: 6px; background: #fff; min-width: 140px; }}
  .filtros button {{ padding: 6px 16px; background: #1a1a2e; color: #fff; border: none; border-radius: 6px; font-size: 0.82rem; cursor: pointer; margin-top: 14px; }}
  .filtros button:hover {{ background: #2d2d5e; }}
  .kpis {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 1rem; padding: 1.5rem 2rem 0.5rem; }}
  .kpi {{ background: #fff; border-radius: 10px; padding: 1.2rem; border: 1px solid #e2e4e9; }}
  .kpi .label {{ font-size: 0.72rem; color: #888; text-transform: uppercase; letter-spacing: 0.04em; margin-bottom: 6px; }}
  .kpi .valor {{ font-size: 2rem; font-weight: 600; color: #1a1a2e; }}
  .kpi .sub {{ font-size: 0.75rem; color: #aaa; margin-top: 2px; }}
  .kpi.verde .valor {{ color: #1d9e75; }}
  .kpi.rojo .valor  {{ color: #d85a30; }}
  .kpi.azul .valor  {{ color: #185fa5; }}
  .graficas {{ display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; padding: 1rem 2rem; }}
  .graficas .ancho {{ grid-column: span 2; }}
  .card {{ background: #fff; border-radius: 10px; padding: 1.2rem; border: 1px solid #e2e4e9; }}
  .card h3 {{ font-size: 0.82rem; font-weight: 500; color: #555; margin-bottom: 1rem; }}
  canvas {{ max-height: 260px; }}
  @media (max-width: 700px) {{ .graficas {{ grid-template-columns: 1fr; }} .graficas .ancho {{ grid-column: span 1; }} }}
</style>
</head>
<body>

<header>
  <div>
    <h1>Dashboard · EFE Colombia 2026</h1>
    <span id="subtitulo">Todos los registros</span>
  </div>
</header>

<div class="filtros">
  <label>Cohorte
    <select id="f-cohorte"><option value="">Todos</option>
      {''.join(f'<option>{c}</option>' for c in filtros["cohortes"])}
    </select>
  </label>
  <label>Grupo
    <select id="f-grupo"><option value="">Todos</option>
      {''.join(f'<option>{c}</option>' for c in filtros["grupos"])}
    </select>
  </label>
  <label>Programa
    <select id="f-programa"><option value="">Todos</option>
      {''.join(f'<option>{c}</option>' for c in filtros["programas"])}
    </select>
  </label>
  <label>Ciudad
    <select id="f-ciudad"><option value="">Todos</option>
      {''.join(f'<option>{c}</option>' for c in filtros["ciudades"])}
    </select>
  </label>
  <label>Etapa
    <select id="f-etapa"><option value="">Todos</option>
      {''.join(f'<option>{c}</option>' for c in filtros["etapas"])}
    </select>
  </label>
  <button onclick="aplicarFiltros()">Aplicar</button>
</div>

<div class="kpis">
  <div class="kpi"><div class="label">Total beneficiarios</div><div class="valor" id="k-total">{total}</div><div class="sub">registros</div></div>
  <div class="kpi verde"><div class="label">Activos</div><div class="valor" id="k-activos">{activos}</div><div class="sub">en curso</div></div>
  <div class="kpi rojo"><div class="label">Desertores</div><div class="valor" id="k-desertores">{desertores}</div><div class="sub">bajas</div></div>
  <div class="kpi azul"><div class="label">Contratados</div><div class="valor" id="k-contratados">{contratados}</div><div class="sub">empleabilidad</div></div>
  <div class="kpi"><div class="label">Tasa retención</div><div class="valor" id="k-retencion">{round(activos/total*100) if total else 0}%</div><div class="sub">activos / total</div></div>
</div>

<div class="graficas">
  <div class="card">
    <h3>Estado de beneficiarios</h3>
    <canvas id="g-estado"></canvas>
  </div>
  <div class="card">
    <h3>Distribución por ciudad</h3>
    <canvas id="g-ciudad"></canvas>
  </div>
  <div class="card ancho">
    <h3>Nota promedio por módulo</h3>
    <canvas id="g-notas"></canvas>
  </div>
  <div class="card ancho">
    <h3>Asistencia promedio por módulo</h3>
    <canvas id="g-asistencia"></canvas>
  </div>
  <div class="card ancho">
    <h3>Estado de contratación / empleabilidad</h3>
    <canvas id="g-empleo"></canvas>
  </div>
</div>

<script>
const DATA = {json.dumps(registros)};
const COLORES = ["#185fa5","#1d9e75","#d85a30","#534ab7","#854f0b","#993556","#3b6d11","#a32d2d","#0f6e56","#993c1d"];

let charts = {{}};

function destroyAll() {{
  Object.values(charts).forEach(c => c.destroy());
  charts = {{}};
}}

function pct(n, total) {{
  return total ? Math.round(n / total * 100) : 0;
}}

function aplicarFiltros() {{
  const cohorte  = document.getElementById("f-cohorte").value;
  const grupo    = document.getElementById("f-grupo").value;
  const programa = document.getElementById("f-programa").value;
  const ciudad   = document.getElementById("f-ciudad").value;
  const etapa    = document.getElementById("f-etapa").value;

  let d = DATA;
  if (cohorte)  d = d.filter(r => r.cohorte  === cohorte);
  if (grupo)    d = d.filter(r => r.grupo    === grupo);
  if (programa) d = d.filter(r => r.programa === programa);
  if (ciudad)   d = d.filter(r => r.ciudad   === ciudad);
  if (etapa)    d = d.filter(r => r.etapa    === etapa);

  const partes = [cohorte, grupo, programa, ciudad, etapa].filter(Boolean);
  document.getElementById("subtitulo").textContent =
    partes.length ? partes.join(" · ") : "Todos los registros";

  renderizar(d);
}}

function renderizar(d) {{
  destroyAll();
  const total = d.length;

  // KPIs
  const activos     = d.filter(r => r.novedad === "Activo").length;
  const desertores  = d.filter(r => r.novedad && r.novedad.toLowerCase().includes("eser")).length;
  const contratados = d.filter(r => r.estado_contratacion && r.estado_contratacion.toLowerCase().includes("ontrat")).length;
  document.getElementById("k-total").textContent      = total;
  document.getElementById("k-activos").textContent    = activos;
  document.getElementById("k-desertores").textContent = desertores;
  document.getElementById("k-contratados").textContent= contratados;
  document.getElementById("k-retencion").textContent  = pct(activos, total) + "%";

  // Estado donut
  const estados = {{}};
  d.forEach(r => {{ const v = r.novedad || "Sin dato"; estados[v] = (estados[v]||0)+1; }});
  charts["estado"] = new Chart(document.getElementById("g-estado"), {{
    type: "doughnut",
    data: {{ labels: Object.keys(estados), datasets: [{{ data: Object.values(estados), backgroundColor: COLORES, borderWidth: 1 }}] }},
    options: {{ plugins: {{ legend: {{ position: "right", labels: {{ font: {{ size: 11 }} }} }} }}, cutout: "60%" }}
  }});

  // Ciudad bar
  const ciudades = {{}};
  d.forEach(r => {{ const v = r.ciudad || "Sin dato"; ciudades[v] = (ciudades[v]||0)+1; }});
  const topCiudades = Object.entries(ciudades).sort((a,b)=>b[1]-a[1]).slice(0,10);
  charts["ciudad"] = new Chart(document.getElementById("g-ciudad"), {{
    type: "bar",
    data: {{ labels: topCiudades.map(x=>x[0]), datasets: [{{ data: topCiudades.map(x=>x[1]), backgroundColor: "#185fa5", borderRadius: 4 }}] }},
    options: {{ indexAxis: "y", plugins: {{ legend: {{ display: false }} }}, scales: {{ x: {{ grid: {{ display: false }} }} }} }}
  }});

  // Notas por módulo
  const notasCols = ["nota_modulo_0","nota_modulo_1","nota_modulo_2","nota_modulo_3","nota_modulo_4","nota_modulo_5","nota_modulo_6","nota_modulo_7"];
  const notasLabels = notasCols.map((_,i) => "M"+i);
  const notasVals = notasCols.map(col => {{
    const vals = d.map(r => parseFloat(r[col])).filter(v => !isNaN(v));
    return vals.length ? Math.round(vals.reduce((a,b)=>a+b,0)/vals.length*100)/100 : null;
  }});
  charts["notas"] = new Chart(document.getElementById("g-notas"), {{
    type: "bar",
    data: {{ labels: notasLabels, datasets: [{{ label: "Nota promedio", data: notasVals, backgroundColor: "#1d9e75", borderRadius: 4 }}] }},
    options: {{ plugins: {{ legend: {{ display: false }} }}, scales: {{ y: {{ min: 0, max: 5, grid: {{ color: "#f0f0f0" }} }}, x: {{ grid: {{ display: false }} }} }} }}
  }});

  // Asistencias por módulo
  const asistCols = ["asistencias_modulo_1","asistencias_modulo_2","asistencias_modulo_3","asistencias_modulo_4","asistencias_modulo_5","asistencias_modulo_6","asistencias_modulo_7","asistencias_modulo_8"];
  const asistLabels = asistCols.map((_,i) => "M"+(i+1));
  const asistVals = asistCols.map(col => {{
    const vals = d.map(r => parseFloat(r[col])).filter(v => !isNaN(v));
    return vals.length ? Math.round(vals.reduce((a,b)=>a+b,0)/vals.length*100)/100 : null;
  }});
  charts["asistencia"] = new Chart(document.getElementById("g-asistencia"), {{
    type: "line",
    data: {{ labels: asistLabels, datasets: [{{ label: "Asistencia promedio", data: asistVals, borderColor: "#534ab7", backgroundColor: "rgba(83,74,183,0.08)", tension: 0.3, fill: true, pointRadius: 4 }}] }},
    options: {{ plugins: {{ legend: {{ display: false }} }}, scales: {{ y: {{ min: 0, grid: {{ color: "#f0f0f0" }} }}, x: {{ grid: {{ display: false }} }} }} }}
  }});

  // Empleabilidad
  const empleo = {{}};
  d.forEach(r => {{ const v = r.estado_contratacion || "Sin dato"; empleo[v] = (empleo[v]||0)+1; }});
  const topEmpleo = Object.entries(empleo).sort((a,b)=>b[1]-a[1]);
  charts["empleo"] = new Chart(document.getElementById("g-empleo"), {{
    type: "bar",
    data: {{ labels: topEmpleo.map(x=>x[0]), datasets: [{{ data: topEmpleo.map(x=>x[1]), backgroundColor: COLORES, borderRadius: 4 }}] }},
    options: {{ plugins: {{ legend: {{ display: false }} }}, scales: {{ y: {{ grid: {{ color: "#f0f0f0" }} }}, x: {{ grid: {{ display: false }} }} }} }}
  }});
}}

renderizar(DATA);
</script>
</body>
</html>"""

with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
    f.write(html)

print(f"Dashboard generado: {OUTPUT_HTML}")
webbrowser.open(OUTPUT_HTML)