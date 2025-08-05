import os
import glob
import numpy as np
import pandas as pd
import streamlit as st
from datetime import timedelta

# -------------------- Configuración --------------------
st.set_page_config(page_title="Monitoreo de precios", layout="wide")
st.title("Monitoreo de precios")
st.header("🧀 Quesos Cremosos en supermercados")

# -------------------- Carga de archivos consolidados --------------------
@st.cache_data
def load_data():
    all_dfs = []
    for fp in glob.glob("Data/Prueba2/productos_unificados_*.csv"):
        df = pd.read_csv(fp, parse_dates=['fecha'])
        df.rename(columns={'producto': 'Producto'}, inplace=True)
        
        # Normalizar nombres de supermercados
        df.columns = [col.title() if col.lower() not in ['fecha', 'producto'] else col for col in df.columns]
        all_dfs.append(df)
    
    return pd.concat(all_dfs, ignore_index=True)

df = load_data()

# -------------------- Última actualización --------------------
ultima_fecha = df['fecha'].max().strftime("%d-%m-%Y")
st.markdown(f"<small>📅 Última actualización: {ultima_fecha}</small>", unsafe_allow_html=True)

# -------------------- Sidebar: Filtros --------------------
st.sidebar.header("Filtros")
productos = ["Todos"] + sorted(df['Producto'].unique())
prod_sel = st.sidebar.selectbox("Producto", productos)
if prod_sel != "Todos":
    df = df[df['Producto'] == prod_sel]

fechas_disponibles = sorted(df['fecha'].unique(), reverse=True)
fecha_sel = st.sidebar.selectbox("Fecha", fechas_disponibles, format_func=lambda x: x.strftime("%d-%m-%Y"))
df_latest = df[df['fecha'] == fecha_sel].copy()

# -------------------- Promedio histórico --------------------
super_cols = [col for col in df.columns if col not in ['fecha', 'Producto']]
df_long = df.melt(id_vars=['fecha', 'Producto'], value_vars=super_cols, var_name='Supermercado', value_name='Precio')
df_long.dropna(subset=['Precio'], inplace=True)

avg_hist = df_long.groupby('Producto')['Precio'].mean().reset_index(name='Promedio histórico')

# -------------------- Tabla ejecutiva --------------------
st.subheader(f"📊 Precios del {fecha_sel.strftime('%d-%m-%Y')}")

pivot = df_latest.set_index('Producto')[super_cols]

df_temp = df_long[df_long['fecha'] == fecha_sel]
df_temp = df_temp.merge(avg_hist, on='Producto', how='left')

pivot['Promedio histórico'] = df_temp.drop_duplicates(subset='Producto').set_index('Producto')['Promedio histórico']

productos_destacados = [
    "Queso Cremoso fraccionado Punta del Agua x Kg.",
    "Queso Cremoso Trozado Punta del Agua x 1 kg."
]

def resaltar_producto(val):
    return 'background-color: blue' if val in productos_destacados else ''

pivot = pivot.reset_index()  # Esto convierte el índice 'Producto' en columna normal
pivot = pivot.rename(columns={pivot.columns[0]: 'Producto'})  # Renombramos esa columna si quedó sin nombre

styled = (
    pivot
    .style
    .format("{:.2f}", subset=super_cols + ['Promedio histórico'])
    .applymap(resaltar_producto, subset=['Producto'])
    .highlight_max(axis=1, subset=super_cols, color='crimson')
    .highlight_min(axis=1, subset=super_cols, color='forestgreen')
)


st.dataframe(styled, use_container_width=True)

st.markdown(
    """
    <div style='text-align: right'>
        <span style='color:green; font-weight:bold'>precio más bajo</span> · 
        <span style='color:red; font-weight:bold'>precio más alto</span>
    </div>
    """,
    unsafe_allow_html=True
)

# -------------------- Evolución por producto --------------------
st.subheader("📈 Evolución en los últimos 30 días")
prod_chart = st.selectbox("Seleccioná un producto", sorted(df['Producto'].unique()))
df_chart = df[df['Producto'] == prod_chart]
df_chart_30 = df_chart[df_chart['fecha'] >= df_chart['fecha'].max() - timedelta(days=30)]

chart_df = df_chart_30.set_index('fecha')[super_cols]
st.line_chart(chart_df)

# -------------------- Oportunidades de ahorro --------------------

# st.subheader("💡 Oportunidades actuales (vs. promedio histórico)")

# df_today = df_long[df_long['fecha'] == df_long['fecha'].max()].copy()
# df_today = df_today.merge(avg_hist, on='Producto')
# df_today['Ahorro (%)'] = 1 - (df_today['Precio'] / df_today['Promedio histórico'])
# df_today = df_today.dropna(subset=['Ahorro (%)'])

# opp = df_today.sort_values('Ahorro (%)', ascending=False).head(5)

# cols = st.columns(len(opp))
# for i, (_, row) in enumerate(opp.iterrows()):
#     with cols[i]:
#         st.metric(
#             label=row['Producto'],
#             value=f"${row['Precio']:,.0f}",
#             delta=f"-{row['Ahorro (%)']:.0%}"
#         )
#         st.caption(row['Supermercado'])

# -------------------- Créditos --------------------
st.sidebar.markdown("---")
st.sidebar.markdown(
    """
    <small>
    👤 Adrian Szklar<br>
    📧 <a href="mailto:szklaradriandatos@gmail.com">szklaradriandatos@gmail.com</a><br>
    🔗 <a href="https://linkedin.com/in/adrian-szklar" target="_blank">LinkedIn</a> · 
    <a href="https://github.com/ASzklar" target="_blank">GitHub</a><br>
    </small>
    """,
    unsafe_allow_html=True
)
