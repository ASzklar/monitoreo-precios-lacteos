import pandas as pd
import numpy as np
import re
import os
from datetime import datetime
from collections import defaultdict

pd.set_option('display.max_colwidth', 200)

def parse_price(price):
    if pd.isna(price):
        return np.nan
    price_str = str(price)
    price_str = price_str.replace('$', '').replace('.', '').strip()
    price_str = price_str.replace(',', '.')
    try:
        return float(price_str)
    except ValueError:
        return np.nan

def unify_products(filepath, product_column, unification_map):
    filename = os.path.basename(filepath)
    match = re.match(r'(\w+)_raw_(\d{4}-\d{2}-\d{2})\.csv', filename)

    if not match:
        print(f"⚠️ Nombre de archivo inesperado: {filename}")
        return None, None

    supermercado = match.group(1)
    fecha_str = match.group(2)

    df = pd.read_csv(filepath)
    df['fecha'] = fecha_str
    df['supermercado'] = supermercado

    reverse_map = {}
    for canonical, variants in unification_map.items():
        for variant in variants:
            reverse_map[variant] = canonical
    df['producto'] = df[product_column].map(reverse_map).fillna(df[product_column])
    df['precio'] = df['precio'].apply(parse_price)

    df = df[['fecha', 'supermercado', 'producto', 'precio']]
    return fecha_str, df

# --- Mapa de unificación editable ---
unification_map_quesos = {
    'Queso Cremoso Cremón La Serenísima x 1 kg.': [
        'Queso Cremón La Serenísima cremoso fraccionado x kg',
        'Queso Cremón Cremoso La Serenísima - Unidad Aprox. 500g',
        'Queso Cremón Cremoso Paquete Por Kg',
        'Queso Cremoso Cremón La Serenísima x 1 kg.'
    ],
    'Queso Cremoso La Paulina x 1 kg.': [
        'Queso Cremoso Fraccionado LA PAULINA Xkg',
        'Queso Cremoso La Paulina Trozado 1kgs',
        'Queso cremoso La Paulina x kg.'
    ],
    'Queso Cremoso Cremón Horma La Serenísima x 1 Kg.': [
        'Queso cremoso Silvia fraccionado x kg.',
        'Queso cremoso Silvia media horma',
        'Queso Cremon Cremoso La Serenisima Fraccionado Aprox 1 Kg'
    ],
    'Queso Cremoso fraccionado Punta del Agua x Kg.': [
        'Queso Cremoso H./Fraccionado Punta del Agua x 1 Kg.',
        'Queso Cremoso PUNTA DEL AGUA X Kg',
        'Queso Cremoso Punta Del Agua Horma X Kg'
    ],
    'Queso Cremoso Cremón Doble Crema Fraccionado La Serenísima x 1 kg.': [
        'Queso Cremoso Cremón Doble Crema Fraccionado La Serenísima x 1 kg.',
        'Queso doble crema cremon fracc La serenísima x kg.'
    ],
    'Queso Cremoso Trozado Punta del Agua x 1 kg.': [
        'Queso Cremoso Trozado Punta del Agua x 1 kg.',
        'Queso Cremoso Fraccionado Punta Del A . 1 Kgm'
    ],
    'Queso Cremoso La Paulina Doble Crema x kg.': [
        'Queso Cremoso La Paulina Doble Crema Paquete 1 Kg',
        'Queso Cremoso Doble Crema Fraccionado La Paulina x 1 Kg.',
        'Queso cremoso La Paulina doble crema x kg.',
        'Queso Cremoso Doble Crema LA PAULINA X Kg'
    ]
}

# --- Configuración de carpetas y ejecución ---
RAW_DATA_PATH     = os.path.join("Data", "Raw")
CLEANED_DATA_PATH = os.path.join("Data", "Prueba2")
PRODUCT_COLUMN     = 'nombre'

# Agrupar archivos por fecha
files_by_date = defaultdict(list)
for filename in os.listdir(RAW_DATA_PATH):
    if filename.endswith('.csv') and '_raw_' in filename:
        match = re.search(r'(\d{4}-\d{2}-\d{2})', filename)
        if match:
            fecha = match.group(1)
            files_by_date[fecha].append(filename)

# Procesar y exportar uno por fecha
for fecha_str, file_list in files_by_date.items():
    all_dfs = []
    for filename in file_list:
        filepath = os.path.join(RAW_DATA_PATH, filename)
        try:
            fecha_detectada, df = unify_products(filepath, PRODUCT_COLUMN, unification_map_quesos)
            if df is not None:
                all_dfs.append(df)
        except Exception as e:
            print(f"❌ Error procesando {filename}: {e}")

    if all_dfs:
        result_df = pd.concat(all_dfs, ignore_index=True)

        # 🚫 Filtrar productos no deseados
        excluir_productos = [
            'Cremoso vegano Felices Las Vacas 500 g.',
            'Queso cremoso Cremón x kg.',
            'Cremoso vegano Biorganic 500 g.',
            'Cremoso Base De Almendras Felices Las Vacas 500 Gr Felices Las Vacas',
            'Queso Untable Adler Cremoso Con Salame 190gr'
        ]

        result_df = result_df[~result_df['producto'].isin(excluir_productos)]


        # 💡 Pivot para tener columnas por supermercado
        df_pivot = result_df.pivot_table(
            index=['fecha', 'producto'],
            columns='supermercado',
            values='precio',
            aggfunc='first'  # o 'mean' si hay múltiples registros
        ).reset_index()

        # Ordenar columnas
        cols_order = ['fecha', 'producto'] + sorted([col for col in df_pivot.columns if col not in ['fecha', 'producto']])
        df_pivot = df_pivot[cols_order]

        # 📝 Exportar CSV final
        os.makedirs(CLEANED_DATA_PATH, exist_ok=True)
        output_filepath = os.path.join(CLEANED_DATA_PATH, f"productos_unificados_{fecha_str}.csv")
        df_pivot.to_csv(output_filepath, index=False, encoding='utf-8')
        print(f"✅ Archivo tabulado generado: {output_filepath}")
