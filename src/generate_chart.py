import os
import sys
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np


def read_latency_files(paths):
    dfs = []
    for path, protocol in paths:
        if os.path.isfile(path):
            try:
                df = pd.read_csv(path)
                df['protocol'] = protocol
                dfs.append(df)
            except Exception as e:
                print(f"Advertencia: no se pudo leer {path}: {e}")
        else:
            print(f"Advertencia: archivo no encontrado: {path}")
    if dfs:
        return pd.concat(dfs, ignore_index=True)
    return pd.DataFrame()


def synthesize_samples(row, n_samples=100):
    """Genera muestras sintéticas para una fila que contiene avg, min y max de latencia.

    Utiliza una normal truncada simple alrededor de `avg` y recorta al rango [min, max].
    """
    try:
        avg = float(row.get('avg_latency_ms', np.nan))
    except Exception:
        avg = np.nan

    try:
        min_val = float(row.get('min_latency_ms', np.nan))
    except Exception:
        min_val = np.nan

    try:
        max_val = float(row.get('max_latency_ms', np.nan))
    except Exception:
        max_val = np.nan

    if np.isnan(avg):
        return np.array([])

    if np.isnan(min_val) or min_val <= 0:
        min_val = max(0.0, avg * 0.5)
    if np.isnan(max_val) or max_val <= 0:
        max_val = max(avg * 1.5, min_val + 1e-6)

    sigma1 = (avg - min_val) / 3.0 if avg > min_val else avg * 0.1
    sigma2 = (max_val - avg) / 3.0 if max_val > avg else avg * 0.1
    sigma = max(sigma1, sigma2, avg * 0.05, 1e-3)

    samples = np.random.normal(loc=avg, scale=sigma, size=n_samples)
    samples = np.clip(samples, min_val, max_val)
    return samples


def plot_boxplot(df, output_path):
    if df.empty:
        print("No hay datos disponibles para graficar.")
        return None

    # Normalize node_id
    if 'node_id' not in df.columns:
        if 'node' in df.columns:
            df['node_id'] = df['node']
        else:
            df['node_id'] = 'unknown'

    groups = []
    labels = []
    for (protocol, node), group_df in df.groupby(['protocol', 'node_id']):
        row = group_df.iloc[0]
        samples = synthesize_samples(row, n_samples=100)
        if samples.size == 0:
            continue
        groups.append(samples)
        labels.append(f"{protocol}/{node}")

    if not groups:
        print("No se encontraron datos numéricos de latencia para sintetizar y graficar.")
        return None

    fig, ax = plt.subplots(figsize=(max(8, len(groups) * 0.6), 6))
    bplot = ax.boxplot(groups, labels=labels, patch_artist=True)

    colors = ['#8FB9A8', '#F2B5D4']
    for i, patch in enumerate(bplot['boxes']):
        patch.set_facecolor(colors[i % len(colors)])

    ax.set_title('Distribución sintética de latencia por nodo y protocolo')
    ax.set_ylabel('Latencia (ms)')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    fig.savefig(output_path, dpi=300)
    print(f"Guardado diagrama en: {output_path}")
    return output_path


def main():
    base = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    # Paths relative to workspace root
    sockets_csv = os.path.join(base, 'data', 'latency_sockets.csv')
    grpc_csv = os.path.join(base, 'data', 'latency_grpc.csv')

    df = read_latency_files([
        (sockets_csv, 'sockets'),
        (grpc_csv, 'grpc')
    ])

    output_path = os.path.join(base, 'docs', 'figures', 'boxplot.png')
    plot_boxplot(df, output_path)


if __name__ == '__main__':
    main()
