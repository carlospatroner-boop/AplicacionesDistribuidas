import os
import sys
import pandas as pd
import numpy as np


def compute_metrics(series: pd.Series):
    """Compute mean, median, std, and 95th percentile for a numeric series."""
    s = pd.to_numeric(series, errors='coerce').dropna()
    if s.empty:
        return None
    mean = s.mean()
    median = s.median()
    std = s.std(ddof=0)
    p95 = np.percentile(s, 95)
    return {
        'mean': mean,
        'median': median,
        'std': std,
        'p95': p95
    }


def format_metrics(metrics: dict):
    return (
        f"Media: {metrics['mean']:.4f}\n"
        f"Mediana: {metrics['median']:.4f}\n"
        f"Desviación estándar: {metrics['std']:.4f}\n"
        f"Percentil 95: {metrics['p95']:.4f}\n"
    )


def read_and_analyze(sockets_path: str, grpc_path: str):
    results = {}

    if os.path.isfile(sockets_path):
        try:
            df_s = pd.read_csv(sockets_path)
            metrics_s = compute_metrics(df_s.get('avg_latency_ms'))
            results['sockets'] = metrics_s
        except Exception as e:
            print(f"Error leyendo {sockets_path}: {e}")
            results['sockets'] = None
    else:
        print(f"Advertencia: archivo no encontrado: {sockets_path}")
        results['sockets'] = None

    if os.path.isfile(grpc_path):
        try:
            df_g = pd.read_csv(grpc_path)
            metrics_g = compute_metrics(df_g.get('avg_latency_ms'))
            results['grpc'] = metrics_g
        except Exception as e:
            print(f"Error leyendo {grpc_path}: {e}")
            results['grpc'] = None
    else:
        print(f"Advertencia: archivo no encontrado: {grpc_path}")
        results['grpc'] = None

    return results


def main():
    base = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    sockets_csv = os.path.join(base, 'data', 'latency_sockets.csv')
    grpc_csv = os.path.join(base, 'data', 'latency_grpc.csv')

    print('\n===== Análisis estadístico de latencias =====\n')
    results = read_and_analyze(sockets_csv, grpc_csv)

    print('--- Sockets TCP ---')
    if results.get('sockets'):
        print(format_metrics(results['sockets']))
    else:
        print('No hay datos disponibles para Sockets TCP.\n')

    print('--- gRPC ---')
    if results.get('grpc'):
        print(format_metrics(results['grpc']))
    else:
        print('No hay datos disponibles para gRPC.\n')


if __name__ == '__main__':
    main()
