import os, re
import pandas as pd
from google.cloud import storage, bigquery

PROJECT = os.environ.get("PROJECT", "rq-pharma-data-lab-26k9")
BUCKET = os.environ["BUCKET"]

SRC_PREFIX = os.environ.get("SRC_PREFIX", "stg/bps/")
TARGET = os.environ.get("TARGET", f"{PROJECT}.silver_bps.bps_purchases")

# Se quiser limitar:
YEARS = [y for y in os.environ.get("YEARS", "").split(",") if y.strip()]
INGEST_DATE = os.environ.get("INGEST_DATE")  # opcional (ex: 2026-02-13)

def _list_parquets():
    client = storage.Client(project=PROJECT)
    blobs = client.list_blobs(BUCKET, prefix=SRC_PREFIX)
    files = []
    for b in blobs:
        if not b.name.endswith(".parquet"):
            continue
        # filtra por year=
        m = re.search(r"year=(20\d{2})", b.name)
        year = m.group(1) if m else None
        if YEARS and (year not in YEARS):
            continue
        if INGEST_DATE and (f"ingest_date={INGEST_DATE}" not in b.name):
            continue
        files.append(f"gs://{BUCKET}/{b.name}")
    return sorted(files)

def _to_int(series):
    return pd.to_numeric(series.astype(str).str.replace(r"[^0-9]", "", regex=True), errors="coerce").astype("Int64")

def _to_num(series):
    # aceita "1234.56" e "1.234,56"
    s = series.astype(str).str.strip()
    s = s.str.replace(".", "", regex=False)          # remove milhar
    s = s.str.replace(",", ".", regex=False)         # vírgula -> ponto
    return pd.to_numeric(s, errors="coerce")

def _to_dt(series):
    # padrão que você mostrou: 2024/01/01 00:00:00.000
    s = series.astype(str).str.strip()
    dt = pd.to_datetime(s, format="%Y/%m/%d %H:%M:%S.%f", errors="coerce")
    if dt.isna().all():
        dt = pd.to_datetime(s, errors="coerce")
    return dt

def main():
    parquet_files = _list_parquets()
    if not parquet_files:
        raise RuntimeError("Nenhum parquet encontrado com os filtros atuais.")

    print(f"Arquivos parquet: {len(parquet_files)} (ex.: {parquet_files[0]})")

    # lê tudo (MVP). Se crescer, a gente muda pra leitura por dataset/partição.
    dfs = []
    for p in parquet_files:
        df = pd.read_parquet(p, engine="pyarrow")
        dfs.append(df)

    df = pd.concat(dfs, ignore_index=True)
    print("Linhas lidas:", len(df))

    # --- normalizações/tipagens (Silver) ---
    df["uf"] = df["uf"].astype(str).str.strip().str.upper()
    df.loc[df["uf"].str.len() != 2, "uf"] = pd.NA

    df["ano_compra"] = _to_int(df["ano_compra"])
    df["source_year"] = _to_int(df["source_year"])
    df["ingest_date"] = pd.to_datetime(df["ingest_date"], errors="coerce").dt.date

    df["compra_ts"] = _to_dt(df["compra"])
    df["insercao_ts"] = _to_dt(df["insercao"])
    df["compra_date"] = df["compra_ts"].dt.date

    df["qtd_itens_comprados"] = _to_int(df["qtd_itens_comprados"])
    df["preco_unitario"] = _to_num(df["preco_unitario"])
    df["preco_total"] = _to_num(df["preco_total"])

    # regras mínimas de qualidade
    df = df[df["compra_date"].notna()]
    df = df[df["preco_unitario"].notna() & (df["preco_unitario"] > 0)]

    # --- load BigQuery ---
    bq = bigquery.Client(project=PROJECT)

    schema = [
        bigquery.SchemaField("ano_compra", "INT64"),
        bigquery.SchemaField("nome_instituicao", "STRING"),
        bigquery.SchemaField("cnpj_instituicao", "STRING"),
        bigquery.SchemaField("municipio_instituicao", "STRING"),
        bigquery.SchemaField("uf", "STRING"),
        bigquery.SchemaField("compra", "STRING"),
        bigquery.SchemaField("insercao", "STRING"),
        bigquery.SchemaField("codigo_br", "STRING"),
        bigquery.SchemaField("descricao_catmat", "STRING"),
        bigquery.SchemaField("unidade_fornecimento", "STRING"),
        bigquery.SchemaField("generico", "STRING"),
        bigquery.SchemaField("anvisa", "STRING"),
        bigquery.SchemaField("modalidade_compra", "STRING"),
        bigquery.SchemaField("tipo_compra", "STRING"),
        bigquery.SchemaField("capacidade", "STRING"),
        bigquery.SchemaField("unidade_medida", "STRING"),
        bigquery.SchemaField("unidade_fornecimento_capacidade", "STRING"),
        bigquery.SchemaField("cnpj_fornecedor", "STRING"),
        bigquery.SchemaField("fornecedor", "STRING"),
        bigquery.SchemaField("cnpj_fabricante", "STRING"),
        bigquery.SchemaField("fabricante", "STRING"),
        bigquery.SchemaField("qtd_itens_comprados", "INT64"),
        bigquery.SchemaField("preco_unitario", "NUMERIC"),
        bigquery.SchemaField("preco_total", "NUMERIC"),
        bigquery.SchemaField("source_year", "INT64"),
        bigquery.SchemaField("ingest_date", "DATE"),
        bigquery.SchemaField("source_object", "STRING"),
        bigquery.SchemaField("load_ts_utc", "STRING"),
        # campos silver
        bigquery.SchemaField("compra_ts", "TIMESTAMP"),
        bigquery.SchemaField("insercao_ts", "TIMESTAMP"),
        bigquery.SchemaField("compra_date", "DATE"),
    ]

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        time_partitioning=bigquery.TimePartitioning(field="compra_date"),
        clustering_fields=["uf", "codigo_br"],
    )

    print("Carregando em:", TARGET)
    job = bq.load_table_from_dataframe(df, TARGET, job_config=job_config)
    job.result()
    print("OK:", TARGET)

if __name__ == "__main__":
    main()
