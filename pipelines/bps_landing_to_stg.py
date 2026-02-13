import os, re, io, zipfile, unicodedata
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
from google.cloud import storage

SP = ZoneInfo("America/Sao_Paulo")

def snake(s: str) -> str:
    s = s.strip().lower()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s

def detect_sep(sample: bytes) -> str:
    # Heurística simples: conta ; vs ,
    text = sample.decode("utf-8", errors="ignore")
    return ";" if text.count(";") > text.count(",") else ","

def main(years: list[int]):
    bucket_name = os.environ["BUCKET"]
    landing_prefix = os.environ.get("LANDING_PREFIX", "landing/bps/")
    stg_prefix = os.environ.get("STG_PREFIX", "stg/bps/")

    ingest_date = os.environ.get("INGEST_DATE") or datetime.now(SP).strftime("%Y-%m-%d")

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    # Lista objetos zip no landing
    blobs = list(client.list_blobs(bucket, prefix=landing_prefix))
    zip_blobs = [b for b in blobs if b.name.endswith(".csv.zip")]

    if not zip_blobs:
        raise RuntimeError(f"Nenhum .csv.zip encontrado em gs://{bucket_name}/{landing_prefix}")

    # Filtra por ano (aceita tanto year=YYYY quanto YYYY.csv.zip)
    def blob_year(name: str) -> int | None:
        m = re.search(r"year=(20\d{2})", name)
        if m:
            return int(m.group(1))
        m = re.search(r"/(20\d{2})\.csv\.zip$", name)
        if m:
            return int(m.group(1))
        return None

    selected = []
    for b in zip_blobs:
        y = blob_year(b.name)
        if y is None:
            continue
        if not years or y in years:
            selected.append((y, b))

    if not selected:
        raise RuntimeError(f"Nenhum zip para anos {years} em gs://{bucket_name}/{landing_prefix}")

    for y, b in sorted(selected, key=lambda t: (t[0], t[1].name)):
        # tenta capturar ingest_date do path, se existir
        m = re.search(r"ingest_date=(\d{4}-\d{2}-\d{2})", b.name)
        ingest = m.group(1) if m else ingest_date

        print(f"\n==> Processando: gs://{bucket_name}/{b.name}")
        data = b.download_as_bytes()

        zf = zipfile.ZipFile(io.BytesIO(data))
        # pega o primeiro csv dentro do zip
        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csv_names:
            raise RuntimeError(f"Zip sem CSV interno: {b.name}")
        csv_name = csv_names[0]

        with zf.open(csv_name) as f:
            # detecta separador
            sample = f.read(4096)
            sep = detect_sep(sample)
            f.seek(0)

            # Lê tudo como string (seguro). Tipagem a gente melhora na Sprint 3.
            reader = pd.read_csv(
                f,
                sep=sep,
                dtype=str,
                encoding="utf-8",
                chunksize=200_000,
                low_memory=False
            )

            part = 0
            for chunk in reader:
                # normaliza colunas
                chunk.columns = [snake(c) for c in chunk.columns]

                # adiciona metadados úteis
                chunk["source_year"] = str(y)
                chunk["ingest_date"] = ingest
                chunk["source_object"] = f"gs://{bucket_name}/{b.name}"
                chunk["load_ts_utc"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"

                # grava parquet local
                local_file = f"/tmp/bps_{y}_{ingest}_part{part:05d}.parquet"
                chunk.to_parquet(local_file, index=False, engine="pyarrow", compression="snappy")

                # sobe para GCS
                gcs_key = f"{stg_prefix}year={y}/ingest_date={ingest}/part-{part:05d}.parquet"
                bucket.blob(gcs_key).upload_from_filename(local_file)

                print(f"   - OK: gs://{bucket_name}/{gcs_key} (rows={len(chunk)})")
                part += 1

        print(f"==> Concluído ano {y} (ingest_date={ingest})")

if __name__ == "__main__":
    # anos opcionais via args (ex: 2024 2025). Se vazio, processa tudo que estiver no landing.
    import sys
    years = [int(a) for a in sys.argv[1:] if re.fullmatch(r"20\d{2}", a)]
    main(years)
