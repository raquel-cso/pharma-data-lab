#!/usr/bin/env bash
set -euo pipefail

: "${BUCKET:?Defina o BUCKET. Ex: BUCKET='seu-bucket' ./bps_to_landing.sh 2025}"

INGEST_DATE="${INGEST_DATE:-$(TZ=America/Sao_Paulo date +%F)}"
FORCE="${FORCE:-0}"
EXTRACT="${EXTRACT:-0}"

years=("$@")
if [ ${#years[@]} -eq 2 ]; then
  start="${years[0]}"; end="${years[1]}"
  years=()
  for ((y=start; y<=end; y++)); do years+=("$y"); done
fi

workdir="/tmp/bps_download"
mkdir -p "$workdir"

exists_gcs () {
  # retorna 0 se existe, 1 se não existe
  gcloud storage ls "$1" >/dev/null 2>&1
}

for YEAR in "${years[@]}"; do
  if [[ ! "$YEAR" =~ ^20[0-9]{2}$ ]]; then
    echo "Ano inválido: $YEAR (use 4 dígitos, ex: 2025)"
    exit 1
  fi

  url="https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/BPS/csv/${YEAR}.csv.zip"
  local_zip="${workdir}/${YEAR}.csv.zip"

  gcs_zip="gs://${BUCKET}/landing/bps/year=${YEAR}/ingest_date=${INGEST_DATE}/${YEAR}.csv.zip"

  echo "==> [$YEAR] Baixando: $url"
  echo "==> [$YEAR] Destino:  $gcs_zip"

  if exists_gcs "$gcs_zip"; then
    if [ "$FORCE" -ne 1 ]; then
      echo "==> [$YEAR] Já existe no GCS. Pulando (use FORCE=1 para sobrescrever)."
      continue
    fi
    echo "==> [$YEAR] FORCE=1: vai sobrescrever."
  fi

  curl -fL "$url" -o "$local_zip"
  if [ ! -s "$local_zip" ]; then
    echo "==> [$YEAR] ERRO: zip baixado vazio."
    exit 1
  fi

  # upload
  gcloud storage cp "$local_zip" "$gcs_zip"

  if [ "$EXTRACT" -eq 1 ]; then
    echo "==> [$YEAR] Extraindo CSV e enviando para raw/..."
    unzip -o "$local_zip" -d "$workdir" >/dev/null

    local_csv="${workdir}/${YEAR}.csv"
    if [ ! -s "$local_csv" ]; then
      echo "==> [$YEAR] ERRO: CSV não encontrado após unzip (esperado ${YEAR}.csv)."
      exit 1
    fi

    gcs_csv="gs://${BUCKET}/raw/bps/year=${YEAR}/ingest_date=${INGEST_DATE}/${YEAR}.csv"
    gcloud storage cp "$local_csv" "$gcs_csv"
    echo "==> [$YEAR] OK raw: $gcs_csv"
  fi

  echo "==> [$YEAR] OK!"
done

echo "Tudo concluído."
