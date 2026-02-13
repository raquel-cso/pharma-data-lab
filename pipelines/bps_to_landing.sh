cat > bps_to_landing.sh << 'EOF'
#!/usr/bin/env bash
set -euo pipefail

# Uso:
#   BUCKET="seu-bucket" ./bps_to_landing.sh 2024 2025
# Opções:
#   INGEST_DATE=YYYY-MM-DD  (padrão: data de hoje em SP)
#   FORCE=1     -> sobrescreve no GCS se já existir
#   EXTRACT=1   -> extrai o CSV e envia para raw/bps/

: "${BUCKET:?Defina o BUCKET. Ex: BUCKET='seu-bucket' ./bps_to_landing.sh 2025}"

INGEST_DATE="${INGEST_DATE:-$(TZ=America/Sao_Paulo date +%F)}"
FORCE="${FORCE:-0}"
EXTRACT="${EXTRACT:-0}"

years=("$@")
if [ ${#years[@]} -eq 0 ]; then
  echo "Passe pelo menos um ano. Ex: ./bps_to_landing.sh 2024 2025"
  exit 1
fi

workdir="/tmp/bps_download"
mkdir -p "$workdir"

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

  # Se já existe no GCS e não é FORCE, pula
  if gsutil -q stat "$gcs_zip" 2>/dev/null; then
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

  if [ "$FORCE" -eq 1 ]; then
    gsutil cp "$local_zip" "$gcs_zip"
  else
    gsutil cp -n "$local_zip" "$gcs_zip"
  fi

  if [ "$EXTRACT" -eq 1 ]; then
    echo "==> [$YEAR] Extraindo CSV e enviando para raw/..."
    unzip -o "$local_zip" -d "$workdir" >/dev/null

    local_csv="${workdir}/${YEAR}.csv"
    if [ ! -s "$local_csv" ]; then
      echo "==> [$YEAR] ERRO: CSV não encontrado após unzip (esperado ${YEAR}.csv)."
      exit 1
    fi

    gcs_csv="gs://${BUCKET}/raw/bps/year=${YEAR}/ingest_date=${INGEST_DATE}/${YEAR}.csv"
    if [ "$FORCE" -eq 1 ]; then
      gsutil cp "$local_csv" "$gcs_csv"
    else
      gsutil cp -n "$local_csv" "$gcs_csv"
    fi
    echo "==> [$YEAR] OK raw: $gcs_csv"
  fi

  echo "==> [$YEAR] OK!"
done

echo "Tudo concluído."
EOF

chmod +x bps_to_landing.sh
