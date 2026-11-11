#!/usr/bin/env bash
# corpus(brbs-etl) EC2 인스턴스 안에서 직접 실행 — check_ocr_deploy.sh와 동일한 방식으로
# 컨테이너/env/로그를 확인한다. corpus-worker는 OCR과 분리된 경량 인스턴스에 있다(brbs-etl).
set -euo pipefail

cd /opt/corpus

echo "=== docker compose ps ==="
docker compose -f docker-compose.corpus.yml --env-file .env.corpus ps

echo
echo "=== corpus-worker 상세(이미지/시작시각/재시작횟수) ==="
CID=$(docker compose -f docker-compose.corpus.yml --env-file .env.corpus ps -q corpus-worker)
docker inspect --format 'IMAGE={{.Config.Image}} STARTED={{.State.StartedAt}} STATUS={{.State.Status}} RESTARTS={{.RestartCount}}' "$CID"

echo
echo "=== .env.corpus 핵심 값(DATABASE_URL 비번·NOTION_TOKEN은 마스킹) ==="
grep -E '^(ENV|LOG_LEVEL|DATABASE_URL|RDS_CA_PATH|AWS_REGION|S3_BUCKET|NOTION_TOKEN|NOTION_SYNC_INTERVAL_SECONDS|CORPUS_CATEGORIES)=' .env.corpus \
  | sed -E 's#(DATABASE_URL=postgresql://[^:]+:)[^@]+(@)#\1****\2#' \
  | sed -E 's#(NOTION_TOKEN=).+#\1****#'

echo
echo "=== RDS CA 번들 마운트 확인 ==="
ls -la /opt/corpus/certs/rds-global-bundle.pem 2>&1 || echo "CA 번들 없음"

echo
echo "=== corpus-worker 최근 로그(40줄) ==="
docker logs --tail 40 "$CID"
