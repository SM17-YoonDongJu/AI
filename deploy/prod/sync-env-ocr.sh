#!/usr/bin/env bash
# prod OCR 호스트(brbs-ocr-prod)의 /opt/ocr/.env.ocr를 새로 쓴다. 설정은 Parameter Store(/brbs/prod/ocr/,
# String), 시크릿(DATABASE_URL)은 Secrets Manager(prod/ocr)에서 받는다 — 백엔드 deploy/prod/sync-env.sh와
# 같은 방식이다. root로 실행한다(SSM RunShellScript는 root, 직접 접속했다면 sudo bash).
#
# 호스트의 .env.ocr는 언제든 이 스크립트로 다시 만들 수 있는 사본이다 — 직접 고치지 말고 원본을 고친 뒤
# 다시 돌린다. 레포가 public이라 설정값도 레포에 두지 않는다.
# OCR_TAG만 예외다: 배포 워크플로(deploy-prod.yml)가 배포한 sha7을 기록하는 값이라 기존 파일 값을
# 그대로 이어 쓴다.
#
# 시크릿을 Parameter Store SecureString에 두지 않는 이유: 인스턴스 롤의 AmazonSSMManagedInstanceCore가
# ssm:GetParameter를 모든 파라미터에 허용하고 기본 키(aws/ssm)는 계정 안 누구나 복호화할 수 있어, k8s 노드
# 등 다른 인스턴스에서도 읽힌다. prod/ocr 시크릿은 이 인스턴스 롤만 읽는다. 그래서 설정 경로에
# SecureString이 섞이면 거부한다.
#
# 값을 못 받거나 필수 키가 빠지면 기존 .env.ocr를 건드리지 않고 실패로 끝난다. 값은 화면·로그에 출력하지 않는다.
set -euo pipefail

REGION="ap-northeast-2"
CONFIG_PATH="/brbs/prod/ocr/"
SECRET_ID="prod/ocr"
ENV_FILE="/opt/ocr/.env.ocr"
REQUIRED_KEYS="ECR_REGISTRY ENV AWS_REGION S3_BUCKET DATABASE_URL SQS_OCR_JOB_QUEUE_URL
SQS_REPORT_JOB_QUEUE_URL VLM_BASE_URL"
export PATH="/usr/local/bin:$PATH"   # bootstrap-ocr-host.sh가 AWS CLI v2를 여기 설치한다

umask 077
work=$(mktemp -d "$(dirname "$ENV_FILE")/.sync.XXXXXX")
trap 'rm -rf "$work"' EXIT

aws ssm get-parameters-by-path --region "$REGION" --path "$CONFIG_PATH" --recursive --output json > "$work/config.json"
aws secretsmanager get-secret-value --region "$REGION" --secret-id "$SECRET_ID" \
  --query SecretString --output text > "$work/secret.json"

# compose의 .env는 따옴표 없는 값 안의 $NAME을 다른 변수로 치환하고 " #" 뒤를 주석으로 잘라낸다. 작은따옴표로
# 감싸면 값이 그대로 쓰인다. 작은따옴표 안에는 작은따옴표·줄바꿈을 안전하게 넣을 방법이 없어 그런 값은 거부한다.
python3 - "$work" "$ENV_FILE" "$REQUIRED_KEYS" > "$work/env" <<'PY'
import json
import re
import sys

work, env_file, required = sys.argv[1], sys.argv[2], sys.argv[3].split()
values = {}
for param in json.load(open(f"{work}/config.json"))["Parameters"]:
    key = param["Name"].rsplit("/", 1)[1]
    if param["Type"] == "SecureString":
        sys.exit(f"{key}가 SecureString입니다 — 시크릿은 Secrets Manager prod/ocr에 둘 것(스크립트 상단 참고)")
    values[key] = param["Value"]
for key, value in json.load(open(f"{work}/secret.json")).items():
    if key in values:
        sys.exit(f"{key}가 설정과 시크릿 양쪽에 있습니다 — 한 곳에만 둘 것")
    values[key] = value
missing = [key for key in required if not values.get(key)]
if missing:
    sys.exit(f"필수 키가 비어 있습니다: {' '.join(missing)}")

# 배포 워크플로가 기록한 태그를 이어 쓴다(없으면 빈 값 — 첫 배포가 채운다).
tag = ""
try:
    for line in open(env_file):
        if line.startswith("OCR_TAG="):
            tag = line.split("=", 1)[1].strip().strip("'\"")
except FileNotFoundError:
    pass
if tag and not re.fullmatch(r"[0-9a-f]{7}", tag):
    sys.exit("기존 OCR_TAG가 7자리 sha가 아닙니다 — 배포 워크플로로 다시 배포할 것")
values.pop("OCR_TAG", None)

for key in sorted(values):
    value = str(values[key])
    if "'" in value or "\n" in value:
        sys.exit(f"{key} 값에 작은따옴표나 줄바꿈이 있어 .env에 안전하게 쓸 수 없습니다")
    print(f"{key}='{value}'")
print(f"OCR_TAG={tag}")
PY

mv "$work/env" "$ENV_FILE"
chmod 600 "$ENV_FILE"
echo "wrote $ENV_FILE ($(grep -c '=' "$ENV_FILE") keys): $(cut -d= -f1 "$ENV_FILE" | tr '\n' ' ')"
