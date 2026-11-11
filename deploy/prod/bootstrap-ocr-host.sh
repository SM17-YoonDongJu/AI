#!/usr/bin/env bash
# prod OCR 워커 EC2(brbs-ocr-prod) 초기 설치 — EC2 user-data로 첫 부팅 때 1회 실행된다.
# 대상 AMI: Ubuntu 24.04(SSM Agent 기본 탑재). GPU·NVIDIA 드라이버는 설치하지 않는다(CPU 워커).
#
# 여기서 하지 않는 것: .env.ocr 값 채우기(시크릿) — README 절차대로 SSM 세션에서 직접 쓴다.
# 컨테이너 기동도 하지 않는다 — 첫 기동은 deploy-prod.yml 배포가 한다.
set -euxo pipefail
export DEBIAN_FRONTEND=noninteractive

apt-get update
apt-get install -y docker.io docker-compose-v2 unzip curl
systemctl enable --now docker

# 배포 스크립트가 aws ecr get-login-password를 쓴다. snap 판은 SSM RunShellScript의 PATH에
# /snap/bin이 없어 못 찾을 수 있어, 공식 v2를 /usr/local/bin에 설치한다.
curl -fsSL https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip -o /tmp/awscliv2.zip
unzip -q /tmp/awscliv2.zip -d /tmp
/tmp/aws/install --update
rm -rf /tmp/aws /tmp/awscliv2.zip

# RDS는 SSL 연결만 허용한다 — compose가 이 번들을 컨테이너에 읽기 전용으로 마운트한다.
install -d -m 755 /opt/ocr/certs
curl -fsSL -o /opt/ocr/certs/rds-global-bundle.pem \
  https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem

# 시크릿(DB 비밀번호)이 들어갈 파일 — 자리만 만들고 권한을 먼저 잠근다.
install -m 600 /dev/null /opt/ocr/.env.ocr
