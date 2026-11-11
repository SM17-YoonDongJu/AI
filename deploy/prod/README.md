# prod OCR 워커 — 전용 EC2(t3.large 스팟)

prod OCR 워커는 전용 EC2 `brbs-ocr-prod`(t3.large 스팟) 한 대에 `ocr-worker` 하나만 띄운다.
GPU는 따로 두지 않는다. 표 문서·저신뢰도 문서의 보조 전사만 dev 클러스터의 vLLM(`brbs-etl`
T4, PaddleOCR-VL)을 NodePort로 같이 쓴다.

```
Backend(prod) ──SQS brbs-ocr-job-queue-prod──▶ brbs-ocr-prod (ocr-worker, CPU)
                                                 │  PaddleOCR·NER: 자체 CPU
                                                 ├─ VLM 보조 ──▶ brbs-etl:30800 (vLLM, 꺼져 있으면 5초 뒤 OCR 엔진 결과로 대체)
                                                 ├─ RDS brbs_prod (ai_owner_prod)
                                                 └─ SQS brbs-report-job-queue-prod ──▶ report(prod, 백엔드 호스트)
```

| 파일 | 역할 |
|---|---|
| `docker-compose.ocr.yml` | 호스트 compose 정본. 배포 때마다 호스트 파일을 이걸로 덮는다 |
| `.env.ocr.example` | 호스트 `/opt/ocr/.env.ocr` 템플릿(시크릿 포함 실제 파일은 git 미커밋) |
| `bootstrap-ocr-host.sh` | EC2 user-data — Docker·AWS CLI·RDS CA 설치 |
| `sync-env-ocr.sh` | 호스트 `.env.ocr` 생성 — Parameter Store `/brbs/prod/ocr/` + Secrets Manager `prod/ocr` |
| `iam-ocr-prod-policy.json` | 인스턴스 역할 인라인 정책(SQS·S3 두 경로·설정/시크릿 읽기만) |
| `setup-ocr-alerts.sh` | 스팟 회수·인스턴스 상태·큐 적체 → Discord 알림(EventBridge) |
| `../../.github/workflows/promote-ocr-prod.yml` | 배포 — dev에서 검증한 `:<sha7>`을 승인 후 올린다 |

## 왜 이렇게

- **t3.large(8GB)**: 문서 1건 처리 최고 메모리가 A4 200dpi 3.85GB, 폰 사진 3.95GB다(PR #81).
  t3.medium(4GB)은 OS 몫(약 0.5GB)을 빼면 여기에 못 미친다. 2 vCPU x86에 4~8GB 사이 크기는 없다.
- **스팟**: SQS는 at-least-once라 회수돼도 처리 중이던 메시지는 가시성 타임아웃(600초) 뒤 재전달·
  멱등 재처리된다. 영구 요청 + 중단 동작 `stop`이라 회수 뒤에도 인스턴스 ID·사설 IP·EBS가 남는다.
- **백엔드 호스트와 분리**: OCR은 CPU를 몰아 쓰고 메모리가 튄다. t3는 CPU 크레딧을 호스트 단위로
  쓰므로 같이 두면 백엔드 API까지 느려진다. 분리해야 OCR만 스팟으로 내릴 수도 있다.
- **배포는 수동 승격**: `main`이 아직 쓰이지 않아 push 트리거 대신 `workflow_dispatch`로 sha7을 받는다.

## 선행 조건

1. **DB 롤** — `brbs_prod`에 `ai_owner_prod`가 있어야 한다. `deploy/schema_split.sql`을 prod 롤
   이름(`*_owner_prod`)으로 바꿔 마스터 계정으로 실행한다. 롤은 RDS 인스턴스 단위라 dev 롤(`ai_owner`)을
   그대로 쓰면 dev 자격증명이 prod DB에 닿는다.
   **OCR은 이 실행이 끝난 뒤에 처음 띄운다.** OCR이 기동마다 `migrations/ai`를 적용하므로, 먼저 뜨면
   `ai` 스키마·테이블 소유자가 첫 기동 계정으로 굳는다.
   - 알려진 이슈: `migrations/ai/007·008·011·012`의 self-grant가 백엔드 롤을 `app_owner`로
     하드코딩한다. prod에선 dev의 `app_owner`가 권한을 받고 `app_owner_prod`는 못 받는다. 고치기 전까지는
     prod용 `schema_split` 실행분에 011(`ai.ocr_results` 컬럼 SELECT)·012(`ai.report_job_failures`)
     권한을 `app_owner_prod`로 직접 넣는다.
2. **vLLM NodePort** — K8s-manifests `envs/dev/etl-vllm.values.yaml`의 `service.nodePort: 30800`이
   머지·Sync돼 있어야 한다.
3. **GitHub Environment `prod`** — 승인자 두 명(`donghoong2`·`jongyo0N`) + 자기 승인 금지라, 실행하지 않은 쪽이
   승인해야 배포된다. Deployment branches는 `dev`만 허용한다.
   승인자를 바꿀 땐 레포 Settings → Environments → `prod`에서 고친다.
   기존 시크릿(`AWS_ROLE_ARN`·`AWS_REGION`·`AWS_ECR_REGISTRY`·`DISCORD_WEBHOOK_*`)을 그대로 쓴다.
   OIDC 역할 `brbs-github-deploy`는 이미 모든 인스턴스에 `ssm:SendCommand`를 할 수 있어 바꿀 게 없다.

## 인프라 만들기 (1회)

계정 ID·SG ID 같은 값은 레포가 public이라 적지 않고 조회해서 쓴다.

```bash
export AWS_PROFILE=brbs AWS_REGION=ap-northeast-2

ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
read VPC SUBNET < <(aws ec2 describe-instances --filters Name=tag:Name,Values=brbs-backend-prod \
  --query 'Reservations[0].Instances[0].[VpcId,SubnetId]' --output text)   # 백엔드 prod와 같은 서브넷(2a)
K8S_SG=$(aws ec2 describe-security-groups --filters Name=group-name,Values=brbs-sg-k8s \
  --query 'SecurityGroups[0].GroupId' --output text)
RDS_SG=$(aws rds describe-db-instances --db-instance-identifier brbs-rds-dev \
  --query 'DBInstances[0].VpcSecurityGroups[0].VpcSecurityGroupId' --output text)
BUCKET=$(aws ssm get-parameter --name /brbs/prod/env/S3_BUCKET --query Parameter.Value --output text)

# 1) SG — 인바운드 없음(접속은 SSM). RDS 5432와 vLLM NodePort 30800에만 출발지로 넣는다.
OCR_SG=$(aws ec2 create-security-group --group-name brbs-sg-ocr-prod \
  --description "prod OCR worker (outbound only)" --vpc-id "$VPC" --query GroupId --output text)
aws ec2 authorize-security-group-ingress --group-id "$RDS_SG" --protocol tcp --port 5432  --source-group "$OCR_SG"
aws ec2 authorize-security-group-ingress --group-id "$K8S_SG" --protocol tcp --port 30800 --source-group "$OCR_SG"

# 2) IAM 역할 — SSM 접속·ECR pull + OCR 전용 SQS·S3.
aws iam create-role --role-name brbs-ocr-prod-role --assume-role-policy-document \
  '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
aws iam attach-role-policy --role-name brbs-ocr-prod-role --policy-arn arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore
aws iam attach-role-policy --role-name brbs-ocr-prod-role --policy-arn arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly
sed -e "s/REPLACE_ME_ACCOUNT_ID/$ACCOUNT/g" -e "s/REPLACE_ME_BUCKET/$BUCKET/g" \
  deploy/prod/iam-ocr-prod-policy.json > /tmp/ocr-prod-policy.json
aws iam put-role-policy --role-name brbs-ocr-prod-role --policy-name brbs-ocr-prod-access \
  --policy-document file:///tmp/ocr-prod-policy.json
aws iam create-instance-profile --instance-profile-name brbs-ocr-prod-profile
aws iam add-role-to-instance-profile --instance-profile-name brbs-ocr-prod-profile --role-name brbs-ocr-prod-role
sleep 15   # 인스턴스 프로파일이 EC2에 보이기까지 지연이 있다

# 3) 스팟 인스턴스 — 영구 요청 + 회수 시 stop. IMDSv2 hop limit 2(컨테이너가 역할 자격증명을 받으려면).
AMI=$(aws ssm get-parameter --name /aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id \
  --query Parameter.Value --output text)
aws ec2 run-instances --image-id "$AMI" --instance-type t3.large \
  --subnet-id "$SUBNET" --security-group-ids "$OCR_SG" \
  --iam-instance-profile Name=brbs-ocr-prod-profile \
  --instance-market-options 'MarketType=spot,SpotOptions={SpotInstanceType=persistent,InstanceInterruptionBehavior=stop}' \
  --metadata-options HttpTokens=required,HttpPutResponseHopLimit=2,HttpEndpoint=enabled \
  --block-device-mappings 'DeviceName=/dev/sda1,Ebs={VolumeSize=30,VolumeType=gp3,Encrypted=true,DeleteOnTermination=true}' \
  --user-data file://deploy/prod/bootstrap-ocr-host.sh \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=brbs-ocr-prod}]' \
                       'ResourceType=volume,Tags=[{Key=Name,Value=brbs-ocr-prod}]' \
                       'ResourceType=spot-instances-request,Tags=[{Key=Name,Value=brbs-ocr-prod}]'
```

- **루트 볼륨 암호화(`Encrypted=true`)**: 진단서 원본을 내려받아 처리하는 서버라, 처리 중 이미지·로그·
  컨테이너 레이어가 디스크에 남는다. 계정에 EBS 기본 암호화가 켜져 있으면 생략해도 암호화되지만, 이 명령은
  그 설정에 기대지 않는다.
- **S3는 두 경로만**: 원본 `report-documents/`(읽기·삭제)와 비식별 사본 `masked/`(쓰기·읽기). 같은 버킷의
  다른 경로(dev 채팅 첨부·자격증 파일·ETL corpus 등)에는 권한이 없다. OCR은 S3 오류를 모두 같은 실패로
  다루므로 `s3:ListBucket` 없이 404가 403으로 보여도 동작이 같다.
- `masked/` 업로드는 SSE-KMS(AWS 관리형 키 `aws/s3`)다. 관리형 키 정책이 계정 안 주체의 S3 경유 사용을
  허용하므로 별도 `kms:*` 권한은 필요 없다. OCR은 PII 컬럼 복호화(`core.crypto`)를 쓰지 않아 백엔드 KMS
  키도 필요 없다.

```bash
# 4) 알림 — 스팟 회수 예고·인스턴스 stopped/running·OCR 큐 적체(15분)를 Discord critical 채널로.
#    인스턴스를 새로 띄울 때마다 다시 돌린다(규칙의 instance-id 갱신).
bash deploy/prod/setup-ocr-alerts.sh
```

## 호스트 설정 (1회)

`.env.ocr`은 손으로 쓰지 않는다. 백엔드 `deploy/prod/sync-env.sh`와 같은 방식으로, 설정은
Parameter Store `/brbs/prod/ocr/`(String), 시크릿은 Secrets Manager `prod/ocr`에 두고
`sync-env-ocr.sh`가 호스트에서 받아 만든다. 키 목록은 `.env.ocr.example`과 같다.

| 위치 | 키 |
|---|---|
| Parameter Store `/brbs/prod/ocr/` | `ECR_REGISTRY` `ENV` `LOG_LEVEL` `AWS_REGION` `S3_BUCKET` `SQS_OCR_JOB_QUEUE_URL` `SQS_REPORT_JOB_QUEUE_URL` `VLM_BASE_URL` `VLM_MODEL` `USE_NER` |
| Secrets Manager `prod/ocr` | `DATABASE_URL` (`ai_owner_prod`, 비밀번호는 `prod/rds-admin`의 `AI_OWNER_PROD_PASSWORD`) |

값을 바꾼 뒤엔 스크립트를 다시 돌리고 워커를 다시 띄운다. 스크립트에 시크릿이 없어 SSM 명령으로 보내도 된다.

```bash
# user-data(bootstrap-ocr-host.sh)가 끝난 뒤 — 레포 루트에서
IID=$(aws ec2 describe-instances --filters Name=tag:Name,Values=brbs-ocr-prod Name=instance-state-name,Values=running \
  --query 'Reservations[0].Instances[0].InstanceId' --output text)
B64=$(base64 < deploy/prod/sync-env-ocr.sh | tr -d '\n')
aws ssm send-command --instance-ids "$IID" --document-name AWS-RunShellScript \
  --parameters "commands=[\"echo $B64 | base64 -d | bash\"]" --query Command.CommandId --output text
# 결과: aws ssm get-command-invocation --command-id <id> --instance-id "$IID" --query StandardOutputContent

# vLLM 도달 확인(SSM 세션에서) — PaddleOCR-VL-1.6-0.9B가 보여야 한다
curl -s http://<brbs-etl 사설 IP>:30800/v1/models
```

## 배포

dev에서 검증한 sha7로 실행한 뒤, Environment `prod` 승인자가 승인하면 배포된다.

```bash
gh workflow run promote-ocr-prod.yml --ref dev -f tag=<sha7>
```

기본 브랜치(`main`)에 이 파일이 없어서, 워크플로는 dev push 트리거로 등록된다(그 push 실행은 배포 없이
건너뛴다). Actions 탭의 Run workflow 버튼은 기본 브랜치 기준이라 안 보일 수 있으니 CLI(또는 API)로 실행한다.
승인 대기 중인 실행은 Actions 탭의 해당 실행 화면에서 승인자가 Review deployments로 승인한다.

1. 태그 형식(7자리 sha) 확인 → ECR에 이미지가 있는지 확인
2. `Name=brbs-ocr-prod`이고 running인 인스턴스 조회
3. SSM으로 compose를 레포 내용으로 덮고, `.env.ocr`의 `OCR_TAG` 갱신 → pull → up
4. 45초 뒤 컨테이너가 `running`이고 재시작 0회인지 확인해 실패를 드러낸다
5. Discord 알림(성공 DEPLOYMENT / 실패 CI_FAILURES)

**롤백**: 이전 sha7로 다시 실행한다.

## 운영

- **스팟 회수**: 2분 전 통지 → stop. 컨테이너는 SIGTERM 뒤 최대 60초 안에 내려간다. 용량이 돌아오면
  자동으로 start되고 `restart: unless-stopped`로 워커가 다시 뜬다. 그동안 OCR 요청은 큐에 쌓인다(유실 없음).
  회수 예고·stopped·running과 큐 적체(가장 오래된 요청 15분 초과)는 `setup-ocr-alerts.sh`가 만든
  EventBridge 규칙이 Discord critical 채널로 알린다. 회수 중에 배포하면 "running 인스턴스 없음"으로 실패한다.
- **영구 종료**: 스팟 요청을 먼저 취소해야 한다. 인스턴스만 terminate하면 영구 요청이 새로 띄운다.
  ```bash
  aws ec2 cancel-spot-instance-requests --spot-instance-request-ids <sir-id>
  aws ec2 terminate-instances --instance-ids <instance-id>
  ```
- **점검**: `docker ps`, `docker logs --tail 100 brbs-ocr-worker`, `grep OCR_TAG /opt/ocr/.env.ocr`.
- **비용**: t3.large 스팟 약 $0.04/시간(2026-11, 2a 기준, 월 약 $29) + gp3 30GB. T3는 기본
  `unlimited`라 평균 CPU가 기준선(30%)을 오래 넘으면 추가 과금된다. CloudWatch `CPUSurplusCreditsCharged`로 본다.
