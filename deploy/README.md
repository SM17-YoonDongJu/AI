# CI/CD — AI 엔진 (OCR · Report · Chatbot)

이 레포의 CI/CD는 GitHub Actions 워크플로 하나(`.github/workflows/ci.yml`)로
**테스트 → 변경 감지 → 빌드·ECR push → EC2 SSM 배포**까지 잇는다.
`dev` 브랜치 기준으로 동작하며, 서비스별로 대상 EC2에 직접 배포한다.

---

## 1. 전체 흐름

```
dev push
  │
  ▼
[test]     ruff(lint) + pytest         ── 실패 시 여기서 중단(배포 안 함)
  │
  ▼
[changes]  변경 감지(paths-filter, base=dev) → 빌드할 서비스 matrix 생성
  │
  ▼
[deploy]   서비스별 병렬: build → ECR push → SSM 배포(호스트 pull/up) → 상태 폴링
```

- **test**: PR·push 공통. 배포의 전제(needs). 실패하면 배포는 시작조차 안 된다.
- **changes**: `dev` push 한정. 변경된 서비스만 골라 JSON matrix를 만든다.
- **deploy**: `dev` push + 변경분 존재 시에만. 서비스별로 병렬 실행(`fail-fast: false`).

> 왜 한 워크플로에 통합했나: 배포를 별도 파일 + `workflow_run`으로 두면 그 워크플로가
> 기본 브랜치(main)에 있어야만 트리거된다(GitHub 제약). 워크플로가 dev에만 올라가는
> 현 구조에선 dev 병합으로 CD가 안 붙으므로, `test → changes → deploy`를 `needs`로 잇는다.

---

## 2. 서비스 ↔ ECR ↔ 배포 대상

| 서비스 | Dockerfile | ECR 저장소 | 배포 대상 EC2 | 호스트 compose |
|--------|-----------|-----------|--------------|----------------|
| **ocr** | `src/ocr_worker/Dockerfile` | `soma/ocr` | GPU EC2 (`OCR_EC2_INSTANCE_ID`) | `/opt/ocr/docker-compose.ocr.yml` |
| **report** | `src/report_worker/Dockerfile` | `soma/report` | backend EC2 (`BACKEND_EC2_INSTANCE_ID`) | `/home/ubuntu/backend/docker-compose.yml` |
| **chatbot** | `src/chatbot/Dockerfile` | `soma/chatbot` | backend EC2 (동일) | 동일 |

- **3개 서비스 모두 이 레포 CI가 직접 SSM으로 배포**한다(별도 dispatch 없음).
- `matrix.target`으로 대상만 분기한다: `ocr` → GPU EC2, `backend` → backend EC2.
- 이미지 태그: `:dev`(가변, 호스트가 pull) + `:<sha7>`(불변, 감사·롤백용) 두 개를 push한다.

---

## 3. 변경 감지 규칙 (paths-filter)

`dev`의 직전 커밋과 비교해 아래 필터에 걸린 서비스만 빌드한다.

| 필터 | 경로 | 트리거되는 서비스 |
|------|------|------------------|
| `shared` | `pyproject.toml`, `uv.lock`, `src/core/**` | ocr · report · chatbot **전부** |
| `ocr` | `src/ocr_worker/**` | ocr |
| `report` | `src/report_worker/**`, `src/rag/**`, `src/guardrail/**` | report |
| `chatbot` | `src/chatbot/**`, `src/rag/**`, `src/guardrail/**` | chatbot |

- 공용 코드(`src/core` 등)를 바꾸면 3개가 전부 재빌드된다.
- 워크플로 파일(`.github/**`)만 바꾼 push는 어떤 필터에도 안 걸려 **deploy가 skip**된다.
  (배포까지 테스트하려면 서비스 코드나 `src/core`를 건드려야 한다.)

---

## 4. 배포 메커니즘 (SSM)

각 deploy job은 대상 EC2로 `aws ssm send-command`를 보내 호스트에서 다음을 실행한다.

```bash
aws ecr get-login-password | docker login ...
docker compose -f <compose> --env-file <envfile> pull  <svc>
docker compose -f <compose> --env-file <envfile> up -d --no-deps <svc>
docker image prune -f
```

- ECR push **직후 같은 job**에서 이어서 pull하므로, 가변 태그(`:dev`)라도 방금 올린 이미지를 받는다.
- `--no-deps`로 해당 서비스만 교체(인프라·다른 서비스 무영향).
- SSH가 아니라 **SSM Agent + IAM 권한**으로 실행한다(키 관리 불필요).
- 명령 후 최대 12분까지 SSM 실행 상태(Success/Failed)를 **폴링해 실제 배포 성공을 검증**한다.
  큰 이미지(OCR=torch/surya) pull이 기본 waiter(~100s)를 넘겨 오판되던 문제를 폴링으로 해소.
- 실패 시 호스트 stderr/stdout 마지막 줄을 워크플로 로그에 출력한다(원인 진단).

---

## 5. 필요한 GitHub 설정

**Secrets**

| 이름 | 용도 |
|------|------|
| `AWS_ROLE_ARN` | OIDC 역할 — ECR push, SSM SendCommand/GetCommandInvocation |
| `AWS_REGION` | 예: `ap-northeast-2` |
| `AWS_ECR_REGISTRY` | `<acct>.dkr.ecr.<region>.amazonaws.com` |
| `DISCORD_WEBHOOK_CI_FAILURES` | CI/배포 실패 알림 |
| `DISCORD_WEBHOOK_CI_BUILDS` | CI 성공 알림 |
| `DISCORD_WEBHOOK_DEPLOYMENT` | 배포 성공 알림 |

**Variables**

| 이름 | 용도 |
|------|------|
| `OCR_EC2_INSTANCE_ID` | GPU EC2 인스턴스 ID |
| `BACKEND_EC2_INSTANCE_ID` | backend EC2 인스턴스 ID |

**AWS 권한(OIDC 역할)**: `ecr:*`(push), `ssm:SendCommand`·`ssm:GetCommandInvocation`
(대상: OCR·backend 두 EC2). 각 EC2 인스턴스 롤에는 ECR pull 권한이 있어야 한다.

---

## 6. 호스트(EC2) 준비물

배포는 이미지만 밀어 넣는다. 각 호스트에는 아래가 **미리 배치**돼 있어야 한다.

### GPU EC2 (`/opt/ocr/`)
- `docker-compose.ocr.yml` — 이 레포 `deploy/docker-compose.ocr.yml`이 정본
- `.env.ocr` — 실제 값(시크릿 포함, git 미커밋). 템플릿: `deploy/.env.ocr.example`
- `certs/rds-global-bundle.pem` — RDS SSL CA 번들 (아래 참고)
- NVIDIA 드라이버 + nvidia-container-toolkit, SSM Agent

### backend EC2 (`/home/ubuntu/backend/`)
- `docker-compose.yml`(report·chatbot 서비스 포함) + `.env.dev` — **backend팀이 관리**
- SSM Agent (메시지 큐는 **AWS SQS 관리형** — backend EC2에 브로커 없음)
- 이 레포는 그 위에서 `pull/up`만 원격 실행한다.

---

## 7. 런타임 필수 설정 (자주 막히는 지점)

배포 파이프라인이 성공해도 아래가 안 갖춰지면 컨테이너가 크래시 루프에 빠진다.

1. **RDS SSL** — RDS는 SSL 연결만 허용(pg_hba). `RDS_CA_PATH`가 비면 `core.db`가 SSL을 꺼
   `no encryption`으로 거부된다. CA 번들을 호스트에 두고 compose 볼륨으로 마운트 후
   `.env`에 `RDS_CA_PATH=/etc/ssl/rds/global-bundle.pem` 지정.
   ```bash
   sudo curl -o /opt/ocr/certs/rds-global-bundle.pem \
     https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem
   ```
2. **pgvector 확장** — 워커 풀은 커넥션마다 `vector` 타입을 등록하므로 DB에 확장이 먼저 있어야
   풀 생성이 성공한다. 빈 DB는 최초 1회 `CREATE EXTENSION IF NOT EXISTS vector;` 필요
   (마이그레이션 `000_extensions.sql`).
3. **SQS 도달** — OCR/Report 워커는 AWS SQS 퍼블릭 엔드포인트에 붙는다. 프라이빗 서브넷이면
   VPC 엔드포인트(interface) 또는 NAT 경유가 필요하고, 워커 IAM Role에 SQS 권한(ReceiveMessage·
   DeleteMessage·GetQueueAttributes 등)이 있어야 한다.

---

## 8. CI/CD 테스트하는 법

1. `dev`에서 `src/core/config.py` 등 shared 파일에 무해한 변경(주석 한 줄)을 커밋.
2. `dev`로 push → test 통과 후 3개 서비스가 build→push→SSM 배포.
3. `gh run watch <run-id>` 또는 Actions 탭에서 각 deploy job의 SSM 폴링 결과 확인.
4. 호스트에서 컨테이너 상태·로그 확인:
   ```bash
   docker compose -f <compose> --env-file <env> ps        # STATUS: Up
   docker logs --tail 100 <container>                     # 에러 없이 기동
   ```

> 워크플로/문서만 바꾼 push는 deploy가 skip된다(§3). 실제 배포 검증에는 서비스/shared 변경이 필요.

---

## 9. 알림 (Discord)

- CI 실패 / 배포 실패 → `DISCORD_WEBHOOK_CI_FAILURES`
- CI 성공(push) → `DISCORD_WEBHOOK_CI_BUILDS`
- 배포 성공 → `DISCORD_WEBHOOK_DEPLOYMENT`

알림 제목엔 `AI` 접두사가 붙어 다른 레포와 구분된다.

---

## 관련 파일

- `.github/workflows/ci.yml` — 파이프라인 정의
- `.github/actions/discord-notify/` — 알림 액션
- `deploy/docker-compose.ocr.yml`, `deploy/.env.ocr.example` — GPU EC2 배포 정본
- `migrations/*.sql` — DB 스키마(확장·테이블)
