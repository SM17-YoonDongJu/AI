# Prod DB 암호화 가이드 (at-rest)

RDS PostgreSQL(`brbs-rds-dev`, `brbs-rds-prod`)의 저장소 레벨(at-rest) 암호화 현황과 운영 체크리스트.
확인일: 2026-08-06, `aws rds describe-db-instances` / `aws kms describe-key` 조회 기준(읽기 전용).

---

## 1. 현재 상태 — 이미 켜져 있음

| 항목 | dev (`brbs-rds-dev`) | prod (`brbs-rds-prod`) |
|---|---|---|
| StorageEncrypted | true | true |
| KMS Key | `aws/rds` (AWS 관리형 기본 키, 계정·리전 공용) | 동일 |
| MultiAZ | false | true |
| PubliclyAccessible | false | false |
| BackupRetentionPeriod | 7일 | 14일 |
| DeletionProtection | false | true |
| DB 스냅샷 | - | 자동 스냅샷 전부 `Encrypted: true` 확인됨 (7/27~8/4) |

**결론**: 볼륨·자동 백업·스냅샷 모두 이미 KMS로 암호화되어 있다. 신규로 켤 작업은 없다 — 아래는 "이미 된 것"을 깨지 않기 위한 운영 체크리스트다.

---

## 2. 왜 이걸로 충분한가 (위협 모델)

at-rest 암호화가 막는 것과 못 막는 것을 구분해야 한다.

| 위협 | at-rest(KMS) | 방어 계층 |
|---|---|---|
| 디스크/스냅샷이 계정 밖으로 물리적으로 유출 | 막음 | KMS (본 문서) |
| RDS 전송 구간 스니핑 | 안 막음(별개 계층) | TLS — `RDS_CA_PATH`, [`src/core/db.py`](../src/core/db.py) `_build_ssl()`. 비면 SSL 꺼져 접속 자체가 거부됨([deploy/README.md](README.md) §7-1) |
| DB 크리덴셜 유출 / 인가된 세션의 평문 조회 | **안 막음** — PG가 인증된 커넥션엔 투명하게 복호화해서 내려준다 | 예방: 저장 전 마스킹. 탐지: pgaudit |
| 저장 데이터 자체가 원문 PII | **안 막음** | 예방: 저장 전 마스킹(OCR 파이프라인, `ocr_results`) |
| "누가 언제 뭘 읽었는지" | 안 막음(탐지 아님) | pgaudit → CloudWatch (`velog-draft-rds-pgaudit-troubleshooting.md`) |

이 레포가 소유한 데이터(`ocr_results`, `corpus_catalog`)는 **마스킹(예방) + at-rest(물리 유출 방어) + pgaudit(탐지)** 세 계층이 이미 갖춰져 있다. 이미 마스킹된 데이터에 컬럼 암호화를 추가로 얹는 건 이 레포 범위에서는 실익이 낮다고 판단해 범위에서 제외했다.

> **참고(이 레포 범위 밖)**: pgaudit 조사 중 발견된 `users`/`reports`/`user_claims`(백엔드팀 소유, Spring Boot 관리)는 마스킹이 없어 원문 PII가 그대로 저장돼 있다. 이 테이블들은 크리덴셜 유출 시나리오에서 at-rest 암호화가 전혀 방어하지 못하는 상태다. 이 레포에서 구현할 사안은 아니므로 백엔드팀 공유용 참고로만 남긴다.

---

## 3. 운영 체크리스트

- [ ] **신규 RDS 인스턴스**는 생성 시 `--storage-encrypted` 필수로 체크(생성 후 켜기 불가 — 켜려면 스냅샷 복사 후 재생성 필요).
- [ ] **KMS 키는 AWS 관리형 기본 키(`aws/rds`)**다 — 계정 전체 RDS가 공유하며, 연 1회 자동 로테이션(AWS 정책, 계정에서 제어 불가), 커스텀 키 정책 불가. 인스턴스별로 decrypt 권한을 IAM에서 세분화하고 싶다면 CMK(고객 관리형 키) 전환이 필요하지만, 이는 스냅샷 복사→새 키로 복원→cutover가 필요한 무중단이 아닌 작업이라 별도 계획 없이는 권장하지 않는다.
- [ ] **스냅샷/리드리플리카는 원본 암호화를 자동 상속**한다 — 별도 조치 불필요. 단, 크로스 리전/크로스 계정으로 스냅샷을 복사할 경우 대상에서 사용 가능한 키로 재암호화가 필요하다(현재 해당 시나리오 없음).
- [ ] **RDS CA 번들(`rds-global-bundle.pem`) 만료 관리** — in-transit 계층이지만 at-rest와 함께 "DB 접속 보안"으로 같이 점검. AWS가 번들을 교체하면 워커가 SSL 핸드셰이크에서 막힌다. 갱신: `deploy/README.md` §7-1 명령 재실행.
- [ ] **prod `DeletionProtection: true` 유지** — 실수로 인스턴스 삭제 시 암호화 여부와 무관하게 데이터 자체가 소실됨.
- [ ] pgaudit 대상 role(`rds_pgaudit`)에 새 민감 테이블이 생기면 GRANT 추가 — 최신 목록은 `velog-draft-rds-pgaudit-troubleshooting.md` 참고.

---

## 4. 검증 명령 (읽기 전용)

```bash
# 인스턴스 암호화 상태
aws rds describe-db-instances --region ap-northeast-2 \
  --query 'DBInstances[].{ID:DBInstanceIdentifier,Encrypted:StorageEncrypted,KmsKeyId:KmsKeyId,MultiAZ:MultiAZ,BackupRetention:BackupRetentionPeriod,DeletionProtection:DeletionProtection}' \
  --output table

# 스냅샷 암호화 상속 확인
aws rds describe-db-snapshots --region ap-northeast-2 --db-instance-identifier brbs-rds-prod \
  --query 'DBSnapshots[].{ID:DBSnapshotIdentifier,Encrypted:Encrypted}' --output table
```

---

## 관련 문서

- [deploy/README.md](README.md) §7-1 — RDS SSL(in-transit) 필수 설정
- `velog-draft-rds-pgaudit-troubleshooting.md` — pgaudit 감사 로그 범위 설정, `users`/`reports` 평문 PII 발견 경위
- [src/core/db.py](../src/core/db.py) — asyncpg 풀의 SSL 컨텍스트 구성
- `migrations/ai/000_extensions.sql` — pgaudit 확장 활성화(RDS 파라미터 그룹 선행 필요)
