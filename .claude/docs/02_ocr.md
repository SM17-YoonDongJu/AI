## 노션 링크:https://app.notion.com/p/02-OCR-37530798f08f8175af29f28aba511dd6

## 참여 컴포넌트

- **Frontend** (React Native / Next.js): 파일 선택 및 업로드 UI
- **~~FastAPI**: 파일 수신, JWT 검증, S3 저장, Kafka 발행, 202 즉시 반환 (Spring Boot 미경유)~~
- **SpringBoot**: 파일 수신, JWT 검증, S3 저장, Kafka 발행, 202 반환
- **AWS S3**: 업로드 파일 암호화 저장
- **Kafka MSK** (`ocr-job-queue`): OCR 작업 비동기 전달
- **FastAPI OCR Worker** (GPU 노드): PaddleOCR, 문서 분류, 엔티티 추출, PII 마스킹
- **PostgreSQL**: OCR 결과·문서 메타데이터 저장

외부 OCR API(Google Vision, Azure OCR 등)는 개인정보보호법 위반 우려로 사용하지 않는다. 모든 처리는 로컬 GPU에서 수행된다.

---

## 소프트웨어 레이어 구조

**[Frontend]**

멀티파트 폼으로 PDF·JPG·PNG·TIFF 파일을 SpringBoot에 전송한다(ALB 직접 진입). 업로드 후 202 응답을 받는다. OCR은 내부 처리 단계이며 별도 알림이 없고, 리포트 생성 완료 시 FCM/APNs Push Notification으로 통합 안내된다.

**~~[FastAPI — 파일 수신 레이어]~~**

~~ALB를 통해 멀티파트 파일을 직접 수신한다(Spring Boot 미경유). JWT(RS256)를 스테이트리스 검증하고 파일명을 UUID로 치환한다. S3에 서버사이드 암호화(SSE-S3)로 저장한다. 업로드 완료 후 Kafka 토픽에 메시지를 발행하고 Frontend에 202 Accepted를 반환한다. 파일 원본은 S3에만 저장하며 로컬 디스크에 잔류시키지 않는다.~~

**[SpringBoot - 파일 수신]**

**[Kafka MSK — ocr-job-queue]**

springboot 파일 수신 레이어가 발행한 OCR 작업 이벤트를 OCR Worker가 소비한다.

**[OCR Worker — OCR 레이어]**

Kafka 메시지를 소비하면 S3에서 파일을 읽어 PaddleOCR(로컬 GPU)로 텍스트를 추출한다. 한국어 모델을 적용하며 표·인감·서명 영역 레이아웃 분석을 포함한다.

**[OCR Worker — 분류·추출·마스킹 레이어]**

첫 페이지 텍스트를 기반으로 문서 유형(진단서·보험증권·지급결과안내문·청구서·기타)을 판정한다. 문서 유형에 따라 진단명(KCD 매핑)·보험사명·상품명·지급금액 등 엔티티를 추출한다. 이후 주민번호·계좌번호·전화번호 등 PII를 정규식 + NER로 탐지하고 마스킹한다. 마스킹된 텍스트만 이후 파이프라인(RAG, LLM)으로 전달된다.

**[PostgreSQL]**

마스킹된 OCR 텍스트, 문서 유형, 추출 엔티티를 ocr_results 테이블에 저장한다.

---

## 데이터 흐름 (순서)

1. Frontend가 멀티파트 파일을 SpringBoot로 전송 (ALB 직접 진입)
2. SpringBoot가 JWT 검증 후 S3에 SSE-S3 암호화 저장 및 Kafka 발행
3. Frontend에 202 즉시 반환
4. OCR Worker가 Kafka 메시지 소비
5. S3에서 파일 읽어 PaddleOCR로 텍스트 추출 (로컬 GPU)
6. 문서 유형 분류 → 엔티티 추출 → PII 마스킹
7. PostgreSQL ocr_results에 저장
8. 리포트 생성 파이프라인(05번)으로 결과 전달
9. 손해사정사 서명 완료 이벤트 수신 시 ocr_results 해당 레코드 삭제
10. S3 원본 파일은 서명일 기준 3년 후 자동 만료 (S3 Lifecycle 정책)

---

## 컴포넌트 간 통신 방식

| 구간 | 방식 |
| --- | --- |
| Frontend → springboot | REST (multipart/form-data, ALB 직접 진입) |
| springboot → S3 | AWS SDK (PutObject, SSE-S3) |
| springboot → Kafka | Kafka Producer |
| OCR Worker → Kafka | Kafka Consumer (ocr-job-queue) |
| OCR Worker → S3 | AWS SDK (GetObject) |
| OCR Worker → PostgreSQL | asyncpg (Python) |

---

## 데이터 보존 및 삭제 정책

| 데이터 | 보존 기간 | 삭제 트리거 |
| --- | --- | --- |
| S3 원본 파일 | 손해사정사 서명일로부터 3년 | 보험금 청구권 소멸시효(3년) 기준 만료 시 자동 삭제 |
| `ocr_results` (마스킹 텍스트·엔티티) | 리포트 확정 전까지만 유지 | 손해사정사 서명 완료 이벤트 발생 시 즉시 삭제 |
| AI 리포트 초안 (JSONB) | 영구 보존 | 사용자 탈퇴 요청 시 별도 검토 |
| 최종 리포트 (PDF·DB 레코드) | 영구 보존 | 사용자 탈퇴 요청 시 별도 검토 |

**설계 근거**

- AI 리포트 초안은 손해사정사 검수·서명의 근거 자료로 DB에 보존한다
- 개인정보보호법 최소 수집·목적 외 보존 금지 원칙 준수

!02_사용자_문서_업로드_및_OCR_처리.png