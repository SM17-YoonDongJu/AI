#!/usr/bin/env bash
# LocalStack ready 훅 — 로컬 dev용 SQS 큐를 생성한다(ocr-job-queue·report-job).
# LocalStack이 준비되면 자동 실행된다(/etc/localstack/init/ready.d). 워커는 .env.example의
# SQS_OCR_JOB_QUEUE_URL·SQS_REPORT_JOB_QUEUE_URL(http://localhost:4566/000000000000/<name>)로 접속한다.
# e2e 테스트는 자체적으로 유니크 큐를 만들므로 이 스크립트와 무관하다.
set -euo pipefail

awslocal sqs create-queue --queue-name ocr-job-queue
awslocal sqs create-queue --queue-name report-job

echo "[init-queues] created SQS queues: ocr-job-queue, report-job"
