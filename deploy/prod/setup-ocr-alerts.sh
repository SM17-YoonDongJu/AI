#!/usr/bin/env bash
# prod OCR 알림 — 스팟 회수 예고·인스턴스 상태 변화(stopped/running)·OCR 큐 적체를 Discord로 보낸다.
# EventBridge가 Discord 웹훅을 직접 부른다(API destination) — Lambda·SNS를 두지 않는다.
# 웹훅은 모니터링 스택과 같은 critical 채널(Secrets Manager infra/monitoring-dev의 ALERT_WEBHOOK_URL_CRITICAL).
#
# 스팟이 회수되면 워커는 조용히 멈추고 요청은 큐에 쌓이기만 한다(유실은 없다). 이 알림이 없으면
# 사용자가 "처리 중"에 멈춘 걸 먼저 알게 된다.
#
# 다시 돌려도 같은 상태가 된다. 인스턴스를 새로 띄웠다면(재생성·교체) 다시 돌려 규칙의 instance-id를 갱신한다.
# 관리자 자격증명으로 레포 루트에서 실행한다: AWS_PROFILE=brbs bash deploy/prod/setup-ocr-alerts.sh
# 비용: 연결(Secrets Manager 1개) 월 약 $0.40 + CloudWatch 알람 1개 월 $0.10 + 호출 건당(무시할 수준).
set -euo pipefail

export AWS_REGION="${AWS_REGION:-ap-northeast-2}"
INSTANCE_NAME="brbs-ocr-prod"
QUEUE_NAME="brbs-ocr-job-queue-prod"
ALARM_NAME="brbs-ocr-prod-queue-backlog"
BACKLOG_SECONDS=900          # 가장 오래된 OCR 요청이 15분 넘게 안 빠지면 알린다(문서 1건 최악 ≈ 10분)
CONNECTION="brbs-discord-alerts"
DESTINATION="brbs-discord-critical"
EVENTS_ROLE="brbs-events-discord-role"

IID=$(aws ec2 describe-instances \
  --filters "Name=tag:Name,Values=$INSTANCE_NAME" "Name=instance-state-name,Values=pending,running,stopping,stopped" \
  --query 'Reservations[0].Instances[0].InstanceId' --output text)
[ "$IID" != "None" ] || { echo "$INSTANCE_NAME 인스턴스가 없습니다" >&2; exit 1; }
WEBHOOK=$(aws secretsmanager get-secret-value --secret-id infra/monitoring-dev --query SecretString --output text \
  | python3 -I -c 'import json,sys; print(json.load(sys.stdin)["ALERT_WEBHOOK_URL_CRITICAL"])')

work=$(mktemp -d); trap 'rm -rf "$work"' EXIT

# 1) 연결 — API destination은 연결이 필수다. Discord 웹훅은 인증이 없어 쓰이지 않는 헤더를 둔다.
if ! aws events describe-connection --name "$CONNECTION" >/dev/null 2>&1; then
  aws events create-connection --name "$CONNECTION" --authorization-type API_KEY \
    --auth-parameters 'ApiKeyAuthParameters={ApiKeyName=X-Unused,ApiKeyValue=none}' >/dev/null
fi
for _ in $(seq 1 30); do
  [ "$(aws events describe-connection --name "$CONNECTION" --query ConnectionState --output text)" = "AUTHORIZED" ] && break
  sleep 2
done
CONN_ARN=$(aws events describe-connection --name "$CONNECTION" --query ConnectionArn --output text)

# 2) API destination — 웹훅 URL이 엔드포인트다(URL 자체가 비밀이라 레포·출력에 남기지 않는다).
if aws events describe-api-destination --name "$DESTINATION" >/dev/null 2>&1; then
  aws events update-api-destination --name "$DESTINATION" --connection-arn "$CONN_ARN" \
    --invocation-endpoint "$WEBHOOK" --http-method POST --invocation-rate-limit-per-second 1 >/dev/null
else
  aws events create-api-destination --name "$DESTINATION" --connection-arn "$CONN_ARN" \
    --invocation-endpoint "$WEBHOOK" --http-method POST --invocation-rate-limit-per-second 1 >/dev/null
fi
DEST_ARN=$(aws events describe-api-destination --name "$DESTINATION" --query ApiDestinationArn --output text)

# 3) EventBridge가 API destination을 부를 역할(이 destination만).
if ! aws iam get-role --role-name "$EVENTS_ROLE" >/dev/null 2>&1; then
  aws iam create-role --role-name "$EVENTS_ROLE" --description "EventBridge -> Discord alerts (API destination)" \
    --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"events.amazonaws.com"},"Action":"sts:AssumeRole"}]}' >/dev/null
fi
python3 -I -c 'import json,sys; print(json.dumps({"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"events:InvokeApiDestination","Resource":sys.argv[1].rsplit("/",1)[0]+"/*"}]}))' \
  "$DEST_ARN" > "$work/role-policy.json"
aws iam put-role-policy --role-name "$EVENTS_ROLE" --policy-name invoke-discord --policy-document "file://$work/role-policy.json"
ROLE_ARN=$(aws iam get-role --role-name "$EVENTS_ROLE" --query Role.Arn --output text)

# 4) 큐 적체 알람 — 인스턴스가 떠 있어도 워커가 죽었거나 못 따라가면 여기서 드러난다.
aws cloudwatch put-metric-alarm --alarm-name "$ALARM_NAME" \
  --alarm-description "prod OCR: 가장 오래된 요청이 ${BACKLOG_SECONDS}초 넘게 처리되지 않음" \
  --namespace AWS/SQS --metric-name ApproximateAgeOfOldestMessage --dimensions "Name=QueueName,Value=$QUEUE_NAME" \
  --statistic Maximum --period 300 --evaluation-periods 1 --threshold "$BACKLOG_SECONDS" \
  --comparison-operator GreaterThanThreshold --treat-missing-data notBreaching

# 5) 규칙과 대상 — Discord 메시지는 input transformer로 만든다.
python3 -I - "$work" "$IID" "$ALARM_NAME" "$DEST_ARN" "$ROLE_ARN" <<'PY'
import json
import sys

work, iid, alarm, dest_arn, role_arn = sys.argv[1:]
rules = {
    "brbs-ocr-prod-spot-interruption": (
        {"source": ["aws.ec2"], "detail-type": ["EC2 Spot Instance Interruption Warning"],
         "detail": {"instance-id": [iid]}},
        {"id": "$.detail.instance-id"},
        "🟠 [prod OCR] 스팟 회수 예고 — brbs-ocr-prod(<id>)가 2분 뒤 멈춥니다. "
        "그동안 OCR 요청은 큐에 쌓이고, 용량이 돌아와 다시 켜지면 처리됩니다.",
    ),
    "brbs-ocr-prod-state-change": (
        {"source": ["aws.ec2"], "detail-type": ["EC2 Instance State-change Notification"],
         "detail": {"instance-id": [iid], "state": ["stopped", "running", "terminated"]}},
        {"id": "$.detail.instance-id", "state": "$.detail.state"},
        "🔴 [prod OCR] brbs-ocr-prod(<id>) 인스턴스가 <state> 상태가 됐습니다. "
        "stopped·terminated면 OCR이 멈춘 것이고, running이면 워커가 restart 정책으로 다시 뜹니다.",
    ),
    "brbs-ocr-prod-queue-backlog": (
        {"source": ["aws.cloudwatch"], "detail-type": ["CloudWatch Alarm State Change"],
         "detail": {"alarmName": [alarm], "state": {"value": ["ALARM", "OK"]}}},
        {"state": "$.detail.state.value"},
        "🟡 [prod OCR] OCR 큐 적체 알람 <state> — ALARM이면 가장 오래된 요청이 15분 넘게 처리되지 않은 것입니다"
        "(인스턴스·컨테이너 상태와 워커 로그 확인). OK면 해소됐습니다.",
    ),
}
for name, (pattern, paths, text) in rules.items():
    # <변수>는 input transformer 자리표시자다 — JSON 문자열 안에 그대로 둔다.
    template = json.dumps({"content": text}, ensure_ascii=False)
    json.dump(pattern, open(f"{work}/{name}.pattern.json", "w"))
    json.dump([{"Id": "discord", "Arn": dest_arn, "RoleArn": role_arn,
                "InputTransformer": {"InputPathsMap": paths, "InputTemplate": template}}],
              open(f"{work}/{name}.targets.json", "w"), ensure_ascii=False)
PY
for name in brbs-ocr-prod-spot-interruption brbs-ocr-prod-state-change brbs-ocr-prod-queue-backlog; do
  aws events put-rule --name "$name" --event-pattern "file://$work/$name.pattern.json" --state ENABLED >/dev/null
  aws events put-targets --rule "$name" --targets "file://$work/$name.targets.json" >/dev/null
done

echo "alerts ready for $IID: rules=3 alarm=$ALARM_NAME destination=$DESTINATION"
