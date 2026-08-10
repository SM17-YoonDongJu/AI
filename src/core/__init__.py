"""공용 인프라 계층 (config·contracts·logging·db·ai_client·sqs 등). 전 워커가 공유."""

# core는 공용 계층이라 이 경로 변경은 CI/CD paths-filter의 shared로 잡혀
# ocr·report·chatbot 세 서비스의 재빌드·재배포를 함께 유발한다.
