"""AI 모델 선정 벤치마크 하네스 (report_worker LangGraph 대상).

목적: 여러 후보 챗 LLM을 우리 실제 파이프라인에 태워 **생성 속도**와 **RAG 정확성(근거성)**을
동일 조건에서 비교하고, 공개 벤치마크(스크리닝)와 합쳐 모델을 선정한다.

설계 원칙:
  - production 코드(impl) 무수정. `core.ai_client`를 런타임 래핑, `settings.llm_model` 런타임 교체.
  - Judge(출력 가드레일)는 고정 레퍼런스 모델로 라우팅 → 자기채점 편향 제거.
  - 도메인 정답(특약·지급률)은 날조하지 않는다. 결정론 라벨만 기본 제공, 나머지는 라벨 템플릿.

실행: `PYTHONPATH=src:scripts python -m benchmark.run --help`
전제: tempVectorDB(policy_chunks 적재) + Ollama(후보/임베딩 모델) 기동. README.md 참고.
"""
