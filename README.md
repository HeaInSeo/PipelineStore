# PipelineStore

DagEdit이 저작한 파이프라인을 저장하고 JUMI 실행 경로와 연결하기 위해 만든 설계 저장소입니다.

## 현재 상태

**구현은 아직 시작되지 않았습니다.** 2026-08-29 Pipeline Plane 독립 설계/반증 결과, current architecture는 `PipelineRevision commit/publication → Run preparation → Binding → Lowering → JUMI submission`을 하나의 **logical Pipeline authority boundary**로 닫았습니다.

중요:

- 이 logical authority는 **단일 process / deployment / repository를 의미하지 않습니다.**
- `PipelineStore` repo는 initial implementation home의 strong candidate이지만 canonical repository owner로 freeze되지 않았습니다.
- 기존 `docs/CERTIFIED_PIPELINE_STORE_DESIGN_v0.2_UNREVIEWED.md`는 현재 Q15/Q16/Q17/Q21/Q22/Q27 및 Pipeline Plane closure 이전의 문서이므로 **HISTORICAL / SUPERSEDED · NON-NORMATIVE**로 취급합니다.
- 특히 v0.2의 `stableRef`, node-level `dataBindings`, `mountPath`, legacy `RegisteredToolDefinition` 조회 중심 lowering, fixed NFS/process/API topology는 current architecture contract로 사용하지 않습니다.

## 현재 권위

Current platform authority는 Notion `00. Current Authority Router → 1. constitution → 2. architecture` 순서를 따릅니다. 이 repo의 과거 설계 문서는 historical evidence일 뿐 current platform authority를 대체하지 않습니다.

## 보존할 historical evidence

기존 v0.2에서 아래 아이디어는 historical evidence로 참고할 수 있습니다.

- immutable pipeline revision/content identity 필요성
- Lowering이 JUMI 바깥에 있어야 한다는 경계
- JUMI의 기존 `SubmitRun` / `GetRun` northbound 계약 재사용 방향

구체적인 PipelineRevision / Run / Binding / Lowering wire, storage, transport, process topology는 current architecture를 기준으로 새로 설계해야 합니다.

## 관련 문서

- `docs/CERTIFIED_PIPELINE_STORE_DESIGN_v0.2_UNREVIEWED.md` — **HISTORICAL / SUPERSEDED**
- `JUMI/docs/JUMI_EXECUTABLE_RUN_SPEC_DRAFT.ko.md`
