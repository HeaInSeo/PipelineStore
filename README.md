# PipelineStore

DagEdit이 저작한 파이프라인(Certified Pipeline)을 저장하고, JUMI가 실행할 수 있도록 내주는 컴포넌트입니다.

## 상태

다음 범위가 구현되어 main에 들어와 있습니다. 모두 bounded slice이며, 아래에 적지 않은 범위는 구현되지 않았습니다.

- **PIPE-I1** — immutable PipelineRevision 저장·exact read (`sqlitestore`, 단일 writer SQLite).
- **PIPE-I0** — pre-Run RunGenerationIntent core (`intent`): automatic 유일성, frozen revision, explicit operation idempotency, RunID attach. Store는 `MemoryStore` 참조 구현뿐이며 durable adapter는 별도 storage gate입니다.
- **PIPE-I2** — PipelineRevision → 실행 요청 lowering (`lowering`). parameter/asset/external binding은 아직 거부합니다.
- **PIPE-I3** — automatic admission local core (`intent.Service.AdmitAutomatic`). automatic intent를 만드는 public 경로는 `AdmitAutomatic` 하나뿐입니다.

현재 설계 권위는 Platform Spec Wiki의 Current Authority Router → Spec & Pipeline Authority Anchor입니다. 이 repo의 문서는 권위를 새로 정의하지 않습니다.

- v0.1 설계(`platform-docs/CERTIFIED_PIPELINE_STORE_DESIGN_v0.1.md`), v0.2 설계: **HISTORICAL / SUPERSEDED / NON-NORMATIVE**

## 관련 문서

- `platform-docs/CERTIFIED_PIPELINE_STORE_DESIGN_v0.1.md` (historical)
- `JUMI/docs/JUMI_EXECUTABLE_RUN_SPEC_DRAFT.ko.md`
