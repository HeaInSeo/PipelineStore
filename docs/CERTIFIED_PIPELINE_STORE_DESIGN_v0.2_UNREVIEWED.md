> # ⚠️ HISTORICAL / SUPERSEDED / NON-NORMATIVE
>
> **이 문서는 현재 권위가 아닙니다.** 역사적 기록으로만 보존되며, 설계 근거나 구현 기준으로 인용해서는 안 됩니다.
>
> 현재 권위는 다음 순서로 확인하십시오:
>
> **Current Authority Router → Constitution → Architecture**
>
> 2026-08-29 Pipeline Plane 독립 설계·반증 결과 이 문서의 모델은 대체되었습니다. 아래 본문은 당시 판단을 그대로 남긴 것으로, 이후 결정과 충돌하는 내용이 포함되어 있습니다.

# Certified Pipeline Store 설계 v0.2 (draft)

상태: 초안 (v0.1의 §10 미결정 사항 중 1~6번을 결정 반영, 7번 정책 스케줄러는 여전히 별도 컴포넌트로 범위 밖)
작성일: 2026-07-27
범위: Certified pipeline 서버 저장소, API 설계, lifecycle, JUMI 연동
비범위: DagEdit authoring UX, JUMI 내부 실행 상세, artifact-handoff 연동 상세, 정책 스케줄러 내부 설계(큐잉/리소스 할당/Kueue 통합)

---

## v0.1 대비 변경 요약

- §10 미결정 사항 7개 중 1~6번을 "결정됨"으로 승격(§10). 7번(정책 스케줄러 자체 설계)만 여전히 미해결로 남김 — 다만 Pipeline Store가 정책 스케줄러에 노출할 조회 API 계약은 §10.6에 추가.
- **중요 정정**: v0.1 §5(Certification 흐름) step 7 "JUMI 성공 → POST /certified → Pipeline Store"는 JUMI의 실제 northbound 계약(`JUMI_GRPC_CONTRACT_DRAFT.ko.md`)과 맞지 않는다. JUMI는 outbound 콜백을 하지 않는 northbound-only 계약이므로, 이 문서에서 Pipeline Store가 `GetRun`을 polling하는 흐름으로 수정했다(§5, §10.2).
- §4.2 REST API에 `PATCH /api/v1/certified/{pipelineCasHash}/deprecate` 신규 엔드포인트 추가(§10.5 deprecation UI 결정에 필요).
- §6 JUMI 연동에 "dry-run은 JUMI 신규 RPC가 아니라 기존 `SubmitRun`을 작은 sampleFixtureSet으로 호출하는 것"이라는 점과, bound-pipeline.json → ExecutableRunSpec 변환("lowering") 책임을 Pipeline Store가 진다는 점을 명시(§6.3, §10.1).
- §11 관련 문서에 JUMI gRPC/Executable Run Spec 문서, NodeGate 개념 설계, 플랫폼 정본 설계 §6-3 참조 추가.

---

## 1. 한 줄 정의

Pipeline Store는 DagEdit이 작성한 파이프라인을 저장하고, JUMI dry-run을 거쳐
certified된 파이프라인을 연구원들이 공유·실행할 수 있도록 제공하는 파일 기반 서비스다.

---

## 2. 설계 원칙

- **파일 기반 (CRD 아님)**: 빈번한 수정이 가능해야 하며 K8s API overhead 없음
- **Authored와 Certified 분리**: 작업 중인 파이프라인과 안정화된 파이프라인은 다른 경로에 저장
- **Certified는 불변**: 한 번 certified된 파이프라인은 수정 불가. pipelineCasHash로 pin
- **Tool과 동일한 재현성 수준**: pipelineCasHash로 완전 재현 가능
- **NFS 기반**: 이미 플랫폼에 있는 TrueNAS NFS 활용 (172.30.1.111:/mnt/RaidZ/NFSset)

---

## 3. 저장소 구조

### 3.1 NFS 디렉토리 레이아웃

```
/mnt/genomics-test/pipelines/
  authored/
    {pipelineId}/
      metadata.json              ← 파이프라인 기본 정보 (이름, 설명, author)
      {version}.pipeline.json    ← DagEdit이 저장하는 BoundPipeline 파일
  certified/
    {pipelineId}/
      {version}/
        bound-pipeline.json      ← 불변, pipelineCasHash로 검증
        certification.json       ← dry-run 결과, 인증 시점
```

### 3.2 파일 포맷

#### authored/{pipelineId}/{version}.pipeline.json

```json
{
  "pipelineId": "wgs-germline",
  "version": "v1.2",
  "description": "WGS germline variant calling pipeline",
  "author": "researcher-a",
  "updatedAt": "2026-06-02T10:00:00Z",
  "nodes": [
    {
      "nodeId": "bwa-node",
      "casHash": "sha256:aaa...",
      "imageDigest": "sha256:bbb...",
      "stableRef": "bwa@0.7.17"
    },
    {
      "nodeId": "samtools-node",
      "casHash": "sha256:ccc...",
      "imageDigest": "sha256:ddd...",
      "stableRef": "samtools@1.9"
    }
  ],
  "dataBindings": [
    {
      "bindingId": "ref-binding",
      "dataCasHash": "sha256:eee...",
      "dataManifestDigest": "sha256:fff...",
      "stableRef": "hg38-reference@2024.01",
      "mountPath": "/data/reference",
      "boundTo": ["bwa-node", "samtools-node"]
    }
  ],
  "edges": [
    {
      "from": "bwa-node",
      "fromPort": "alignment",
      "to": "samtools-node",
      "toPort": "reads"
    }
  ]
}
```

#### certified/{pipelineId}/{version}/bound-pipeline.json

authored와 동일한 구조 + 불변 필드:

```json
{
  "pipelineId": "wgs-germline",
  "version": "v1.2",
  "pipelineCasHash": "sha256:ggg...",
  "nodes": [ ... ],          ← imageDigest 고정
  "dataBindings": [ ... ],   ← dataManifestDigest 고정
  "edges": [ ... ]
}
```

**pipelineCasHash 계산:**
```
pipelineCasHash = SHA256(
  JSON.stringify({nodes, dataBindings, edges})  ← certifiedAt 제외
)
```
certifiedAt을 제외하는 이유: 동일한 파이프라인 구성은 인증 시점과 무관하게 같은 hash를 가져야 함.

#### certified/{pipelineId}/{version}/certification.json

```json
{
  "pipelineCasHash": "sha256:ggg...",
  "pipelineId": "wgs-germline",
  "version": "v1.2",
  "certifiedAt": "2026-06-02T12:00:00Z",
  "certifiedBy": "researcher-a",
  "deprecated": false,
  "dryRunResult": {
    "status": "succeeded",
    "jumiRunId": "dryrun-run-001",
    "durationSeconds": 142,
    "nodesExecuted": 2,
    "sampleFixtureSet": "wgs-small-smoke"
  }
}
```

`deprecated` 필드는 v0.1 §7에서 이미 예고된 필드를 여기서 명시적으로 스키마에 반영한다(§10.5 참고).

---

## 4. Pipeline Store 서비스

### 4.1 역할

```
[DagEdit (클라이언트)]
  authored pipeline 저장/수정 → Pipeline Store API
  certification 요청 → Pipeline Store API → JUMI 트리거

[연구원 (클라이언트)]
  certified pipeline 목록 조회 → Pipeline Store API
  파이프라인 선택 후 실행 요청 → JUMI

[정책 스케줄러 (배치 실행 시)]
  certified pipeline 명세 조회 → Pipeline Store API
  (큐잉/리소스 할당은 정책 스케줄러 내부 책임 — §10.6)

[NodeKit 관리 화면 (관리자)]
  certified pipeline deprecate 처리 → Pipeline Store API (§10.5)

[JUMI (서버)]
  dry-run(=작은 sampleFixtureSet SubmitRun) 완료 → Pipeline Store가 polling으로 확인 → certified 등록
  실제 실행 시 → Pipeline Store API로 bound-pipeline.json 조회
```

### 4.2 REST API

```
# Authored pipeline 관리 (DagEdit이 사용)
POST   /api/v1/pipelines
       body: {pipelineId, version, ...}
       → authored/{pipelineId}/{version}.pipeline.json 저장

GET    /api/v1/pipelines/{pipelineId}/versions
       → authored 버전 목록

GET    /api/v1/pipelines/{pipelineId}/versions/{version}
       → authored pipeline 파일 반환

PUT    /api/v1/pipelines/{pipelineId}/versions/{version}
       → authored pipeline 파일 덮어쓰기 (수정)

DELETE /api/v1/pipelines/{pipelineId}/versions/{version}
       → authored 삭제 (certified 영향 없음)

# Certification 요청 (DagEdit이 사용)
POST   /api/v1/pipelines/{pipelineId}/versions/{version}/certify
       body: {sampleFixtureSet: "wgs-small-smoke"}
       → Pipeline Store가 bound-pipeline을 ExecutableRunSpec으로 lowering
       → JUMI RunService.SubmitRun 직접 호출 (§10.1)
       → 202 Accepted + {certificationJobId}

GET    /api/v1/certifications/{certificationJobId}
       → dry-run 진행 상태 polling (Pipeline Store 내부적으로 JUMI GetRun을 polling한 결과를 반영, §10.2)

# Certified pipeline 관리 (연구원, 정책 스케줄러, NodeKit 관리 화면이 사용)
GET    /api/v1/certified
       → certified pipeline 목록 (최신 버전 우선, deprecated 기본 숨김)
       → query: ?pipelineId=wgs-germline, ?author=researcher-a, ?includeDeprecated=true

GET    /api/v1/certified/{pipelineCasHash}      ← JUMI 실행 시, 정책 스케줄러 조회 시 사용 (§10.1, §10.6)
       → bound-pipeline.json + certification.json 반환

GET    /api/v1/certified/{pipelineId}/versions  ← 연구원/정책 스케줄러 버전 이력 조회
       → 모든 certified 버전 목록

PATCH  /api/v1/certified/{pipelineCasHash}/deprecate   ← 신규 (v0.2), NodeKit 관리 화면이 사용 (§10.5)
       body: {deprecated: true, reason?: string}
       → certification.json의 deprecated 필드만 갱신 (bound-pipeline.json은 불변 유지)
```

**주의**: `POST /api/v1/certified`(JUMI가 dry-run 완료 후 호출하던 v0.1의 콜백 엔드포인트)는 v0.2에서 제거한다.
JUMI는 Pipeline Store에 콜백하지 않는다 — 이유는 §10.2 참고. certified 등록은 Pipeline Store 자신이
`GetRun` polling 결과를 보고 내부적으로 수행한다.

### 4.3 서비스 배치

```
seoy 호스트 바이너리로 실행 (NodeVault, NodePalette와 동일 패턴)
포트: :8090 (HTTP REST)
NFS 마운트: /mnt/genomics-test/pipelines/ → 파일 저장소
JUMI gRPC: → SubmitRun / GetRun 호출 (§10.1, §10.2)

systemd unit: pipeline-store.service
```

### 4.4 구현 단순성

초기 구현은 최대한 단순하게:
- 별도 DB 없음 — NFS 파일이 곧 저장소
- 서비스 간 mTLS/네트워크 정책만 적용, 사용자 단위 인증/인가는 아직 없음(§10.3)
- atomic write (temp file → rename) 적용
- index 파일 (`index.json`) 으로 목록 빠른 조회 지원

---

## 4-b. 파이프라인 실행 진입점 — 정책 스케줄러 (확정, §10.6 참고)

파이프라인 실행 요청은 **JUMI로 직접 가지 않는다** (단일 샘플 단일 파이프라인 예외).
대량 배치 실행은 반드시 **정책 스케줄러**를 경유한다.

```
[단일 샘플, 단일 파이프라인]
  DagEdit → JUMI  (직접 호출 가능)

[대량 샘플, 배치 실행]
  DagEdit (또는 Research Portal)
    → 정책 스케줄러  ← 현재 미설계, 미구현 (다른 트랙에서 별도 설계 중)
        - 실행 요청 큐잉
        - 리소스 할당 정책 적용
        - 우선순위 결정
        - 사용자/프로젝트 쿼터 관리
        - certified pipeline 조회는 Pipeline Store API 재사용 (§10.6)
    → JUMI (실행)
```

**정책 스케줄러 역할 (개념 수준):**
- 연구원이 "wgs-germline v1.2로 샘플 500개 분석 요청" 제출
- 스케줄러가 큐에 넣고 정책에 따라 순서/리소스 결정
- 리소스 가용 시 JUMI에 실행 요청 전달

**kube-slint는 정책 스케줄러가 아니다:**
kube-slint는 metrics 수집/평가 및 guardrail 관리 도구 (관측 레이어).
정책 스케줄러는 현재 미설계 — 별도 컴포넌트로 추후 설계 필요(이 문서 범위 밖).

**Pipeline Store와의 관계 (v0.2에서 인터페이스만 확정):**
- Pipeline Store: certified pipeline 명세 저장/제공
- 정책 스케줄러: 실행 요청 관리 및 JUMI 트리거 (내부 설계는 이 문서 범위 밖)
- 두 컴포넌트는 독립적(Pipeline Store가 스케줄러를 포함하지 않음)이며, 연결점은 §10.6에서 확정한
  기존 `GET /api/v1/certified/{pipelineCasHash}` 조회 API 하나뿐이다.

---

## 5. Certification 흐름 (v0.2에서 수정)

```
[DagEdit]
  1. 파이프라인 작성 완료
  2. POST /certify → Pipeline Store

[Pipeline Store]
  3. authored pipeline 읽기
  4. bound-pipeline(nodes/dataBindings/edges)을 ExecutableRunSpec으로 lowering
     (각 node의 stableRef/imageDigest로 NodeVault RegisteredToolDefinition 조회 → image/command/args/
     resourceProfile/timeoutPolicy 채움. §10.1 참고)
  5. JUMI RunService.SubmitRun 직접 호출
     (lowering된 spec + sampleFixtureSet 반영)
  6. SubmitRun 응답의 run_id를 저장, 202 Accepted + certificationJobId를 DagEdit에 반환

[Pipeline Store — 비동기]
  7. 짧은 backoff(예: 2s → 5s → 10s 상한)로 JUMI GetRun(run_id)을 polling
  8. RUN_STATUS_SUCCEEDED 도달 시:
     - pipelineCasHash 계산
     - certified/ 디렉토리에 저장 (certification.json.dryRunResult에 jumiRunId, durationSeconds 등 기록)
  9. RUN_STATUS_FAILED/CANCELED 도달 시: certificationJobId 상태를 실패로 기록(certified 등록하지 않음)

[DagEdit]
  10. GET /api/v1/certifications/{certificationJobId} polling으로 완료 확인

[연구원]
  11. GET /certified → 목록에서 파이프라인 선택
  12. 자신의 샘플 데이터 연결 → JUMI 실행 요청 (단일 실행) 또는 정책 스케줄러 경유(배치)
```

**v0.1과의 차이**: v0.1 step 7 "JUMI 성공 → POST /certified → Pipeline Store"는 JUMI가 Pipeline Store에
콜백한다고 가정했으나, JUMI northbound 계약에는 outbound 콜백이 없다(§10.2). v0.2는 Pipeline Store가
`GetRun`을 스스로 polling해서 완료를 확인하는 흐름으로 고쳤다.

---

## 6. JUMI 연동

### 6.1 JUMI가 certified pipeline을 읽는 방법

옵션 A — Pipeline Store API 경유:
```
JUMI → GET /api/v1/certified/{pipelineCasHash}
     → bound-pipeline.json 수신
     → K8s Job spec 생성
```

옵션 B — NFS 직접 읽기:
```
JUMI pod에 NFS 마운트
JUMI → /mnt/genomics-test/pipelines/certified/{id}/{ver}/bound-pipeline.json 직접 읽기
```

**권장: 옵션 A** (API 경유)
- 파일 경로 구조가 JUMI에 노출되지 않음
- 추후 저장소 변경 시 JUMI 코드 수정 불필요
- pipelineCasHash 검증을 Pipeline Store가 담당

### 6.2 실행 시 pin 검증

JUMI가 실행 전 반드시 확인:
```
bound-pipeline.json의 pipelineCasHash
= SHA256(JSON.stringify({nodes, dataBindings, edges}))
→ 불일치 시 실행 거부 (파일 변조 감지)
```

### 6.3 dry-run은 JUMI 신규 기능이 아니다 (v0.2 신규, §10.1 근거)

JUMI northbound 계약(`JUMI_GRPC_CONTRACT_DRAFT.ko.md`)에는 `RunService`에 `SubmitRun` / `GetRun` /
`ListRunNodes` / `ListNodeAttempts` / `ListRunEvents` / `CancelRun` 6개 unary RPC만 있고, 전용
dry-run RPC는 없다. `SubmitRun`은 이미 "등록 성공"과 "실행 완료"를 분리하는 비동기 계약이라
(같은 문서 §4.4), Pipeline Store가 작은 `sampleFixtureSet`(예: `wgs-small-smoke`)으로 `SubmitRun`을
호출하는 것 자체가 곧 "dry-run"이다. NodeSentinel의 L3 dry-run(`--dry-run=server`, 실제 실행 없이
manifest만 검증)과는 성격이 다르다 — Pipeline Store의 dry-run은 작은 fixture로 실제 K8s Job을
실행하는 "smoke run"에 가깝다. 따라서 JUMI에 새 RPC 추가를 요청할 필요가 없다.

### 6.4 Lowering 책임 (v0.2 신규, §10.1 근거)

`PLATFORM_CANONICAL_DESIGN_v1.0.md` §6-3(2026-07-24 코드 검증)은 "DagEdit → Pipeline Store →
Lowering → JUMI" 체인에서 authored pipeline을 JUMI가 이해하는 Executable Run Spec으로 변환하는
Lowering 컴포넌트가 플랫폼 어디에도 구현되어 있지 않다는 것을 확인했다(Go 코드베이스 전체 검색
결과 0줄). JUMI의 유일한 외부 진입점 `SubmitRun`은 이미 lowering이 끝난 spec만 받는다
(`JUMI_EXECUTABLE_RUN_SPEC_DRAFT.ko.md` §2, §14.1: "JUMI는 authored pipeline을 직접 해석하지 않는다").

v0.1 §5 step 4 "JUMI에 dry-run 요청 (pipelineSpec + sampleFixtureSet 전달)"은 이 변환 단계를
암묵적으로 건너뛰고 있었다. v0.2에서는 **Pipeline Store 자신이 이 lowering 책임을 진다**고 명시한다
— 별도 Lowering 컴포넌트를 새로 만들지 않는다(신규 컴포넌트 하나를 더 추가하는 것보다, 이미 JUMI를
직접 호출하기로 결정한 Pipeline Store의 `pkg/jumi/client.go`에 변환 로직을 두는 편이 더 단순하다).

```
pkg/jumi/client.go (v0.1 §9 패키지 구조에 이미 있던 파일, 책임 구체화)

func (c *Client) SubmitDryRun(
    ctx context.Context,
    boundPipeline store.BoundPipeline,
    sampleFixtureSet string,
) (jumiRunId string, err error) {
    // 1. boundPipeline.nodes의 stableRef/imageDigest로 NodeVault에서
    //    RegisteredToolDefinition을 조회 → image/command/args/resourceProfile/timeoutPolicy 확보
    spec := lowerToExecutableRunSpec(boundPipeline, sampleFixtureSet)

    // 2. 기존 JUMI RunService.SubmitRun 재사용 (신규 RPC 없음)
    resp, err := c.runServiceClient.SubmitRun(ctx, &jumipb.SubmitRunRequest{
        Spec: spec,
        Metadata: &jumipb.RequestMetadata{
            SourceSystem: "pipeline-store",
            TraceId:      traceIdFrom(ctx),
        },
    })
    if err != nil {
        return "", err
    }
    return resp.RunId, nil
}

func (c *Client) PollRun(ctx context.Context, jumiRunId string) (*jumipb.GetRunResponse, error) {
    return c.runServiceClient.GetRun(ctx, &jumipb.GetRunRequest{RunId: jumiRunId})
}
```

**주의**: `RegisteredToolDefinition` 조회는 Pipeline Store가 NodeVault의 기존 조회 API(읽기 전용)를
호출하는 것으로, NodeVault가 소유한 `RegisteredToolDefinition` 생성/등록 로직을 Pipeline Store가
대신 수행하는 것이 아니다. 어떤 NodeVault API(REST 또는 gRPC)를 재사용할지 구체적인 엔드포인트
선정은 이 문서 범위 밖이며, PipelineStore 저장소 구현 착수 시 NodeVault 쪽과 조율이 필요하다.

---

## 7. Versioning 전략

```
같은 pipelineId의 여러 version이 공존 가능:
  wgs-germline v1.0 (certified, deprecated 예정)
  wgs-germline v1.1 (certified, active)
  wgs-germline v1.2 (authored, dry-run 진행 중)

각 certified version은 독립적으로 불변.
v1.0으로 분석한 샘플은 언제나 v1.0으로 재현 가능.
```

**Deprecation (v0.2에서 구체화, §10.5 참고):**
certification.json에 `deprecated: true` 필드 추가 — `PATCH /api/v1/certified/{pipelineCasHash}/deprecate`로 갱신.
deprecated된 버전은 목록에서 기본적으로 숨김, 명시적 요청(`?includeDeprecated=true`) 시 노출.
삭제는 하지 않음 (재현성 보존). deprecation 처리 화면은 NodeKit 관리 화면에 둔다(§10.5).

---

## 8. 연구원 공유 흐름

```
[연구원 A — pipeline author]
  DagEdit에서 "wgs-germline v1.2" 작성
  Certification 요청 → dry-run 통과
  → 서버에 certified 등록

[연구원 B, C]
  Pipeline Store UI(또는 DagEdit) → GET /certified
  "wgs-germline v1.2 by researcher-a" 선택
  자신의 샘플 FASTQ 연결
  JUMI 실행 요청
  → 동일한 tool + data binding으로 분석
```

---

## 9. Pipeline Store 패키지 구조 (신규 repo)

```
github.com/HeaInSeo/PipelineStore/
  cmd/
    pipeline-store/
      main.go
  pkg/
    store/
      store.go        ← 파일 읽기/쓰기 (NFS)
      store_test.go
    api/
      handler.go      ← REST 핸들러
      handler_test.go
    cashhash/
      hash.go         ← pipelineCasHash 계산
      hash_test.go
    jumi/
      client.go       ← JUMI gRPC 클라이언트 (SubmitRun/GetRun 호출 + lowering, §6.3-6.4)
  deploy/
    pipeline-store.service   ← systemd unit
  go.mod
```

---

## 10. 결정된 사항 (v0.1 §10 미결정 사항 중 1~6번 승격)

### 10.1 Pipeline Store → JUMI dry-run 요청 방식

**결정**: JUMI gRPC 직접 호출(`RunService.SubmitRun`). 중간 큐를 도입하지 않는다.

**근거**:
- JUMI northbound 계약은 이미 6개 unary RPC(`SubmitRun`/`GetRun`/`ListRunNodes`/`ListNodeAttempts`/
  `ListRunEvents`/`CancelRun`)로 고정되어 있다(`JUMI_GRPC_CONTRACT_DRAFT.ko.md` §3). 별도의
  dry-run 전용 RPC는 없으며, 작은 `sampleFixtureSet`으로 `SubmitRun`을 호출하는 것 자체가 dry-run이다
  (§6.3 참고) — 새 RPC를 JUMI 팀에 요청할 필요가 없다.
- NodeVault→NodeSentinel 연동이 이미 같은 형태의 플랫폼 표준을 확립했다: `EnqueueValidationWork`
  gRPC를 직접 호출하고 즉시 반환, 처리는 비동기(`NODESENTINEL_VALIDATION_FLOW_SPEC_v0.1.md` §5.1).
- JUMI 자신이 큐 기반 입력을 명시적으로 거부한다: "Redis Streams는 JUMI 입력 계약이 아니다. JUMI는
  Redis 메시지를 직접 읽는 consumer가 아니라, 실행 명세를 받는 execution app이다"
  (`JUMI_EXECUTABLE_RUN_SPEC_DRAFT.ko.md` §14.3).

**대안 비교**:

| 방식 | 장점 | 단점 |
|---|---|---|
| 중간 큐(Redis Streams 등) | Pipeline Store 재시작에도 요청 유실 방지, backpressure 관리 용이 | JUMI가 큐 consumer 모델을 명시적으로 거부(§14.3); 새 인프라 컴포넌트 추가; 정책 스케줄러(§4-b)가 이미 "실행 요청 큐잉"을 맡기로 되어 있어 책임 중복 |
| gRPC 직접 호출(채택) | 기존 계약 재사용, 새 컴포넌트 불필요, NodeVault→NodeSentinel과 동일 패턴 | Pipeline Store 프로세스가 죽으면 진행 중이던 요청 추적을 자체적으로 복구해야 함(단, `SubmitRun`은 이미 등록된 `run_id`로 재개 가능하므로 영향은 제한적) |

**부수 결정**: bound-pipeline.json → ExecutableRunSpec 변환("lowering")은 별도 Lowering 컴포넌트가
플랫폼에 없으므로(`PLATFORM_CANONICAL_DESIGN_v1.0.md` §6-3) Pipeline Store가 직접 수행한다.
구체적 인터페이스는 §6.3, §6.4 참고.

### 10.2 JUMI dry-run 완료 통보 방식

**결정**: Pipeline Store가 `GetRun`을 polling한다. JUMI가 Pipeline Store에 콜백하지 않는다.

**근거**:
- JUMI 계약 문서는 명시적으로 "`SubmitRun` 성공은 등록 성공을 의미한다. 실행 완료까지 기다리는
  동기 API가 아니다"라고 하며, 표준 예시 흐름은 "caller가 `GetRun`으로 상태를 조회"다
  (`JUMI_GRPC_CONTRACT_DRAFT.ko.md` §4.4, §12). JUMI 계약 전체에 outbound 콜백/webhook 개념이
  존재하지 않는다 — northbound-only 설계다.
- NodeVault↔NodeSentinel 사이의 결과 전달도 콜백 RPC가 아니라 NodeSentinel이 결과를 NodeVault
  Index에 직접 쓰는 "공유 저장소 쓰기" 패턴이다(`PLATFORM_COMPONENT_MAP_v0.1.md` 데이터 흐름 절).
  이 패턴은 Pipeline Store↔JUMI 사이에는 적용하기 어렵다 — JUMI가 Pipeline Store의 NFS
  `pipelines/` 디렉토리에 쓰기 권한을 가질 이유가 없고, §6.1에서 이미 "JUMI는 Pipeline Store API로만
  읽는다"고 정했으므로 쓰기 방향도 대칭적으로 API(Pipeline Store가 능동적으로 확인) 쪽이 맞다.

**대안 비교**:

| 방식 | 장점 | 단점 |
|---|---|---|
| JUMI→Pipeline Store 콜백(v0.1이 가정했던 방식) | 지연 없이 즉시 알림, polling 부하 없음 | JUMI 계약에 outbound 책임이 없어 JUMI 팀에 새 기능(콜백 URL 등록, 재시도/타임아웃 처리)을 요청해야 함 |
| Pipeline Store가 `GetRun` polling(채택) | JUMI 계약을 전혀 바꾸지 않음, JUMI는 이미 하는 일(상태 조회 응답)만 하면 됨 | polling 주기만큼 완료 확인 지연, 약간의 폴링 부하(짧은 backoff로 완화) |

**정정 사항**: v0.1 §5 step 7 "JUMI 성공 → POST /certified → Pipeline Store"는 이 결정에 따라 v0.2
§5에서 "Pipeline Store가 `GetRun`을 backoff polling"으로 수정했다. v0.1 §4.2의 `POST /api/v1/certified`
엔드포인트(JUMI가 호출하는 콜백)는 v0.2 §4.2에서 제거했다.

### 10.3 인증/인가 — 단계적 위임

**결정 (단계적)**:
- **1단계 (현재, NodeGate 이전)**: Pipeline Store 자체는 사용자 단위 인증/인가를 구현하지 않는다.
  seoy 호스트 내부 네트워크 경계 + 서비스 간 mTLS/네트워크 정책만 적용한다(v0.1 §4.4의 "인증/인가
  없음(추후 추가)"보다 한 단계 구체화한 것 — 최소한 Pipeline Store `:8090`에 대한 접근을 알려진
  클라이언트(DagEdit, JUMI, NodeKit 관리 화면, 정책 스케줄러)로 제한하는 네트워크 정책은 초기부터 둔다).
- **2단계 (NodeGate 준비 후)**: `NODEGATE_CONCEPTUAL_DESIGN_v0.1.md` §6이 모델 B(토큰 발급자)를
  채택할 경우, Pipeline Store는 같은 문서 §9-3이 예고한 "각 데이터플레인 앱에 authz 미들웨어를
  추가하는 마이그레이션" 대상에 포함되어, NodeGate가 발급한 단기 토큰을 검증하는 미들웨어를 REST
  API 앞단에 추가한다. Pipeline Store 자체 신원 스토어는 새로 만들지 않는다.

**대안 비교**:

| 방식 | 장점 | 단점 |
|---|---|---|
| Pipeline Store 자체 authn/authz 구현 | NodeGate 완성을 기다리지 않아도 됨 | NodeGate가 나중에 나오면 이중 구현/마이그레이션 비용; NodeVault/JUMI 등 플랫폼 어디에도 authz 코드가 없다는 것이 이미 확인된 GAP(`NODEGATE_CONCEPTUAL_DESIGN_v0.1.md` §1)인데 Pipeline Store만 먼저 구현하면 오히려 플랫폼 일관성이 깨짐 |
| NodeGate로 위임(채택) | 플랫폼 전체 authz 모델과 일관, 중복 구현 없음 | NodeGate가 아직 모델 A/B조차 미정(같은 문서 §6, §9-2)이라 실제 마이그레이션 시점이 불확실 — 그 사이 Pipeline Store는 "네트워크 경계만"인 상태로 남는 리스크를 그대로 안는다 |

**정직한 한계**: 누가 certify 요청 가능한지, 누가 실행 가능한지에 대한 최종 세부 정책(예: certify
권한을 author 본인만 가지는지, 관리자도 가능한지)은 이 문서가 확정하지 않는다. NodeGate 자체가
아직 개념 설계(Draft v0.1) 단계이고 신원/테넌시 모델도 미정이라(`NODEGATE_CONCEPTUAL_DESIGN_v0.1.md`
§7), 그 위에 세부 정책을 지금 못박으면 NodeGate 쪽 결정이 바뀔 때 다시 뒤집힐 위험이 크다. 이 항목은
조직 결정(NodeGate 모델 확정, 신원 모델 확정)이 선행되어야 하는 항목으로 남긴다.

### 10.4 Pipeline Store 자체의 고가용성

**결정**: v0.2 범위에서 NFS 이중화/HA를 설계하지 않는다. Pipeline Store는 기존 TrueNAS NFS 마운트
(`/mnt/genomics-test/pipelines/`)를 그대로 쓰고, NFS 장애 시 Pipeline Store도 함께 불가용해지는
것을 알려진 리스크로 명시한다.

**근거 (정직한 리스크 인정)**: TrueNAS(`172.30.1.111`, HP ProLiant MicroServer Gen8 단일 박스,
`NodeVault/docs/TRUENAS_NFS_RUNBOOK.md`)는 이미 NodeVault, genomics 데이터 저장의 표준 스토리지로
운영 중이며(`PLATFORM_COMPONENT_MAP_v0.1.md`: "TrueNAS NFS — ✅ 운영 중"), 문서 어디에도 NFS 서버
자체의 이중화(active-active, failover)는 없다. 이는 Pipeline Store가 새로 짊어지는 리스크가 아니라
**플랫폼이 이미 감수하고 있는 리스크**다. Pipeline Store만 별도로 이 문제를 해결하려 하면 오히려
플랫폼 전체의 스토리지 전략과 어긋난다.

**현실적 완화책 (설계 변경이 아니라 운영 권고 수준)**:
- atomic write(temp file → rename, v0.1 §4.4에 이미 있음)는 유지 — 순간 장애 중 쓰기가 겹쳐도 부분
  쓰기로 인한 파일 손상 위험은 낮춘다.
- certified/ 디렉토리는 append-only(불변, §2)이므로, 정기 백업(TrueNAS snapshot 또는 rsync)만으로도
  복구 가능하다 — 이는 Pipeline Store 코드가 아니라 TrueNAS 운영 절차(`TRUENAS_NFS_RUNBOOK.md`) 쪽에
  위임한다.
- 별도 스토리지 백엔드 이전, NFS 다중화 등은 v0.2 범위에서 **명시적으로 제외**한다. 억지로 해법을
  만들지 않는다.

### 10.5 certified pipeline deprecation UI

**결정**: NodeKit 쪽 관리 화면(Phase 6 관리자 워크벤치의 일부)에서 처리한다. DagEdit에는 deprecation
관리 기능을 추가하지 않는다.

**근거**: NodeKit의 책임 경계 정의(`CLAUDE.md` §1)는 이미 `AdminToolList`/`AdminDataList`를
"Admin-only view of registered tools/datasets"로 NodeKit 소관으로 명시하고, "모든 admin UX
semantics(status feedback, error display, policy management UI)"도 NodeKit 소관으로 둔다. Certified
pipeline은 `RegisteredToolDefinition`과 마찬가지로 "certify(등록) 이후 불변 산출물"이라는 점에서
동일 패턴이며, 이 세션에서 이미 방향이 정해진 Phase 6 관리자 워크벤치(제안K: "DagEdit=저작,
NodeKit=관리")와 정확히 일치한다.

**구체적 반영**: deprecation은 certification.json의 `deprecated` 필드(§3.2, §7)를 세팅하는 것뿐이므로,
NodeKit 관리 화면에 "AdminPipelineList"(가칭) 뷰를 추가하고 v0.2에서 신설한
`PATCH /api/v1/certified/{pipelineCasHash}/deprecate`(§4.2)를 호출하는 흐름으로 구현한다.

**대안 비교**:

| 방식 | 장점 | 단점 |
|---|---|---|
| 별도 관리 화면(새 웹 UI) | Pipeline Store 전용 UI로 독립적 | 새 프론트엔드 프로젝트가 하나 더 생기고, Phase 6 워크벤치와 별개로 인증/세션 문제를 또 풀어야 함 |
| NodeKit 관리 화면에 통합(채택) | 기존 admin 워크벤치의 세션/인증/레이아웃 재사용, "NodeKit=관리" 원칙과 일관 | NodeKit이 Pipeline Store REST API를 새로 알아야 함 — 현재 NodeKit `CLAUDE.md` 범위 문서에는 Pipeline Store 연동이 아직 없으므로, 실제 구현 시점에는 NodeKit 쪽 스코프 문서에도 이 통합이 추가되어야 한다(이 문서는 그 자체를 NodeKit에 지시하는 문서는 아니다) |

### 10.6 Pipeline execution 요청 진입점

**결정 (v0.1 §4-b 그대로 확정)**:
- 단일 샘플·단일 파이프라인 실행: DagEdit → JUMI 직접 호출.
- 대량 샘플·배치 실행: DagEdit(또는 Research Portal) → 정책 스케줄러(큐잉/리소스 할당/우선순위/쿼터) → JUMI.
- kube-slint는 정책 스케줄러가 아니다(관측 도구일 뿐).

이 결정은 추가 논의 없이 v0.1 §4-b 내용을 그대로 확정안으로 반영한 것이다.

**Pipeline Store ↔ 정책 스케줄러 인터페이스 계약 (v0.2 신규, 정책 스케줄러 내부 설계는 범위 밖)**:
정책 스케줄러가 certified pipeline 명세를 조회할 때는 §4.2의 기존 조회 API를 그대로 재사용한다 —
정책 스케줄러 전용 신규 API를 만들지 않는다.

```
정책 스케줄러 → GET /api/v1/certified/{pipelineCasHash}
             → bound-pipeline.json + certification.json 수신
             → (이후 큐잉/리소스 배정/JUMI 트리거는 정책 스케줄러 내부 책임, 이 문서 범위 밖)
```

정책 스케줄러는 연구원 클라이언트와 동일하게 pipelineCasHash로 pin된 사양을 읽기만 하면 되므로,
JUMI 실행 시 사용하는 것과 동일한 엔드포인트를 공유해도 무방하다. 큐잉 알고리즘, Kueue 통합 등
정책 스케줄러 내부 설계는 별도 트랙에서 다룬다.

### 10.7 정책 스케줄러 설계 자체 — 여전히 범위 밖

v0.1과 동일하게 미해결로 남긴다. 이 항목(큐잉 알고리즘, 리소스 할당 정책, Kueue 통합 등)은 다른
설계 트랙에서 별도로 진행 중이며, 이 문서는 §10.6에서 확정한 조회 인터페이스 계약 이상을 다루지 않는다.

---

## 11. 관련 문서

| 문서 | 경로 |
|------|------|
| 플랫폼 아키텍처 결정 | `batch-integration/docs/master-plan/PLATFORM_ARCHITECTURE_DECISIONS_v0.1.md` |
| JUMI Executable Run Spec | `JUMI/docs/JUMI_EXECUTABLE_RUN_SPEC_DRAFT.ko.md` |
| JUMI gRPC Contract Draft | `JUMI/docs/JUMI_GRPC_CONTRACT_DRAFT.ko.md` |
| JUMI Durable Registry Options | `JUMI/docs/JUMI_DURABLE_REGISTRY_OPTIONS.ko.md` |
| TrueNAS NFS Runbook | `NodeVault/docs/TRUENAS_NFS_RUNBOOK.md` |
| NodeSentinel Validation Flow Spec | `NodeSentinel/docs/NODESENTINEL_VALIDATION_FLOW_SPEC_v0.1.md` |
| NodeGate 개념 설계 | `NodeGate/docs/NODEGATE_CONCEPTUAL_DESIGN_v0.1.md` |
| 플랫폼 정본 설계 (§6-3 Pipeline Store/Lowering 공백 확인) | `platform-docs/PLATFORM_CANONICAL_DESIGN_v1.0.md` |
| 플랫폼 컴포넌트 지도 (TrueNAS NFS 운영 상태, NodeVault↔NodeSentinel 데이터 흐름) | `platform-docs/PLATFORM_COMPONENT_MAP_v0.1.md` |
