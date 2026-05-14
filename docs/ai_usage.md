# AI Usage

Tools used:

- OpenAI Codex coding agent in this local workspace.
- Local Python 3.12 and `pytest` for executable checks.
- Official/public documentation from AWS, Google Cloud, ClickHouse, and the challenge repository for source verification.

AI helped generate candidate architectures, repo skeleton, test harness structure, documentation wording, adversarial review prompts, and audit checklists.

Human decisions in this packet:

- reliability boundary is accepted-event durability, not impossible browser-event durability;
- Kinesis-first MVP over MSK-first;
- source-label policy and refusal to present local simulation as a cloud benchmark;
- fallback semantics where failed fallback writes are not acknowledged;
- compliance boundary that separates technical erasure actions from legal policy;
- scope control around a reviewer demo harness instead of a broad architecture binder.

Weak spots:

- local simulation is not production proof;
- no AWS smoke test was run;
- source docs were verified, but this is not a paid architecture review;
- cost model uses editable assumptions, not quotes;
- AI drafted some wording and code, but claims were checked against generated evidence and source records.
