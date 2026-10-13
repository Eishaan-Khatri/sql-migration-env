# Grader audit

*1 October 2026. Reproduce from the repository root with `python audit/exploit_probe.py`.*

The grader was probed with scripted policies that do **not** solve the migration, to see how much score it still awards. A grader that is safe to train against should hold all of them near the floor.

## Policies

| Policy | What it does |
|---|---|
| `no_op` | Leaves the seeded source database untouched |
| `schema_only` | Builds the exact target schema and migrates zero rows |
| `one_row_lost` | The correct answer with one row deleted from every table |
| `wrong_values` | Correct schema, keys and row counts, every non-key value wrong |
| `golden` | The reference solution (sanity check) |

## Results

| Task | no_op | schema_only | one_row_lost | wrong_values | golden |
|---|---|---|---|---|---|
| column-restructure | 0.68 | 0.50 | 0.92 | 0.76 | 0.99 |
| soft-delete-restoration | 0.64 | 0.50 | 0.95 | 0.76 | 0.99 |
| table-normalization | 0.29 | 0.50 | 0.88 | 0.76 | 0.99 |
| schema-version-merge | 0.28 | 0.50 | 0.96 | 0.76 | 0.99 |
| multi-entity-extraction | 0.29 | 0.50 | 0.90 | 0.76 | 0.99 |
| cascade-migration | 0.61 | 0.50 | 0.86 | 0.76 | 0.99 |
| dual-source-consolidation | 0.25 | 0.50 | 0.89 | 0.76 | 0.99 |
| data-poisoning-quarantine | 0.53 | 0.50 | 0.89 | 0.76 | 0.99 |
| **mean** | **0.45** | **0.50** | **0.90** | **0.76** | **0.99** |

## What this shows

1. **The floor is high.** Doing nothing scores 0.45 on average and 0.68 on the easiest task. Scores should be read against that floor, not against zero.
2. **Schema alone is worth half.** A correct but empty schema collects the full schema and integrity credit.
3. **Row count is paid separately from row content.** The row comparison gives 0.4 for matching the count, so a table of the right size with wrong values still earns data credit. That is why `wrong_values` reaches 0.76.
4. **Data loss is graded linearly.** Losing a row costs a few points. In a real migration, any lost row is a failure.
5. **The step reward does not hide this.** The reward is the change in score, so rewards over an episode sum to the final score minus the starting score. A policy trained on it optimises the final partial-credit score.

## The old model baseline table

- The README used to carry model scores for Qwen 32B and GPT-OSS 120B. They were measured on 12 April 2026, before the last two grader commits.
- Its data-poisoning row (0.02 and 0.15) is not reproducible on the current grader, where doing nothing scores 0.53.
- The table has been removed from the README until it is re-run. The old figures are kept in `exploit_probe.py` only for comparison against the floor.

## Planned fixes

- A strict terminal check: success only when the end state equals the reference on rows and constraints.
- Keep the progress score only as potential-based shaping.
- Report every score next to the `no_op` floor.
- Replace the regex SQL filter with SQLite's authorizer hook.
- Generate seeds procedurally so tasks cannot be memorised.
