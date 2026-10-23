# Validation status

Initial workspace validation, 2026-10-23:

- All five ADRs written before the core implementation; scheduling and production
  compatibility deliberately remain provisional.
- Source-level review completed for typed chaining, transactional staging,
  fencing/recovery, null representation, aggregate timestamps, window clocks,
  and explicit unsupported repartitioning.
- Python trace generator executed for all five scenarios (308–310 rows each);
  all shell scripts syntax-checked, Maven XML parsed, local documentation links
  checked, and staged changes checked for whitespace errors.
- Go build, gofmt, vet, tests, race detector, benchmarks, and Java differential
  oracle have **not run** in the authoring environment. Its `/usr/bin/go` is the
  Go board-game program, not the Go compiler; Maven is absent. Network requests
  to install the toolchain could not complete.
- GitHub connector identity resolved to SoraDaibu. It exposes no repository-create
  operation here. CLI publishing is blocked by unavailable network access;
  `gh auth status` also reports its configured token invalid. No public repository
  creation or upload is claimed.

The code is unverified until a real Go toolchain and the Java oracle run. CI is
included to execute these gates after publication; a workflow file is not a test
result. No benchmark winner or production EOS guarantee is claimed.

A temporary `bootstrap.yml` pull-request workflow can format the source and
export it through a job log, run the Go/race/example suite, execute the Java
oracle, and record both scheduling strategies. This provides a validation path
when the authoring workspace cannot install tools. Remove that workflow after
committing formatted source and obtaining passing ordinary CI checks. It has
read-only repository permissions and exports only project Go source.

Before publication, use `scripts/format.sh` and `scripts/validate.sh`, run the
independent Java suite and benchmark matrix, and update this status with actual
results. `scripts/publish.sh` performs those checks before creating a public repo.
