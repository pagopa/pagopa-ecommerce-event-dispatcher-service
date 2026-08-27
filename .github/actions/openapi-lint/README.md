# OpenAPI lint

Lints an OpenAPI spec with [Spectral](https://github.com/stoplightio/spectral) and fails the job on findings at or above a given severity. Replaces `zuplo/rmoa-action`, which depends on a cloud API key and only exposes numeric thresholds, with no way to disable a single rule.

The rules live in the repository, in `.spectral.yaml`.

## Usage

```yaml
- uses: actions/checkout@34e114876b0b11c390a56381ad16ebd13914f8d5 # v4

- name: OpenAPI lint - ${{ matrix.spec.version }}
  uses: ./.github/actions/openapi-lint
  with:
    spec_path: ${{ matrix.spec.path }}
```

The spec matrix stays in the calling workflow, because the number of specs differs per repository.

## Inputs

| Input | Required | Default | Description |
| --- | --- | --- | --- |
| `spec_path` | yes | | Path to the OpenAPI spec, relative to the repository root |
| `ruleset` | no | `.spectral.yaml` | Path to the Spectral ruleset |
| `fail_severity` | no | `warn` | Lowest severity that fails the job: `error`, `warn` or `info` |
| `fail_on_violation` | no | `true` | Set to `false` for a report-only rollout |
| `spectral_version` | no | `6.16.3` | Version of `@stoplight/spectral-cli` |
| `owasp_ruleset_version` | no | `2.0.1` | Version of `@stoplight/spectral-owasp-ruleset` |

## Behaviour worth knowing

**A lint that cannot run fails the job, in report-only mode too.** Spectral exits `1` when it finds results but `2` or more when it could not run at all, and in that case it writes no report. A gate that does not tell the two apart passes silently and stops protecting anything. An unloadable ruleset is a configuration error, not a finding, so `fail_on_violation: false` does not suppress it.

Two cases need more than the exit code, since Spectral reports them as ordinary findings: an unrecognised document yields exit `0` and one `unrecognized-format` **warning** having run no rules, and an unparseable spec yields `parser` findings. Both are configuration errors, so neither `fail_severity` nor `fail_on_violation` suppresses them.

**Noise is handled by severity, not by a budget.** There is deliberately no `max_warnings`. A rule too noisy to block on is downgraded in `.spectral.yaml`, by name and with the reason, so the exception is visible in review; a numeric budget hides findings by count and, as the RMOA rollout showed, only ever grows. Use `fail_severity: error` to let a repository migrate while its warnings are still being worked through, and `fail_on_violation: false` when nothing should block yet.

**Only errors and warnings are annotated on the pull request.** GitHub renders at most 10 annotations per type per step, so annotating info findings as well would push the actionable ones out of the view. Info findings are reported as counts, broken down by rule, in the job summary.

**The npm packages are installed next to the ruleset.** Spectral resolves the packages a ruleset extends starting from the directory of the ruleset file, and these repositories have no `package.json`. The install therefore targets `dirname(ruleset)` and creates only a `node_modules` directory, which is already ignored by git.

`openapi-lint.sh` installs the packages itself and reads the binary back from the same directory, so the path is derived in exactly one place. That is also why the action is a single step: handing an input-derived path between two steps means writing it to `GITHUB_ENV`, which CodeQL flags as environment variable injection, since a ruleset path containing a newline would let a caller inject arbitrary variables into the job.

The install runs every time and is never skipped on an existing `node_modules`: skipping would lint with whatever version happened to be there rather than the pinned one, so a local run could disagree with CI. It costs about 2 seconds once the packages are cached, and a CI checkout is always cold anyway. To run the script outside Actions:

```bash
SPEC_PATH=api-spec/event-dispatcher-api.yaml RULESET=.spectral.yaml \
  FAIL_SEVERITY=warn FAIL_ON_VIOLATION=true \
  SPECTRAL_VERSION=6.16.3 OWASP_RULESET_VERSION=2.0.1 \
  bash .github/actions/openapi-lint/openapi-lint.sh
```
