# Contributing to Dext.Nats

Thank you for contributing to Dext.Nats.

## Canonical repository

All development is coordinated through:

`https://github.com/usofm/dext_nats`

Open issues and pull requests against this repository. Do not target another `dext_nats` repository as the project's upstream.

## Clone and remotes

Recommended setup:

```bash
git clone https://github.com/usofm/dext_nats.git
cd dext_nats
git remote -v
```

Expected canonical remote:

```text
origin  https://github.com/usofm/dext_nats.git (fetch)
origin  https://github.com/usofm/dext_nats.git (push)
```

For an existing clone:

```bash
git remote set-url origin https://github.com/usofm/dext_nats.git
```

Do not configure `kitesoft/dext_nats` as an authoritative `upstream` remote. If external repositories are inspected for interoperability or historical research, treat them only as external references and do not automatically merge or synchronize from them.

## Development rules

- Preserve public API compatibility unless a deliberate breaking release is approved.
- Keep new implementation work feature-oriented and avoid growing large facade units unnecessarily.
- Add focused tests for behavior changes.
- Keep protocol and JetStream wire behavior compatible with NATS specifications.
- Preserve Apache-2.0 licensing and existing authorship/provenance.
- Hosted CI must remain green; Delphi compiler validation is required for changes that alter Delphi implementation paths.

See `UPSTREAM.md` for the repository independence policy.