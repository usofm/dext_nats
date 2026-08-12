# Canonical Upstream

The canonical and authoritative upstream repository for **Dext.Nats** is:

`https://github.com/usofm/dext_nats`

## Project direction

`usofm/dext_nats` is maintained as an independent project. Development, releases, issues, pull requests, CI, architecture decisions, and documentation are owned here.

`kitesoft/dext_nats` is not an upstream dependency and must not be configured as the project's `upstream` Git remote. Historical commits, authorship, and Apache-2.0 notices are retained as required; historical provenance does not imply ongoing synchronization or governance.

## Canonical Git remote

For a normal clone:

```bash
git clone https://github.com/usofm/dext_nats.git
cd dext_nats
git remote -v
```

The canonical `origin` must be:

```text
https://github.com/usofm/dext_nats.git
```

If an existing working copy has another origin, reset it with:

```bash
git remote set-url origin https://github.com/usofm/dext_nats.git
```

Do not add `kitesoft/dext_nats` as an `upstream` remote for normal project development.

## Independence policy

- No automatic synchronization from `kitesoft/dext_nats`.
- No CI workflow may fetch, merge, rebase, or compare against `kitesoft/dext_nats` as an authoritative source.
- No release process depends on another `dext_nats` repository.
- New issues and pull requests belong in `usofm/dext_nats`.
- Public API and architecture decisions are governed by this repository.
- Historical license and commit attribution remain intact.

## GitHub fork-network status

GitHub may temporarily show this repository as belonging to a fork network until the repository owner uses **Settings → General → Danger Zone → Leave fork network**. That GitHub-hosting relationship is separate from the source-code and governance policy above.

After GitHub detaches the repository, `usofm/dext_nats` is expected to be reported as a standalone repository rather than as a fork.