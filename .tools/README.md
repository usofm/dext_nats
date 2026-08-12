# Local tools

Binary tools are intentionally not committed to the repository.

To install the Windows amd64 NATS Server used by the local integration tests:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/download-nats-server.ps1
```

The default version is `2.11.6`. Override it when validating against another server release:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/download-nats-server.ps1 -Version 2.11.6
```

Downloaded/extracted files stay under `.tools/` and are ignored by Git.
