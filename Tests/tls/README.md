# Local TLS NATS for `Dext.Net.Nats.Tests`

Self-signed fixture used by T-01..T-03 (`VerifyServerCertificate=False`).

## Start (keep cleartext `-js` on 4222 separately)

```bat
nats-server -c Tests\tls\nats-tls.conf
```

Working directory must be `Tests\tls` (relative `cert_file` / `key_file`), or start from that folder.

## Enable tests

```bat
set DEXT_NATS_TLS_HOST=127.0.0.1
set DEXT_NATS_TLS_PORT=4223
```

OpenSSL x86 DLLs (`libssl-3.dll`, `libcrypto-3.dll`) must sit beside the test exe
(already copied under `Output\Win32\Debug` from Delphi-Cross-Socket OpenSSL3 x86).
