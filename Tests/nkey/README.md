# Local NKey NATS for `Dext.Net.Nats.Tests`

Bare NKey user (no JWT resolver). Fixture seed matches the NATS docs sample.

## Start (keep cleartext `-js` on 4222 separately)

```bat
nats-server -c Tests\nkey\nats-nkey.conf
```

## Enable tests

```bat
set DEXT_NATS_NKEY_HOST=127.0.0.1
set DEXT_NATS_NKEY_PORT=4224
set DEXT_NATS_NKEY_SEED=SUACSSL3UAHUDXKFSNVUZRF5UHPMWZ6BFDTJ7M6USDXIEDNPPQYYYCU3VY
```

Or point at the seed / creds file:

```bat
set DEXT_NATS_NKEY_PORT=4224
set DEXT_NATS_NKEY_SEED_FILE=Tests\nkey\user.nk
rem set DEXT_NATS_CREDS_FILE=Tests\nkey\sample.creds
```

OpenSSL x86 `libcrypto-3.dll` must sit beside the test exe (same as TLS tests under `Output\Win32\Debug`).
