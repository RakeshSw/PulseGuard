# PulseGuard clean-room setup on Windows

This guide is intended for the final personal-laptop installation test.

## 1. Prerequisites

Install and start Docker Desktop. Confirm from PowerShell:

```powershell
docker version
docker compose version
docker info
```

## 2. Extract the package

Extract `PulseGuard-1.0.1-poc-clean-source.zip` to a short local path, for example:

```text
C:\Projects\PulseGuard-1.0.1-poc
```

Avoid running directly from inside the ZIP.

## 3. Open PowerShell in the project directory

```powershell
cd "C:\Projects\PulseGuard-1.0.1-poc"
```

If local policy blocks unsigned scripts, use a process-only bypass:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass -Force
```

This setting ends when the PowerShell window closes.

## 4. Start PulseGuard

```powershell
.\scripts\start.ps1
```

The script creates `.env` if it is absent and generates local random secrets. It then builds and starts the Compose environment and waits for the main endpoints.

## 5. Validate

```powershell
docker compose ps
.\scripts\status.ps1
.\scripts\test-opsai-v06.ps1
```

Open:

- http://localhost:8095/ - Incident Console
- http://localhost:8096/ - Investigation
- http://localhost:8090/ - Scenario Controller
- http://localhost:8089/ - Locust
- http://localhost:3000/ - Grafana

The generated Grafana password is stored only in `.env`:

```powershell
Select-String -Path .env -Pattern '^GRAFANA_ADMIN_(USER|PASSWORD)='
```

## 6. Enable a real AI provider

The default `LLM_PROVIDER=mock` is deterministic and should not be described as real AI.

For Azure OpenAI, edit `.env` and set:

```env
LLM_PROVIDER=azure_openai
AZURE_OPENAI_ENDPOINT=https://YOUR-RESOURCE.openai.azure.com
AZURE_OPENAI_API_KEY=YOUR_KEY
AZURE_OPENAI_DEPLOYMENT=YOUR_DEPLOYMENT_NAME
```

Then run:

```powershell
docker compose up -d --build --force-recreate opsai-agent
Invoke-RestMethod http://localhost:8096/health | ConvertTo-Json -Depth 6
```

## 7. Final test sequence

1. Confirm the initial incident and investigation counts are zero.
2. Confirm the Wikimedia traffic adapter is connected.
3. Run a latency scenario.
4. Confirm incident detection.
5. Review the investigation payload and response.
6. Review policy and approval behavior.
7. Confirm action execution and recovery verification.
8. Reset the scenario.
9. Run the full validation script.

## 8. Stop or delete

Preserve data:

```powershell
.\scripts\stop.ps1
```

Completely delete the local PulseGuard runtime:

```powershell
.\scripts\clean-pulseguard-completely-for-transfer-v1.ps1 `
  -ProjectRoot $PWD `
  -ConfirmDataLoss `
  -RemoveLocalSecrets
```


## Final clean-room release test

This test deletes all PulseGuard containers, volumes, images, and database data:

```powershell
.\scripts\test-pulseguard-public-release-e2e.ps1 `
  -ProjectRoot $PWD `
  -ConfirmDataLoss
```

Run it only on a Docker-enabled machine prepared for a destructive test.
