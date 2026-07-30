# Run PulseGuard in GitHub Codespaces

GitHub Codespaces can run the complete PulseGuard Docker Compose environment in a browser-hosted Linux development machine. The repository configuration requests at least 8 CPU cores, 16 GB RAM, and 32 GB storage because the demo builds and runs 18 services.

## Create the Codespace

1. Open the PulseGuard repository on GitHub.
2. Select **Code** and then **Codespaces**.
3. Select **Create codespace on main**. If GitHub shows machine options, choose an 8-core or larger machine.
4. Wait for the browser editor and terminal to finish loading.

The configuration installs Docker-in-Docker and forwards the primary PulseGuard ports. The services do not start automatically, which avoids consuming compute until you are ready to demonstrate them.

## Start the complete demo

In the Codespaces terminal, run:

```bash
bash .devcontainer/start-pulseguard.sh
```

The first run builds every image from source and can take several minutes. When startup completes, Codespaces opens the Incident Console automatically. You can also use the **Ports** tab to open:

| Surface | Port |
|---|---:|
| PulseGuard Incident Console | 8095 |
| PulseGuard Investigation | 8096 |
| Scenario Controller | 8090 |
| Automation and Live Activity | 8097 |
| Predictive Console | 8098 |
| Locust | 8089 |
| Grafana | 3000 |
| Prometheus | 9090 |

Forwarded ports remain private by default. Keep them private for normal use. Share or make a port public only for a controlled demonstration, and return it to private immediately afterward.

## Optional real-AI mode

The default Codespaces demo uses deterministic mock investigation. For Azure OpenAI, add these as GitHub Codespaces secrets before creating or restarting the Codespace:

```text
LLM_PROVIDER=azure_openai
AZURE_OPENAI_ENDPOINT
AZURE_OPENAI_API_KEY
AZURE_OPENAI_DEPLOYMENT
```

For OpenAI, add:

```text
LLM_PROVIDER=openai
OPENAI_API_KEY
OPENAI_MODEL
```

Do not write these values into the repository or commit `.env`.

## Stop or destroy the demo

Stop containers while preserving the database and monitoring volumes:

```bash
bash .devcontainer/stop-pulseguard.sh
```

Delete all PulseGuard containers, local images, volumes, database, metrics, and the generated `.env`:

```bash
bash .devcontainer/destroy-pulseguard.sh
```

Stop or delete the Codespace from GitHub when the demonstration is over to avoid unnecessary compute and storage usage.
