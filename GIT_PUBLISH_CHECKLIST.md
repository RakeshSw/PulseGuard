# Git Publishing Checklist

Use this checklist only after the final personal-laptop test passes.

## 1. Clean the source machine

```powershell
.\scripts\clean-pulseguard-completely-for-transfer-v1.ps1 `
  -ProjectRoot $PWD `
  -ConfirmDataLoss `
  -RemoveLocalSecrets `
  -RemovePatchBackups
```

This must leave no PulseGuard containers, volumes, database, metric history, runtime logs, or local secret files.

## 2. Review public content

Confirm that:

- the visible product name is PulseGuard
- no rejected former public branding remains
- internal `opsai-*` identifiers are described as compatibility names
- screenshots contain no usernames, keys, tokens, private URLs, or corporate information
- `.env.example` contains placeholders only
- the README startup instructions match the current Compose file
- all published code and assets are yours or redistributable

## 3. Run release verification

```powershell
.\scripts\verify-public-release.ps1 -ProjectRoot $PWD
```

Resolve every failure before continuing.

## Docker clean-room test

On a Docker-enabled Windows machine, run:

```powershell
.\scripts\test-pulseguard-public-release-e2e.ps1 `
  -ProjectRoot $PWD `
  -ConfirmDataLoss
```

Do not tag the public release until the script ends with `Docker E2E validation passed`. Keep the generated report outside the repository.

## 4. Initialize Git

```powershell
git init
git branch -M main
git add .
git status
git commit -m "Initial public release of PulseGuard"
```

Inspect `git status` and the staged diff before the commit:

```powershell
git diff --cached --stat
git diff --cached
```

## 5. Create the GitHub repository

Create an empty repository named `pulseguard` in GitHub. Do not add a generated README, license, or `.gitignore` because these files already exist locally.

Then connect and push:

```powershell
git remote add origin https://github.com/<YOUR_GITHUB_USERNAME>/pulseguard.git
git push -u origin main
```

## 6. Enable GitHub Pages

In the repository:

1. Open **Settings**.
2. Open **Pages**.
3. Select **Deploy from a branch**.
4. Select branch `main`.
5. Select folder `/docs`.
6. Save.

The static site will be published from `docs/index.html`.

## 7. Final public review

From a logged-out browser, verify:

- repository landing page
- GitHub Pages site
- README links
- license
- no secret files in commit history
- no broken local-only URLs presented as public endpoints
- project disclaimer and safety boundaries are visible
