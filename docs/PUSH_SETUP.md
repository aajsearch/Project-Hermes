# Push from Cursor / command line (Project Hermes)

To allow `git push hermes ...` to work **without typing your password** (e.g. from Cursor’s terminal or when the assistant runs the command), use one of these.

---

## Option A: SSH (recommended)

The `hermes` remote is set to SSH. Once your Mac has an SSH key added to GitHub, push will use it and not prompt.

### 1. Create an SSH key (if you don’t have one)

In a terminal (outside Cursor if needed):

```bash
ssh-keygen -t ed25519 -C "your-email@example.com" -f ~/.ssh/id_ed25519_hermes -N ""
```

(`-N ""` = no passphrase, so it can be used non-interactively. Use a passphrase if you prefer and add the key to the agent once with `ssh-add`.)

### 2. Add the key to the macOS keychain and agent

```bash
ssh-add --apple-use-keychain ~/.ssh/id_ed25519_hermes
```

### 3. Add the **public** key to GitHub

- Copy the key:
  ```bash
  cat ~/.ssh/id_ed25519_hermes.pub
  ```
- GitHub → **Settings** → **SSH and GPG keys** → **New SSH key** → paste and save.

### 4. Test and push

```bash
ssh -T git@github.com
cd /path/to/Project-Hermes   # or Hades-prediction-market
git push hermes amaresh_botimplementation:main
```

The `hermes` remote is already set to `git@github.com:aajsearch/Project-Hermes.git`, so no URL change is needed.

---

## Option B: HTTPS with Personal Access Token (PAT)

If you prefer HTTPS:

1. GitHub → **Settings** → **Developer settings** → **Personal access tokens** → create a token with `repo` scope.
2. Store it in the macOS keychain (run once in your terminal):
   ```bash
   git credential-osxkeychain erase
   host=github.com
   protocol=https
   ```
   (Press Enter twice.) Then the next time you run `git push hermes ...` and Git prompts, use your GitHub **username** and the **token** as the password. The keychain will remember it.

The `hermes` remote is currently set to **SSH**. To use HTTPS instead:

```bash
git remote set-url hermes https://github.com/aajsearch/Project-Hermes.git
```

Then push; when prompted, use your GitHub username and the PAT as the password.

---

## Summary

- **Remote URL:** `hermes` is set to **SSH** (`git@github.com:aajsearch/Project-Hermes.git`) so that once your SSH key is on GitHub and in `ssh-add`, push works without a prompt.
- To push from Cursor or from a script, use **Option A** and ensure the key is in the agent (`ssh-add --apple-use-keychain ...`).
