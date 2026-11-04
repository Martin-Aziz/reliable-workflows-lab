# Publishing this standalone repository on GitHub

The project was authored and tested locally. The connected GitHub tool does not currently support creating a new repository, so **no GitHub repository has been created for this project yet**.

Option A — using the GitHub CLI (after installing it and logging in):

```bash
# Run from the extracted reliable-workflows-lab folder.
git init -b main
git add .
git commit -m "feat: implement phase 1 leased job queue"
gh repo create Martin-Aziz/reliable-workflows-lab --public --source=. --remote=origin --push
```

Option B — GitHub website:

1. Create a new, empty public repository called `reliable-workflows-lab` under `Martin-Aziz` (do not initialize it with a README/license, since these files are in the archive).
2. From the extracted project folder, run:

```bash
git init -b main
git add .
git commit -m "feat: implement phase 1 leased job queue"
git remote add origin https://github.com/Martin-Aziz/reliable-workflows-lab.git
git push -u origin main
```

Use GitHub authentication supported by your local Git setup. Do not embed a personal-access token in a command, URL, or repository file. Review all source files and the license before making the project public.

Once created, future phases should use feature branches, reviewable pull requests, technical acceptance gates, and user review before merge.
