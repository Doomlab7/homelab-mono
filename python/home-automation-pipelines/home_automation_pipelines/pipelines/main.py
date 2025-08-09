from typing import List

import httpx
from prefect import flow
from prefect import task


@task(retries=3)
def get_stars(repo: str):
    url = f"https://api.github.com/repos/{repo}"
    count = httpx.get(url).json()["stargazers_count"]
    print(f"{repo} has {count} stars!")


@flow(name="GitHub Stars")
def github_stars(repos: List[str]):
    for repo in repos:
        get_stars(repo)


if __name__ == "__main__":
    repos = ["pypeaday/dotfiles", "pypeaday/AltaCV", "waylonwalker/lockhart"]
    # run the flow!
    github_stars(repos)
