import requests
import json
import os
import argparse as args

#Import required libraries.

# ---------------
# Configuration
# ---------------
parser = args.ArgumentParser(description="Fetch all repos for a GitHub organization")
parser.add_argument("--org", type=str, required=True, help="GitHub organization name")
args = parser.parse_args()

ORG_NAME = args.org  # Get org name from command line
token = os.getenv("GITHUB_TOKEN")  # GitHub token from environment

headers = {
    "Accept": "application/vnd.github.v3+json"
}
# -------------------------
# Get all repos in the org
# -------------------------
def get_all_repos(org): # Function to get all repos from specific organization "ORG_NAME". 
    repos = []
    page = 1

    # Fetch repos from the GitHub API page by page.
    while True:
        url = f"https://api.github.com/orgs/{org}/repos?per_page=100&page={page}"
        headers = {
            'Authorization': f'token {token}'
        }
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            print(f"Error fetching repos: {response.status_code}")
            break
        page_data = response.json()
        if not page_data:
            break
            # Breaks when no more repos to fetch, ie end of list.

        # Extracts neccesary info from each repo and stores it in a dictionary and moves to next page until no more repos remain.
        for repo in page_data:
            repos.append({
                "repo_id": repo["id"],
                "repo_name": repo["name"],
                "repo_owner": repo["owner"]["login"],  # Owner login (username)
                "org_name": org  # Add the organization name
            })
        page += 1
 
    return repos

# -----------------------
# Get all PRs for a repo
# -----------------------
def get_pull_requests(repo_info): # After extracting all repos, gets the PRs from each repo.
    org_name = repo_info["org_name"]
    repo_id = repo_info["repo_id"]
    repo_name = repo_info["repo_name"]
    repo_owner = repo_info["repo_owner"] 
    # Gets neccessary metadata from the PRs
    
    prs = []
    total_prs = 0
    merged_prs = 0
    last_merged_at = None
    page = 1

    # Fetch repos PRs from the GitHub API page by page.
    while True:
        url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/pulls?state=all&per_page=100&page={page}"
        headers = {
            'Authorization': f'token {token}'
        }
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            print(f"Error fetching PRs for {repo_name}: {response.status_code}")
            # Tells us if there was an error with fetching the repo PR and the status code of the error.
            break
        page_data = response.json()
        if not page_data:
            break
        for pr in page_data:

            # Fetch PR reviews to get the PR approvers.
            reviews_url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/pulls/{pr['number']}/reviews"
            reviews_resp = requests.get(reviews_url, headers=headers)
            approvers = []
            if reviews_resp.status_code == 200:
                reviews_data = reviews_resp.json()
                approvers = [r["user"]["login"] for r in reviews_data if r["state"] == "APPROVED"]

                # Want to make there is at least 1 reviewer for the PR if it was merged.
                if len(set(approvers)) > 0:
                    multiple_approvers = len(approvers) # Show number of approvers.
                    cr_passed = True #if it has at least one approver passed code review.
                else:
                    multiple_approvers = None
                    cr_passed = False
            else:
                # Tells us if there was an error collecting the reviewers for a PR, the PR number and the repo it is from.
                print(f"Warning: Could not fetch reviews for PR #{pr['number']} in {repo_name}")

            # Fetch status of the last commit to check if it has status checks and all required status checks passed.
            commit_sha = pr["head"]["sha"]
            status_url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/commits/{commit_sha}/status"
            status_resp = requests.get(status_url, headers=headers)

            has_checks = False
            all_checks_passed = False

            if status_resp.status_code == 200:
                status_data = status_resp.json()
                checks = status_data.get("statuses", [])  
                # list of individual checks for a PR.
    
                if len(checks) > 0:  # there is atleast one check for the PR.
                    has_checks = True
                    all_checks_passed = status_data.get("state") == "success"
                else:
                    # no checks configured for this PR.
                    has_checks = False
                    all_checks_passed = None #returns null as if there are no checks, then no output for if all checks have been passed. 
            else:
                print(f"Warning: Could not fetch status for commit {commit_sha} in {repo_name}")
                # Tells us if there was an error collecting the status for the PR, what the commit status was and the repo it is from. 

            # Extracts neccesary info from each repo pull request and stores it in a dictionary and moves to next page until no more repos remain. 
            prs.append({
                "repo_id": repo_id,
                "repo_name": repo_name,
                "repo_owner": repo_owner,
                "org_name": org_name,
                "pr_id": pr["id"],
                "pr_number": pr["number"],
                "title": pr["title"],
                "author": pr["user"]["login"],
                "merged_at": pr.get("merged_at"),
                "approvers": multiple_approvers,
                "CR_passed": cr_passed,
                "has_checks": has_checks,
                "CHECKS_PASSED": all_checks_passed
            })

            # We also want the number of pull request and merged requests, here we count number of prs and merged prs.

        page += 1
    return prs

# -----------------------
# Fetch and save to JSON
# -----------------------
if __name__ == "__main__":
    repos = get_all_repos(ORG_NAME)

    # Create a directory for the output files unless the directory already exits
    os.makedirs("../repo_data", exist_ok=True)

    for repo in repos:
        prs = get_pull_requests(repo)
        repo_data = {
            # all repo info fields directly
            "pull_requests": prs,
        }

        # Create a .json file for each repo with PR details and save in the directory we created.
        filename = os.path.join("../repo_data", f"{repo['repo_name']}.json")

        with open(filename, "w", encoding="utf-8") as f:
            json.dump(repo_data, f, indent=4)
        
        print(f"Saved data for {repo['repo_name']} → {filename}")
