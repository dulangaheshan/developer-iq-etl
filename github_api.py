# Author: Dulanga Heshan
# Date Created: YYYY-MM-DD
# Date Updated: YYYY-MM-DD
from time import sleep
from datetime import datetime, timedelta
from utils.dynamodb_handler import create_users_table, add_or_update_user, add_or_update_repository, \
    create_repositories_table, create_events_table, add_event
#
# # using an access token
# auth = Auth.Token("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")
access_token = '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'

import requests


def incremental_load_repos(org_name):
    # create_repositories_table()
    # create_users_table()
    create_events_table()
    # sleep(30)
    url = f'https://api.github.com/orgs/{org_name}/repos'
    headers = {'Authorization': f'token {access_token}'}
    response = requests.get(url, headers=headers)
    for repository in response.json():
        # add_or_update_repository(repository)
        # get_contributors_for_repository(org_name, repository['name'])
        events = get_events_for_date(org_name, repository['name'])
        print(events)
        for event in events:
            extract_event_details(event)


def get_contributors_for_repository(org_name, repo_name):
    url = f'https://api.github.com/repos/{org_name}/{repo_name}/contributors'
    headers = {'Authorization': f'token {access_token}'}
    contributors_data = requests.get(url, headers=headers).json()
    for contributor in contributors_data:
        add_or_update_user(contributor, repo_name)
    return contributors_data


def get_contributions(org_name, repo_name):
    url = f"https://api.github.com/repos/{org_name}/{repo_name}/stats/contributors"
    headers = {'Authorization': f'token {access_token}'}
    response = requests.get(url, headers=headers)
    print(response)
    if response.status_code == 200:
        return response.json()
    else:
        return dict()


def extract_event_details(event):
    event_type = event.get("type", "")
    payload = event.get("payload", {})
    actor_details = event.get("actor", {})
    repository_details = event.get("repo", {})
    org_details = event.get("org", {})

    created_at_str = event.get("created_at", "")

    created_at_date = datetime.strptime(created_at_str, "%Y-%m-%dT%H:%M:%SZ")
    day_id = created_at_date.strftime("%Y%m%d")

    details = {
        "event_type": event_type,
        "actor_login": actor_details.get("login", ""),
        "actor_id": actor_details.get("id", ""),
        "actor_avatar_url": actor_details.get("avatar_url", ""),
        "repository_id": repository_details.get("id", ""),
        "repository_name": repository_details.get("name", "").split("/")[1],
        "org_id": org_details.get("id", ""),
        "org_login": org_details.get("login", ""),
        "created_at": event.get("created_at", ""),
        "day_id": day_id,
    }

    # Handle ForkEvent
    if event_type == "ForkEvent":
        forkee_details = payload.get("forkee", {})
        details["forked_repository_name"] = forkee_details.get("full_name", "")
        details["forked_repository_description"] = forkee_details.get("description", "")

    # Handle WatchEvent
    elif event_type == "WatchEvent":
        action = payload.get("action", "")
        details["watch_action"] = action

    # Handle IssuesEvent
    elif event_type == "IssuesEvent":
        action = payload.get("action", "")
        issue_details = payload.get("issue", {})
        details["issue_title"] = issue_details.get("title", "")
        details["issue_state"] = issue_details.get("state", "")
        details["issue_body"] = issue_details.get("body", "")
        details["issue_comments"] = issue_details.get("comments", 0)

    # add_event(details)


def get_events_for_date(org_name, repo_name, page=1):
    url = f'https://api.github.com/repos/{org_name}/{repo_name}/events'
    params = {'per_page': 100, 'page': page}
    headers = {'Authorization': f'token {access_token}'}
    response = requests.get(url, params=params, headers=headers)

    if response.status_code != 200:
        print(f"Error: Unable to fetch events for {org_name}/{repo_name}.")
        return []

    events = response.json()

    # Check if events occurred before yesterday at the current time
    yesterday = datetime.utcnow() - timedelta(days=1)
    yesterday_start = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday_end = datetime.utcnow()

    filtered_events = [event for event in events if yesterday_start <= parse_date(event['created_at']) <= yesterday_end]

    # If there are more events, recursively fetch them
    if len(events) == 100:
        next_page = page + 1
        filtered_events += get_events_for_date(org_name, repo_name, page=next_page)

    # print(filtered_events)
    return filtered_events


def parse_date(date_string):
    return datetime.strptime(date_string, '%Y-%m-%dT%H:%M:%SZ')


def main():
    org_name = 'facebook'
    incremental_load_repos(org_name)

    #
    # # Iterate through repositories
    # for repo in repositories:
    #
    #     repo_name = repo['name']
    #     # Get contributors for each repository
    #     contributions_list = get_contributions(org_name, repo_name)
    #     sleep(30)
    #     contributors_list = get_contributors_for_repository(org_name, repo_name, access_token)
    #     sleep(30)
    #
    #     for contribution in contributions_list:
    #         print(contribution, "sdsdsds")
    #
    #     contributions_logins = {contribution["author"]["login"] for contribution in contributions_list}
    #
    #     # Find contributors in contributors_list whose login is present in contributions_list
    #     common_contributors = [contributor for contributor in contributors_list if
    #                            contributor["login"] in contributions_logins]
    #
    #     print(repo['name'])
    #
    #     # print("Contributors from contributors_list present in contributions_list:", common_contributors)
    #
    #     # # Output contributors for the repository
    #     # print(f"\nContributors for {org_name}/{repo_name}:")
    #     # for contributor in contributors:
    #     #     print(contributor)
    #     # print(contributor['login'])


if __name__ == "__main__":
    main()
