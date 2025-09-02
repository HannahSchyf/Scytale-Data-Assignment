## Here you can find a list of all possible filters you can use and arguments.   

### Date range 
There are two filters for date range, start date (will show everything after or on some specified date) and end date (will show everything before or on some specified date). These filters are used by,

```
--start_date YYYY-MM-DD
--end_date YYYY-MM-DD
```
### Passed code review
This filter will only show PRs which have passed code review,

```
---passed_CR
```

### Merged PRs
This filter will show only PRs which have been merged,

```
--PR_merged
```

### Author
This filter will show only PRs by a specified author,

```
--author AUTHOR_NAME
```
### Repo
This filter will show only PRs from a specified repo,

```
--repo REPO_NAME
```
### Checks passed
This filter will check if all required checks were passed for a PR,

```
--passed_checks
```
### Is Compliant
This filter checks if the PR that was merged is complaint but checking that the merged PR passed all required checks if there are any.
```
--is_compliant
```