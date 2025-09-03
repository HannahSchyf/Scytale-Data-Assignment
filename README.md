# Scytale-Data-Assignment
### The Scytale data assignment gets the repository from an organisation, retrieves the repos and their pull requests, saves the necessary pull request information in JSON files, and converts them to a table with PySpark. A summary of the endpoints and authentication method used is given in the Scytale_assignment_summary PDF. 

## Getting started

#### Clone the directory

```
git clone https://github.com/hannahschyf-boop/Scytale-Data-Assignment.git
cd Scytale-Data-Assignment
```
#### In the Scytale-Data-Assignment directory you will find two directories, run and repo_data.

## Step one - Getting the organisation repos and PRs
This is done by running the extract.py script.

```
cd run
python extract.py --org "Your organisation name"
```
This will create JSON files for each repo in the organisation, where the PRs are saved with all necessary information. 
These files are found in the repo_data directory. 

## Step two - transforming the data 
Here the data from the JSON files are transformed to a Spark schema table. The data is then flattened to produce a data frame where each PR has its own row with the necessary information. 
This is done by running the transform.py script, this is also in the run directory.

```
python transform.py
```
## Additional filters
If you would like to filter the output data from the transform level, this can be done by using a flag filter.
```
python transform.py --some-flag flag_argument
```
A list of possible flags is given in the README.md file in the run directory. 




