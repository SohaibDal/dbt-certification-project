# dbt Certification Project
The main goal of this project is to deliver the tools and practice required to pass the dbt Analytics Engineering Exam.

- [dbt Certification Project](#dbt-certification-project)
   * [Scenario](#scenario)
      + [Data Architecture](#data-architecture)
      + [Data Model](#data-model)
         - [Raw Layer](#raw-layer)
         - [Core Layer](#core-layer)
      + [Data Dictionary](#data-dictionary)
   * [1. Development Setup](#1-development-setup)
      + [1.1 Setup your Mac](#11-setup-your-mac)
      + [1.2 Git](#12-git)
   * [2. Clone the dbt Certification Project Repository](#2-clone-the-dbt-certification-project-repository)
      + [Generate a New SSH Key](#generate-a-new-ssh-key)
      + [Add the SSH key to your GitHub account](#add-the-ssh-key-to-your-github-account)
      + [Git Clone dbt Certification Project](#git-clone-dbt-certification-project)
   * [Docker Version (Optional)](#docker-version---optional)
   * [3. Setup Your Python Environment](#3-setup-your-python-environment)
   * [4. Activate Your Virtual Environment](#4-activate-your-virtual-environment)
   * [5. Install Python packages required](#5-install-python-packages-required)
   * [6. Setup dbt Project](#6-setup-dbt-project)
   * [7. Install dbt Power Extension for VSCode](#7-install-dbt-power-extension-for-vscode)
   * [8. How to use Git on this Project](#8-how-to-use-git-on-this-project)
      + [8.1.dbt Certification Project Git Workflow](#81dbt-certification-project-git-workflow)
      + [8.2.How to get new updates from production to your branch](#82-how-to-get-new-updates-from-production-to-your-branch)
   * [9. Project Challenges](#9-project-challenges)
   * [10. Additional Content](#10-additional-content)

## Scenario
- You have been hired by a new company that wants to publish the next big hit of boardgame and needs your help to identify the factors to create this new boardgame, like:
    - Countries with highest demand (based on number of reviews and positive reviews, what are the kind of games that users rank the best).
    - Define metrics to calculate the average rating for each dimension (designers, categories, artists, mechanics)

To do that the company relies on your expertise to perform insights on the datasets available and build recommendations from it.

### Data Architecture

![Data Architecture](./docs/images/dbt_certification_project_architecture.png)


**Components:**
- [Snowflake L&D Sandbox](https://jsa18243.snowflakecomputing.com)
   - Sign in using OKTA SSO
- [GitHub Project Repository](https://github.com/Hakkoda1/dbt-certification-project)
- **Data Sources:**
  - [Google Drive](https://drive.google.com/drive/folders/1bPY5YzS8G94SX6kMfo8ZqUAoxGSvfGUT?usp=sharing)
  - [GitHub Public Repo](https://github.com/beefsack/bgg-ranking-historicals)

### Data Model

#### Raw Layer
[Data Model](https://miro.com/app/board/uXjVN-3i7mo=/?moveToWidget=3458764574304978327&cot=14)

#### Core Layer
[Data Model - Snowflake Schema](https://miro.com/app/board/uXjVN-3i7mo=/?moveToWidget=3458764574588845913&cot=14)


### Data Dictionary

[Data Dictionary](https://docs.google.com/spreadsheets/d/1W3oXg2I52cy2oLPJQz7Ah4a8TQGju9yByI57JWWFbEc/edit?usp=drive_link)

---

## 1. Development Setup

The following steps will help you to configure your mac for Python development, automation, and command line use.

You can also check this [recorded session](https://hakkoda-io.zoom.us/rec/share/LwqsBWVikejAkrXlBmSzwJsotuL68ymm1mXTfI1QgLn3Dw1ZG7BjSsTisF3WejY.xopJ495myIIIxwSt?startTime=1704362599000) that goes through project setup step-by-step. (Passcode: 7%#DU9#b)

### 1.1 Setup your Mac

You should install some command line tools, and configure your terminal and editor:

- Setup [Hombebrew](https://hakkoda.atlassian.net/wiki/spaces/DQ/pages/23068946/01+Setup+Homebrew+Git)
- Setup your Mac [terminal and Zsh](https://hakkoda.atlassian.net/wiki/spaces/DQ/pages/23167067/03+Trick-out+Zsh)
- Setup your Mac OS with [Visual Studio Code](https://hakkoda.atlassian.net/wiki/spaces/DQ/pages/56098886/05+Setup+Visual+Studio+Code+VS+Code)

### 1.2 Git

- Confirm `git` is installed: ```git --version```
- Sign up on GitHub using your Hakkoda email
- Sign in to GitHub using your Hakkoda account
- Click on the top left button (alongside the GitHub logo) to expand the left panel and press the *Single sign-on* button like shown below
 ![GitHub Single Sign-On](/docs/images/github_single_sign_on_button.png)

- Press the Continue button
 ![GitHub Single Sign-On](/docs/images/github_single_sign_on_hakkoda.png)
 
- Finally you have the main page with top panel with the links to access repositories, projects, teams and so on.
 ![GitHub Single Sign-On](/docs/images/github_hakkoda_account_main.png)

 #### Configure global git settings
```bash
git config --global user.name "[github_username]"
git config --global user.email "[github_email]"
git config --global init.defaultBranch main
git config --global --add push.default current
git config --global push.autoSetupRemote true
git config --global pull.rebase false
```

**Note:** make sure to replace [github_username] and [github_email] in the path above with your own

To confirm your git settings run the following command:
```bash
git config --global --list
```

## 2. Clone the dbt Certification Project Repository

Like all Hakkoda repos, the dbt Certification Project repo can only be access locally using SSH keys. If you haven't already setup SSH follow these steps or skip straight to the `git clone` command below.

### Generate a New SSH Key

Open a Terminal. If you haven't already, [setup your terminal](https://hakkoda.atlassian.net/wiki/spaces/DQ/pages/23167067/03+Trick-out+Zsh). Then create an SSH key. Ensure that you save your key passphrase in Bitwarden.

``` shell
# Generate the SSH key
# NOTE: 
#  * Update your email address before running the keygen command

ssh-keygen -t ed25519 -C "your_email@hakkoda.io"
```

Check the example below on how to adjust the above command to your e-mail account.

When asked to enter file in which to save the key just hit **Enter** on your keyboard to accept to save on the suggested file. If it already exists, it will ask if you want to overwrite.

![SSH-Keys](/docs/images/ssh-keys.png)

When asked to *Enter passphrase* you can use Bitwarden to generate the passphrase and save as something like `LaptopSSHKey`. You will need to provide this passphrase every time you need to unlock your SSH key

```bash
# Add your SSH key to the ssh-agent
eval "$(ssh-agent -s)"

# Update your SSH config to automatically load keys into the ssh-agent and 
# store passphrases in your keychain.
cat <<- EOF >> ~/.ssh/config
Host github.com
  AddKeysToAgent yes
  UseKeychain yes
  IdentityFile ~/.ssh/id_ed25519
EOF

# For MacOS, you can also store your passphrase in the Apple keychain. You may 
# need to provide your mac password to access your keychain. 
ssh-add --apple-use-keychain ~/.ssh/id_ed25519
```

### Add the SSH key to your GitHub account

Follow [instructions](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/adding-a-new-ssh-key-to-your-github-account#adding-a-new-ssh-key-to-your-account) to add your ***public*** key to your GitHub account.

``` shell
# Copy public key to clipboard
pbcopy < ~/.ssh/id_ed25519.pub
```

Go to the [GitHub Keys page](https://github.com/settings/keys) and click the `New SSH key` button. Then:

- **Add a description title**: `LaptopSSHKey`
- **Paste the key**: cmd + v

If requested, enter your GitHub password to save.

Then to enable SSO for your key, click `Configure SSO` and then `Authorize`. Click `continue` to enable SSO. Go through Okta authentication steps. Click `continue` to complete.

When finalized it should look like this. ![GitHub SSH Key](./docs/images/github_ssh_key.png "GitHub SSH Key")

### Git Clone dbt Certification Project

Before cloning your repo, you should open your terminal to a development folder to store our project code in.

For example:

``` shell
# Ensure that you are in your "home" directory
cd ~

# Create a new development directory, if it doesn't already exist
mkdir Develop

# Change to the development directory
cd Develop

# Clone project repository (NOTE: uses SSH key)
git clone git@github.com:Hakkoda1/dbt-certification-project.git
```
---
## Docker Version - Optional

There is a docker version available on this repository that you can use instead of setting up a python environment on your local machine.

Check this [video tutorial](https://www.loom.com/share/9e939165c7dd4ae5a4c3df10a2dfecf5?sid=cd9230c1-de4b-4c0b-9b3e-8254cdfe3f73) on how to setup the docker-compose.

To install docker desktop on Mac you can use this [url](https://docs.docker.com/desktop/install/mac-install/).

**Note:** If you choose this path, you can jump directly to [chapter 6](#6-setup-dbt-project) of the tutorial.



---

## 3. Setup Your Python Environment

Setup Python for dbt use with Snowflake. Because dbt-core is currently compatible with Python versions from 3.8 - 3.11, we have setup the project to use Python 3.10, but feel free to install any version that is supported by dbt ([source](https://docs.getdbt.com/faqs/Core/install-python-compatibility)).

``` shell
brew install python@3.10 virtualenv
```

Create your Python virtual environment. (If you have used another Python version, please change the code below accordingly)

``` shell
# Change to dbt-certification-project repository root folder
cd dbt-certification-project/

# Setup a project python virtual environement
virtualenv .dbt-env --python=python3.10
```

## 4. Activate Your Virtual Environment

***Every time*** you open a new terminal, you will need to source your Project Python environment:

``` bash
# Source your project environment
source .dbt-env/bin/activate
```

## 5. Install Python packages required
Run the following command to install the required Python packages for the project:

``` bash
pip install -r requirements.txt
```

## 6. Setup dbt Project

When you clone the repository the dbt project was already initialized, so you can skip the command `dbt init`.

After that, you need to use the `sample-profiles.yml` template file to create your own `profiles.yml` by running the following command inside dbt folder:

``` bash
# Change to dbt project folder
 cd dbt/

# Rename the file sample-profiles.yml to profiles.yml
cp sample-profiles.yml profiles.yml
```

## 7. Install dbt Power Extension for VSCode

dbt Power User extension is a great tool to bring a similar experience to dbt Cloud version using dbt-core on VSCode.

Below are listed the features that this extension provides:

- Setting up the dbt Power User extension
- Column Level Lineage
- dbt query data results preview with file export and analysis
- Generate dbt models from source files or SQL queries
- Generate model and column descriptions
- Click to execute parent/child models and tests
- Query explanations for complex dbt models

Follow the video below on a step-by-step tutorial on how to install.

[Tutorial Installation](https://www.loom.com/share/9eb5eac5bb3b4e11a34941af4ae9e686)

Below there are a couple of additional videos with useful information on how to setup and take advantage of the features from dbt Power User extension.

**Note:** Those are from the dbt Zero to Hero Udemy course that is part of the learning path for the dbt Analytics Engineering Certification Exam.

- [How to get an API Key for the Advanced Features](https://hakkoda.udemy.com/course/complete-dbt-data-build-tool-bootcamp-zero-to-hero-learn-dbt/learn/lecture/40657702#overview)
- [Use AI to Generate Documenation](https://hakkoda.udemy.com/course/complete-dbt-data-build-tool-bootcamp-zero-to-hero-learn-dbt/learn/lecture/40657714#overview)
- [Generate dbt Model from Source Definition or SQL](https://hakkoda.udemy.com/course/complete-dbt-data-build-tool-bootcamp-zero-to-hero-learn-dbt/learn/lecture/40657718#overview)
- [Working with Column-Level Lineage](https://hakkoda.udemy.com/course/complete-dbt-data-build-tool-bootcamp-zero-to-hero-learn-dbt/learn/lecture/40657724#overview)
- [Find Problems in your dbt Project with Health Check](https://hakkoda.udemy.com/course/complete-dbt-data-build-tool-bootcamp-zero-to-hero-learn-dbt/learn/lecture/40657748#overview)
- [Use AI to Interpret Queries via Query Explanations](https://hakkoda.udemy.com/course/complete-dbt-data-build-tool-bootcamp-zero-to-hero-learn-dbt/learn/lecture/40657752#overview)


## 8. How to use Git on this Project

Git is an integral part of version control and CI/CD. It makes contributing to a project with multiple contributors extremely easy. Version control is essential and allows any project to move to any snapshot of time in the development history.

### 8.1.dbt Certification Project Git Workflow

dbt Certification Project uses the Simple Flow branching convention with a few types of branches:

- **Short running branches (features)**: temporary dedicated branches for work-in-progress features
   - start from `develop`, to develop changes that are then integrated (merged) back to `develop`
   - enables parallel development with minimal conflicts with other features
   - For example, to create a new feature branch we need to ensure we are basing it off of the most recent `develop` branch and then creating the new branch:

      ``` bash
      git checkout develop
      git pull
      # In this scenario we should use the [feature_first-initial-last-name] structure to identify each member developing branch (ex: feature_fbalseiro)
      git checkout -b feature_fbalseiro
      ```

- **Long running branches (protected)**: branches that require pull requests to merge in changes
   - `develop`: changes staged for the next production release, should be merged to main when a release version is ready. This is is part of our CI/CD process.
   - `main`: current version in production, each merge on this branch should be tag and deployed.

``` mermaid
graph BT;
    feature_1 --> develop;
    feature_2 --> develop;
    feature_x --> develop;
    develop --> main;
```


### 8.2. How to get new updates from production to your branch

As you're going through the project and working on your branch `ex: feature_fbalseiro` there can be new updates on `main` branch due to new features or challenges that have been added to the repository.

In that scenario, if you want to to get access to that on your local machine, you can perform the following steps:

1. Check first in which branch are you working with the command `git branch` and the branch should be identified with a `*` before the name, like this:
```bash 
❯ git branch                           
  add-pr-template
  add-snapshot
  develop
* feature_fbalseiro
  github-cronjob
  main
```
2. Check if you have all your files staged and pushed to the remote repository by running the command `git status`
  2.1. If you get an output like the following you should first run `git push` to push your development to the remote repository branch:
  ```bash 
   ❯ git status
   On branch develop
   Your branch is ahead of 'origin/develop' by 1 commit.
     (use "git push" to publish your local commits)
   
   Changes to be committed:
     (use "git restore --staged <file>..." to unstage)
           modified:   ../README.md
  ```
  2.2. If you get an output that your branch is up to date and nothing to commit, then you can move to the next step

3. Checkout of your branch to the main branch by running the command `git checkout main` to switch to the main branch

4. Finally run `git pull` to pull the latest version of main branch on remote repository to your local machine

5. If you want to go back again to your working branch just run `git checkout feature_fbalseiro` (adjust to your branch name)


## 9. Project Challenges

1. [Customize Profile Directory](./docs/challenges/01_customize_profile_directory/01_customize_profile_directory.md)
2. [Setup Environment Variables](./docs/challenges/02_env_profiles/02_env_profiles.md)
3. [Add Sources](./docs/challenges/03_add_sources/03_add_sources.md)
4. [Testing and Documenting Sources](./docs/challenges/04_test_document_sources/04_test_document_sources.md)
5. [Add Snapshot](./docs/challenges/05_add_snapshot/05_add_snapshot.md)
6. [Add Staging Models](./docs/challenges/06_staging_models/06_staging_models.md)
7.   
     - [Add Seeds](./docs/challenges/07_add_seeds/07_add_seeds.md)
     - [Add Seeds: Custom Schema Name](./docs/challenges/07_add_seeds/07_add_seeds_custom_schema.md) 
8. [Add Source Freshness](./docs/challenges/08_add_source_freshness/08_add_source_freshness.md)
9. [Add Singular Test](./docs/challenges/09_add_singular_test/09_add_singular_test.md)
10. [Add Generic Test](./docs/challenges/10_add_generic_test/10_add_generic_test.md)
11. [Configure Test Severity](./docs/challenges/11_config_test_severity/11_config_test_severity.md)
12. [Update Staging Models](./docs/challenges/12_update_staging_models/12_update_staging_models.md)
13. [Customize Data Test Name](./docs/challenges/13_customize_data_test_name/13_customize_data_test_name.md)
14. [Add Intermediate Models](./docs/challenges/14_add_intermediate_models/14_add_intermediate_models.md)
15. [Add Variables](./docs/challenges/15_add_variables/15_add_variables.md)
16. [Add Tags](./docs/challenges/16_add_tags/16_add_tags.md)
17. [Add Marts Core Models](./docs/challenges/17_add_marts_core_models/17_add_marts_core_models.md)
18. [Generate Documentation](./docs/challenges/18_generate_documentation/18_generate_documentation.md)
19. [Add Macros](./docs/challenges/19_add_macros/19_add_macros.md)
20. [dbt run-operation](./docs/challenges/20_run_operation/20_run_operation.md)
21. [Hooks](./docs/challenges/21_hooks/21_hooks.md)
22. [Understanding DAGs](./docs/challenges/22_understanding_dags/22_understanding_dags.md)

### 9.1. Additional Challenges - Not Covered in the Exam
23. [Add dbt Expectations](./docs/challenges/23_add_expectations/23_add_expectations.md)
24. [Add CI Workflow](./docs/challenges/24_add_ci_workflow/24_add_ci_workflow.md)
25. [Add Schedule Daily Run Workflow](./docs/challenges/25_add_daily_job/25_add_daily_job.md)
26. [Add Deploy to Production (Post-Merge) Workflow](./docs/challenges/26_add_deploy_to_prod/26_add_deploy_to_prod.md)



## 10. Additional Content
- [dbt Project Checklist](https://docs.google.com/spreadsheets/d/1-76_IaWnq9d6vPZjltZBHR-8kL5lnrqu/edit?usp=sharing&ouid=114687234302613403012&rtpof=true&sd=true)
- [dbt Style Guide](./docs/style_guide.md)
- GitHub Workflows for GitHub Actions:
  - [Continuous Integration](/.github/workflows/ci.yml): Runs on Pull Requests to main branch
  - [Daily dbt job](/.github/workflows/scheduled_daily_run.yml): Runs daily on a schedule using cron to build and test source freshness, models, snapshots and seeds
  - [Upload to Snowflake](./.github/workflows/upload_to_snowflake.yml): Runs daily on a schedule using cron to execute a [python script](./.github/scripts/upload_to_snowflake.py) that loads a `.csv` file to a stage on Snowflake
- [Pull Request Template](./.github/pull_request_template.md): Template that is applied each time a Pull Request is triggered
- Snowflake Scripts:
  - [Snowflake Setup](./docs/snowflake_setup.md)
  - [Stored Procedure](./docs/snowflake_load_data_stage_table.md) on Snowflake to load data from internal stage to table Rankings
  - [Task](./docs/snowflake_task_call_procedure.md) to automate Stored Procedure to load data from stage


