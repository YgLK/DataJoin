<h1 align="center">  DataJoin

<h3 align="center">  Left, Right or Inner join the CSV files


[![python](https://img.shields.io/badge/language-python-%23306998)](https://www.python.org/)

## Table of contents  

> ### 0. [Short description](#description)
> ### 1. [Project structure](#project-structure)
> ### 2. [Initial setup](#initial-setup)
> ### 3. [Run the script](#run-the-script)
> ### 4. [Tests]("#tests"")
> ### 5. [Technologies used](#technologies-used) 
> ### 6. [Assets used](#assets-used) 



## Short description  <a name="description"></a>

**DataJoin** is an executable program, which reads two csv files, joins them on the specified column and then prints 
the result to the standard output.  It's possible to specify the join type (inner, left or right). 
Script can be performed on the **input** which **can be bigger than the available machine memory**, 
because data is read with the use of the python generators which do not store their contents in memory.

The program is executed with the following command: 
```shell
join file_path file_path column_name join_type
```
 
* `file_path` - path to the CSV file (two, one for each file)
* `column_name` - column name to join on. These must be found in both CSV files.
* `join_type - ` - selected join type, possible values: `[inner, left, right]` <br>
Default `join_type` = `inner`, because it's  the simplest and most common form of 
the join is the SQL, inner join is the default of the SQL join types used in most database management systems (`JOIN` is equal to `INNER JOIN` in PostgreSQL for instance)

WARNING! Before applying join on your CSV files make sure that your dataset doesn't 
contain any missing values. <br>
You can fill missing values with this simple Python code
```python
import pandas as pd

filepath = "PATH/TO/THE/CSV/FILE"
file = pd.read_csv(filepath)
# fill the missing values with "NaN"
file = file.fillna("NaN")
# save the result CSV file
file.to_csv("PATH/TO/SAVE/filename.csv")
```
You can also check Jupyter Notebook where 


## Project structure  <a name="project-structure"></a>

    .
    ├── data                    # Directory with the test data
    │    ├── data11.csv         # CSV dataset used for testing (Kaggle)
    │    └── data12.csv         # CSV dataset used for testing (Kaggle)
    ├── join.py                 # Project source code file
    ├── tests.py                # Project tests
    ├── sonar-cube              # SonarQube raport
    ├── .gitignore              
    ├── LICENSE
    └── README.md


## Initial setup  <a name="initial-setup"></a>

### 1. Install Python3 

```sh
# for unix based systems
$ sudo add-apt-repository ppa:deadsnakes/ppa

$ sudo apt-get update

$ sudo apt install python3.9

$ python3 -V
# last command should show "Python 3.9.X"
```

You can create special virtual environment for the project.
```sh
    python3 -m venv .env/
    source .env/bin/python activate
```
For other operating systems see the official [download website](https://www.python.org/downloads/release/python-3810/) for Python.

### 2. Clone the repository
```sh
$ git clone git@github.com:YgLK/DataJoin.git
```

### 3. Run DataJoin from the terminal 
Allow executing the `join.py` file by changing its permissions
```shell
$ chmod +x join.py
```

If you use WSL on Windows, remember to convert Windows/DOS-style line endings (CR+LF) to Unix-style line endings (LF) in your DataJoin script.
```shell
# PROBLEM:
$ ./join.py
/usr/bin/env: ‘python3\r’: No such file or directory
# SOLUTION:
# install dos2unix
$ sudo apt install dos2unix
# run convert on your script 
$ dos2unix /PATH/TO/YOUR/WINDOWS_FILE
# for instance
$ dos2unix join.py
# then run script from the WSL terminal without problems
$ ./join.py 
```

To work with `join file_path file_path column_name join_type` you need to add an alias to your system.
```shell
# Open bashrc in order to add new alias
$ vim ~/.bashrc
# Add custom alias to the file which will let us run script with the `join` command, for instance:
    #My custom aliases
    alias join=”python3 /PATH/TO/YOUR/SCRIPT/join.py”
# Save the file and open new terminal session
```
After completing previous instructions now you can finally use the join script.


### Finally, run the DataJoin with the following command  <a name="run-the-script"></a>
```sh
    join file_path file_path column_name join_type
```


## Tests  <a name="description"></a>
Tests are implemented with the use of **Pandas** library which allows easy CSV files manipulation.

In order to run the tests move to the project directory and run:
```shell
python3 tests.py
```

## Technologies used <a name="technologies-used"></a>

| Resource link                                        |                    Description                    |                                                                                                                                License |
| :--------------------------------------------------- | :-----------------------------------------------: | -------------------------------------------------------------------------------------------------------------------------------------: |
| [Python v3.9](https://www.python.org/)             | The main programming language used in the project | [ZERO-CLAUSE BSD LICENSE](https://docs.python.org/3/license.html#zero-clause-bsd-license-for-code-in-the-python-release-documentation) |

## Assets used  <a name="assets-used"></a>

| Description | Source |
| :-----: | :-----: | 
| Sample datasets used to check joins correctness | [Kaggle dataset](https://www.kaggle.com/datasets/piterfm/2022-ukraine-russian-war?select=russia_losses_equipment.csv)|

The Kaggle datasets has their missing values filled with NaN (in the Jupyter Notebook with the use of pandas.DataFrame.fillna method) and then saved and used for testing as the `/data/data11.csv` and  `/data/data12.csv` files.



