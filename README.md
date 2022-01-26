# Complaints Report

## Overview
- This repository contains batch processing code for generating customer complaints report. Code is written in scala 2.12.10 and spark 3.0.1

### Author 
Author of the code is Tushar Kesarwani <tushar.kesarwani2@gmail.com>


## How to develop
Develop locally. Tests are good to create mock data and run tests against it. Developing against Hadoop clusters is
possible but generally not necessary and reduces the velocity.

## How to run from local
- Use intelliJ run application to run the program locally
- Use the below CLI Input in intelliJ run configuration as it is :

```bash
ComplaintReport
local[1]
file:///Users/tkesarwani/git_tree/personal/MyScalaPlayground/input/complaints.jsonl
file:///Users/tkesarwani/git_tree/personal/MyScalaPlayground/input/category_names.json
file:///Users/tkesarwani/git_tree/personal/MyScalaPlayground/input/service_names.json
file:///Users/tkesarwani/git_tree/personal/MyScalaPlayground/output
```

## How to test

```bash
mvn test
```

## How to build

```bash
mvn clean compile install
```


## How to run
Use the below unix script to execute code in cluster

```bash
sh deploy/run.sh ComplaintReport local[1] file:///Users/tkesarwani/git_tree/personal/MyScalaPlayground/input/complaints.jsonl file:///Users/tkesarwani/git_tree/personal/MyScalaPlayground/input/category_names.json file:///Users/tkesarwani/git_tree/personal/MyScalaPlayground/input/service_names.json file:///Users/tkesarwani/git_tree/personal/MyScalaPlayground/output
```