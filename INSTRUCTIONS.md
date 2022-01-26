# Data Engineer Applicant Exercise

## Overview

Welcome to Mettle's Data Engineer Applicant Exercise. To be considered for a Data Engineer position you must successfully complete the task below.

Please write code that you would be happy to see in a production environment. Feel free to make any changes to this repo as you see fit.

## Task

### Description

The `./input` directory contains 3 files: `complaints.jsonl` (JSON line format) which contains complaints, `category_names.json` which contains a map of category names and `service_names.json` which contains a map of service_names.

In the `./src` directory create an application to extract the data, process/hydrate it and outputs it as CSV to the `./output` folder:

### Requirements

1. The solution should be written in Scala, Java or Python.
2. Strip out all unnecessary escaped quotes `"\"TEXT\"" => "TEXT"`.
3. The `./output` must contain a CSV file which contains data for the following headers: `service_name, category_name, complaint_created_at, complaint_text`.
4. Provide clear instructions of how to build / run this project.

## Submitting Your Code

Once you have completed/reviewed your code and are happy with it please email us a zipped copy of this directory for our team to review.

