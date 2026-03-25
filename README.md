# FinGuard ETL Pipeline 🏦

![Pipeline Status](https://github.com/azuzasiyothula/finguard-etl/actions/workflows/pipeline.yml/badge.svg)
![Python](https://img.shields.io/badge/Python-3.14-blue?logo=python)
![Pandera](https://img.shields.io/badge/Validated%20with-Pandera-brightgreen)
![Tests](https://img.shields.io/badge/Tests-8%20passing-success)

---

## What is this? 

This is a data pipeline I built to start learning data engineering properly.

It reads raw financial transaction data, cleans it up, checks it for issues,
and saves it to a database. The part I spent the most time on is the validation
step, the idea being that bad data should get caught before it reaches the
database, not after.

I got the idea from my QA background, to never push code to production without
running tests first, so I thought, why not do the same thing for data.

---

## So, how does it work? 🚀

```
Raw CSV  -->  Extract  -->  Transform  -->  Validate  -->  Load  -->  SQLite DB
                                               |
                                       if checks fail:
                                       pipeline stops here
                                       nothing gets loaded
```

**Extract** - reads the CSV into a pandas dataframe

**Transform** - cleans the data: removes duplicates, drops empty rows,
standardises column names, adds a high-value transaction flag for anything over R1000

**Validate** - runs 5 quality checks on the cleaned data using Pandera.
If anything fails the whole pipeline stops. I wanted to make sure dirty
data never makes it to the database.

**Load** - saves the clean data to SQLite and prints a quick summary

---

## Tech I used ⚒️

- Python 3.14
- pandas - for cleaning and transforming the data
- Pandera - for the data validation checks
- SQLite - for storing the final clean data
- GitHub Actions - automatically runs the tests every time I push
- pytest - for the unit tests

---

## A problem I ran into ⚠️

I originally planned to use Great Expectations for the validation step because
it came up in a lot of data engineering tutorials. When I installed it, it kept
throwing a Pydantic v1 compatibility error with Python 3.14 and I couldn't
get it to work.

After some googling I found Pandera which does the same thing and works fine
with Python 3.14. The syntax is also a lot cleaner so I'm glad I switched.

---

## How to run it yourself 👩🏾‍💻

**1. Clone the repo**
```bash
git clone https://github.com/azuzasiyothula/finguard-etl.git
cd finguard-etl
```

**2. Create and activate a virtual environment**
```bash
python3 -m venv venv
source venv/bin/activate
```

**3. Install dependencies**
```bash
pip install -r requirements.txt
```

**4. Get the dataset**

Download `fraudTrain.csv` from Kaggle:
https://www.kaggle.com/datasets/kartik2112/fraud-detection

Drop it in the `data/` folder.

**5. Run the pipeline**
```bash
python3 pipeline.py
```

**6. Run the tests**
```bash
pytest tests/ -v
```

---

## What the output looks like 

```
==================================================
  FinGuard ETL Pipeline
==================================================

Step 1: Extracting data...
Reading data from data/fraudTest.csv...
Done! Loaded 555719 rows and 23 columns

Step 2: Transforming data...
Starting data cleanup...
Removed 0 duplicate rows
Removed 0 rows with missing values
Cleanup done. 555719 rows ready to validate

Step 3: Validating data quality...

Running data quality checks...
----------------------------------------
merchant column: OK
category column: OK
amount range: OK
fraud flag values: OK
high value flag values: OK
----------------------------------------
All checks passed - safe to load the data


Step 4: Loading clean data to database...
Saving 555719 rows to database...
Done! Data saved to data/finguard.db

Quick summary of what got loaded:
----------------------------------------
Total rows: 555719
Fraudulent transactions: 2145
High value transactions: 1583
Average amount: 69.39
----------------------------------------

==================================================
  Pipeline finished successfully!
==================================================
```

---

## Tests

```
tests/test_pipeline.py::test_duplicates_removed                   PASSED
tests/test_pipeline.py::test_high_value_flag_applied              PASSED
tests/test_pipeline.py::test_normal_flag_applied                  PASSED
tests/test_pipeline.py::test_no_negative_amounts                  PASSED
tests/test_pipeline.py::test_output_columns_are_correct           PASSED
tests/test_pipeline.py::test_clean_data_passes_validation         PASSED
tests/test_pipeline.py::test_negative_amount_fails_validation     PASSED
tests/test_pipeline.py::test_bad_fraud_flag_fails_validation      PASSED
```

---

## Folder structure

```
finguard-etl/
├── .github/workflows/
│   └── pipeline.yml       # runs tests automatically on every push
├── data/                  # put your CSV here (not tracked by git)
├── logs/                  # pipeline logs land here
├── tests/
│   └── test_pipeline.py
├── extract.py
├── transform.py
├── validate.py
├── load.py
├── pipeline.py
├── conftest.py
└── requirements.txt
```

---

## What I'll be doing differently as I progress

- Use PostgreSQL instead of SQLite for something more realistic
- Add proper scheduling with Apache Airflow instead of just GitHub Actions
- Load data incrementally instead of replacing the whole table each run
- Add row level logging so you can trace exactly which records failed
- Look into dbt for the transformation layer

---

## About

Built by **Azuza Siyothula**

BSc IT student at Richfield Graduate Institute of Technology.
Background in QA automation, learning data engineering.
Looking for graduate programmes or junior roles in FinTech and banking.

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-0077B5?logo=linkedin&logoColor=white)](https://linkedin.com/in/azuza-siyothula/)
[![Email](https://img.shields.io/badge/Email-Contact-red?logo=gmail&logoColor=white)](mailto:azuzasiyothula10@gmail.com)
