# Vipunen ETL for Staging Area (SA)

..with Python and MS SQL

## Requirements

### Software
In CentOS installation via `yum` with dependencies(!) is sufficient and Python pip packages from, well, `pip`.
* FreeTDS and unixODBC
  * yum package(s):
    * `freedts-devel`
* Python 2
  * yum package(s):
    * `python-devel`
    * `python-pip`
  * pip packages:
    * `urllib3`
    * `requests`
    * `pymssql`

### Settings
* Database connection
  * Environment variables
    * `PYMSSQL_TEST_SERVER`
    * `PYMSSQL_TEST_DATABASE`
    * `PYMSSQL_TEST_USERNAME`
    * `PYMSSQL_TEST_PASSWORD`
* APIKEYS or authentication
  * Environment variables
    * `[APINAME]_USERNAME`
    * `[APINAME]_PASSWORD`
    * where `[APINAME]` is "ARVO" or "AIPAL" (should be generalized!)

## Loader scripts

For the purpose of these staging area data loading scripts there has been made a generic `dboperator.py` script. What is special about this script is that it requires those environment variables mentioned above, it opens database connection at init, and there is a function called `columns` that must be always called before any insert statements as it sets up column information to the module. When `dboperator` is used via codesets and classifications the column types are not handled. These tables (for dimensions if you will) are assumed to be in database already. It is just the opposite for fact tables which are (re-)created at each data load call (except for few facts which still have their own data loading scripts, see below).

### Codesets and classifications

Each classification script (filename contains word "luokitus") handles columns and data directly on their own since they all are specific in their content. But a more generic script `codes.py` loads codesets which all have the same structure and are loaded in the same database table.

### Fact data

There is a generic fact loader script `load.py` but it does not, or might not (changes have been made), work on more sophisticated APIs that leave out fields based on null values or other reasons. See the script itself for more details.

One attempt towards figuring out columns on-the-fly has been made (see `dboperator.py` and its function `columns`). This is not a complete generalization yet but could be sufficient enough for now.

Some fact loaders require data specific handling still (see todo section below).

## TODO
* Database connection variable naming.
* APIKEY name generalization, only one API is called at a time.
* For APIs that do not deliver all columns in every answer.
  * Read all data through one time especially for this to be sure all columns are counted for?
  * Make scripts use database ALTER commands? E.g. add columns if they are missing. Drop columns is probably not possible or wise.
    * May result in a terrible mess over time, though. So if this path is chosen other means of maintenance will be required (which maintenance most likely is required anyway).
    * This thought could, however, be useful for loading data over time.
* How to handle facts which are not loaded all at once?
