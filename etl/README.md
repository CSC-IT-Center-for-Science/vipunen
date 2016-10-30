# Vipunen ETL

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

## SA

### SA Fact Loaders

There is a generic fact loader script `sa-load.py` but it does not work on more sophisticated APIs that leave out fields based on null values or other reasons. See the script itself for more details.

One attempt towards figuring out columns on-the-fly has been made (see `dboperator.py` and function `columns`). This is not a complete generalization yet but could be sufficient enough for now.

Some fact loaders require data-specific handling still (see todo section below).

## TODO
* Database connection variable naming.
* APIKEY name generalization, only one API is called at a time.
* For APIs that do not deliver all columns in every answer.
  * Read all data through one time especially for this to be sure all columns are counted for?
  * Make scripts use database ALTER commands? E.g. add columns if they are missing. Drop columns is probably not possible or wise.
    * May result in a terrible mess over time, though. So if this path is chosen other means of maintenance will be required (which maintenance most likely is required anyway).
    * This thought could, however, be useful for loading data over time.
* How to handle facts which are not loaded all at once?
