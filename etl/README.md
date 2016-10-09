# Vipunen ETL

..with Python and MS SQL

## Requirements
* Python 2
  * python-devel
  * python-pip
* pip
  * urllib3
  * requests
  * pymssql
* FreeTDS and unixODBC
  * freedts-devel
  * yum'ed
* ...

## Notes about fact SA loaders
* There is a generic fact loader script `sa-load.py` but it does not work on more sophisticated APIs that leave out fields based on null values or other reasons. See the script itself for more details.
