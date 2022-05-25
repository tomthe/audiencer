# Audiencer

A python package to collect Facebook audience estimates. 

This package is similar to PySocialWatcher (old version: https://github.com/maraujo/pySocialWatcher/commits?author=maraujo by Matheus Araujo and contributors, maintained version: https://github.com/joaopalotti/pySocialWatcher by Joao Palotti). But it has a few differences:


* Audiencer saves every request immediately to a SQLite database (export as a compatible csv is planned)
* Audiencer has some options to make as few requests as possible, by:
  * predicting the result of a query and skipping the query if it is probably below 1000
  * allowing different ways to combine the different targeting criteria
* Audiencer can automatically build combinations of queries from which it can calculate audience estimates for queries that result in fewer than 1000 monthly active users. (This feature is not completely ready yet)
* Audiencer is still an alpha-version and not enough tested
* While Audiencer expects the same input-format as pySocialWatcher, it will always collect "all" of a targeting-category, before doing the more specific collections
* not possible to use directly from the command-line yet


Similarities between Audiencer and pySocialWatcher:

* same input-files
