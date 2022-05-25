# Audiencer

A python package to collect Facebook audience estimates. 

This package is similar to PySocialWatcher (old version: https://github.com/maraujo/pySocialWatcher/commits?author=maraujo by Matheus Araujo and contributors, maintained version: https://github.com/joaopalotti/pySocialWatcher by Joao Palotti). But it has a few differences:

* designed to make as few requests as possible, by:
  * predicting the result of a query and skipping the query if it is very probably below 1000
  * allowing