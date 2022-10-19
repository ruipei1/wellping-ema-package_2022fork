<img src=".img/SSNL.jpg" width=500px>

<br> 

# WellPing-EMA-Parser
Converts Stanford Communities Project EMA data from JSON to CSV

This package conveniently wraps an `EMA_Parser` object for all your social network analysis needs

## User Notes

### Before running the script:

Your target directory should have one JSON file with all participants' EMA data

<img src=".img/tree-before.png">

<br> <br>

### After running the script:

Your target directory will contain:

* Subject-wise CSV files of pings and answers (not shown in the screenshot below)
* Composite CSV of all subjects
* A JSON file containing participant data with existenatial errors (to be parsed separately)
* An error log of parsing issues that did **not** prevent subjects from inclusion in the CSV

<br>

<img src=".img/tree-after.png">