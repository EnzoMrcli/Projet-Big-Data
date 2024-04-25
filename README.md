# Projet Big Data

[![forthebadge](http://forthebadge.com/images/badges/built-with-love.svg)](http://forthebadge.com)  [![forthebadge](http://forthebadge.com/images/badges/powered-by-electricity.svg)](http://forthebadge.com)

Taking a cue from the Cloud Healthcare Unit's (CHU) endeavors to digitally transform its operations, this project aims to deliver a decision-making architecture and includes ELT/ETL jobs designed to streamline data extraction, loading, and transformation processes.

# Summary

 - [Usage guide](#Usage-Guide)
 - [Authors](#Authors)

# Usage Guide

## Importing the workspace into Talend Studio

To import this workspace into Talend Studio, follow these steps:
1. Unzip Jobs-Big-Data.zip
2. Open Talend Studio.
3. Click on import an existing project and Select
4. Enter a name and select the root directory of the unzipped folder workspace
5. Clickon Finish and it's done !

## Automated Script Execution for Apache Hive

This automation script is crafted to streamline the creation and updating of Hive tables by reacting to the arrival of new data files in HDFS. To successfully execute the script, follow the steps below:

### Prerequisites
- Ensure a properly configured Hadoop and Hive instance.
- Verify that the paths to the scripts and data in HDFS are correctly set in `initialisation.sh`.

### Configuring Access Rights
Before launching the script, it's crucial to grant appropriate execution rights. Execute the following command to provide necessary permissions:

chmod +x scripts/*.sh

Replace scripts/ with the actual path where the scripts are located.

### Launching the Script
To initiate the automation process, run the main script main.sh using the following command:
./main.sh
Ensure you run the script from its containing directory or make sure the path to the script is correct.

The main.sh script will periodically check for new data files and trigger the creation or update of Hive tables accordingly.

### Logs
The script logs all significant operations in a log file to facilitate tracking and diagnostics. Regularly check this file to stay informed about the script's execution status.

# Authors 

* **FODIL Nel** _alias_ [@nel34](https://github.com/nel34)
* **GOUADFEL Rayan** _alias_ [@AirG213](https://github.com/AirG213)
* **MARCELLI Enzo** _alias_ [@EnzoMrcli](https://github.com/EnzoMrcli)
