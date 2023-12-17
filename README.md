This project aims to analyze airline delay data using various Big Data technologies such as PySpark, Hive, and Sqoop. The project is divided into several components, each contributing to the overall analysis. Below is a summary of each component and its purpose:

Files Overview
1. airline_ml.ipynb
This Jupyter Notebook contains the machine learning aspect of the project. The key steps include:

FL_DATE: Flight date (object)
OP_CARRIER: Operating carrier (object)
OP_CARRIER_FL_NUM: Operating carrier flight number (int)
ORIGIN: Origin airport (object)
DEST: Destination airport (object)
DEP_TIME: Departure time (object)
DEP_DELAY: Departure delay (int)
CRS_ARR_TIME: Scheduled arrival time (object)
ARR_TIME: Actual arrival time (object)
ARR_DELAY: Arrival delay (int)
CANCELLED: Flight cancellation status (float)
DIVERTED: Flight diversion status (float)
CRS_ELAPSED_TIME: Scheduled elapsed time (int)
ACTUAL_ELAPSED_TIME: Actual elapsed time (int)
AIR_TIME: Air time (int)
DISTANCE: Flight distance (float)
CRS_DEP_TIME: Scheduled departure time (object)
Data Preprocessing
Timestamp Split:

The FL_DATE and CRS_DEP_TIME columns are split into year, month, day, hour, minute, and second for further analysis.
Feature Selection:

Unnecessary columns such as FL_DATE, CRS_DEP_TIME, DEP_TIME, CRS_ARR_TIME, ARR_TIME, ARR_DELAY, CANCELLED, DIVERTED, ACTUAL_ELAPSED_TIME, AIR_TIME are dropped.
Label Assignment:

The column DEP_STATUS is created based on the departure delay. Flights are labeled as 'OnTime' if the delay is less than 15 minutes and 'Delay' otherwise.
Origin and Carrier Information:

The model is initially trained for flights originating from ALT with OP_CARRIER as WN. Relevant columns are dropped.
Dependent and Independent Features:

Dependent features (DEP_DELAY and DEP_STATUS) are assigned to y, and independent features are assigned to X.
Label Encoding:

Label encoding is applied to categorical columns using LabelEncoder and OneHotEncoder for further analysis.
Machine Learning Model
Decision Tree and Random Forest Classification
Initial Model:

Decision Tree and Random Forest classifiers are trained on the dataset. Initial accuracy scores are reported.
Round 2 Model:

Introduction
This project focuses on analyzing airline delay data using machine learning techniques. The dataset contains various features related to flight details, departure, and arrival times.

Dataset Overview
The dataset includes the following columns:

FL_DATE: Flight date (object)
OP_CARRIER: Operating carrier (object)
OP_CARRIER_FL_NUM: Operating carrier flight number (int)
ORIGIN: Origin airport (object)
DEST: Destination airport (object)
DEP_TIME: Departure time (object)
DEP_DELAY: Departure delay (int)
CRS_ARR_TIME: Scheduled arrival time (object)
ARR_TIME: Actual arrival time (object)
ARR_DELAY: Arrival delay (int)
CANCELLED: Flight cancellation status (float)
DIVERTED: Flight diversion status (float)
CRS_ELAPSED_TIME: Scheduled elapsed time (int)
ACTUAL_ELAPSED_TIME: Actual elapsed time (int)
AIR_TIME: Air time (int)
DISTANCE: Flight distance (float)
CRS_DEP_TIME: Scheduled departure time (object)
Data Preprocessing
Timestamp Split:

The FL_DATE and CRS_DEP_TIME columns are split into year, month, day, hour, minute, and second for further analysis.
Feature Selection:

Unnecessary columns such as FL_DATE, CRS_DEP_TIME, DEP_TIME, CRS_ARR_TIME, ARR_TIME, ARR_DELAY, CANCELLED, DIVERTED, ACTUAL_ELAPSED_TIME, AIR_TIME are dropped.
Label Assignment:

The column DEP_STATUS is created based on the departure delay. Flights are labeled as 'OnTime' if the delay is less than 15 minutes and 'Delay' otherwise.
Origin and Carrier Information:

The model is initially trained for flights originating from ALT with OP_CARRIER as WN. Relevant columns are dropped.
Dependent and Independent Features:

Dependent features (DEP_DELAY and DEP_STATUS) are assigned to y, and independent features are assigned to X.
Label Encoding:

Label encoding is applied to categorical columns using LabelEncoder and OneHotEncoder for further analysis.
Machine Learning Model
Decision Tree and Random Forest Classification
Initial Model:

Decision Tree and Random Forest classifiers are trained on the dataset. Initial accuracy scores are reported.
Round 2 Model:

A refined dataset (X2) is created with fewer features, and Decision Tree and Random Forest classifiers are trained again. Improved accuracy scores are achieved.
Hyperparameter Tuning:

Grid search is employed for hyperparameter tuning, and the best parameters are identified.
Final Model:

The final Random Forest model is trained with tuned hyperparameters.
Model Evaluation
Confusion matrices and classification reports are generated to evaluate the performance of the models.
Model Persistence
The final Random Forest model is saved using the pickle module for future use.
Instructions for Use

Data Loading and Exploration: Loading the dataset and exploring the structure and features.
Data Transformation: Handling missing values, splitting timestamps, and feature engineering.
Feature Selection: Selecting relevant features and assigning labels based on departure delays.
Model Training: Using Decision Tree and Random Forest classifiers to predict departure delays.
Model Evaluation: Assessing the model's performance using metrics like accuracy, precision, recall, and F1-score.
2. hive.hql
This Hive script is used to create an external table named airline in Amazon RDS for exporting incremental data to S3. It defines the table schema and structure for storing airline-related data.

3. sqoop.txt
This text file contains Sqoop commands for importing and exporting data between Amazon RDS and S3. Key operations include exporting full and incremental data to RDS and importing data from RDS to Hadoop HDFS and Hive.

4. pythonscript.py
This Python script utilizes PySpark for data cleaning, transformation, and processing. It covers tasks such as dropping unwanted columns, handling null values, cleaning data formats, and converting data types. Additionally, it demonstrates the process of storing data in Parquet format in S3 and handling incremental data.

Usage Instructions
Jupyter Notebook (airline_ml.ipynb):

Open the Jupyter Notebook using a Python environment with necessary libraries (e.g., PySpark, scikit-learn).
Execute the cells sequentially to load, preprocess, and train machine learning models.
Hive Script (hive.hql):

Execute the Hive script in a Hive environment to create the external table airline in Amazon RDS.
Sqoop Commands (sqoop.txt):

Run the Sqoop commands in a terminal to perform data import/export operations between RDS and Hadoop.
Python Script (pythonscript.py):

Ensure you have a PySpark environment set up.
Replace placeholder values and execute the script to clean, transform, and store data in Parquet format.
Note
Adjust file paths and configurations based on your environment.
Ensure proper access and permissions for data sources and destinations.
Monitor and adapt the scripts for any changes in the dataset or technology stack.
This README provides a high-level overview of the project and instructions for running each component. Tailor the commands and configurations according to your specific setup and requirements.





