# NYCOpenData-Profiling-Analysis

[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)

Data Profiling, Quality and Analysis on public dataset on NYCOpenData.

[Dataset: NYC Open Data](https://opendata.cityofnewyork.us/)

### Task 1 : Generic Profiling

Open data often comes with little or no metadata. You will profile a large collection of open data sets and derive metadata that can be used for data discovery, querying, and identification of data quality problems.<br>

For each column in the dataset collection, you will extract the following metadata
1. Number of non-empty cells
2. Number of empty cells (i.e., cell with no data)
3. Number of distinct values
4. Top-5 most frequent value(s)
5. Data types (a column may contain values belonging to multiple types)

Identify the data types for each distinct column value as one of the following:<br>
* INTEGER (LONG)<br>
* REAL<br>
* DATE/TIME<br>
* TEXT<br>

For each column count the total number of values as well as the distinct values for each of the above data types.<br>
For columns that contain at least one value of type INTEGER / REAL report:<br>
* Maximum value<br>
* Minimum value<br>
* Mean<br>
* Standard Deviation<br>

For columns that contain at least one value of type DATE report:<br>
* Maximum value<br>
* Minimum value<br>

For columns that contain at least one value of type TEXT report:<br>
* Top-5 Shortest value(s) (the values with shortest length)<br>
* Top-5 Longest values(s) (the values with longest length)<br>
* Average value length

![Viz-1](https://github.com/gandalf1819/NYCOpenData-Profiling-Analysis/blob/master/Visualizations/task1-viz.png)

### Task 2 : Semantic Profiling

For each column, identify and summarize semantic types present in the column. These can be generic types (e.g., city, state) or collection-specific types (NYU school names, NYC agency). <br>
For each semantic type T identified, enumerate all the values encountered for T in all columns present in the collection.<br>
You will look for the following types and add one or more semantic type labels to the column metadata together with their frequency in the column:<br>

* Person name (Last name, First name, Middle name, Full name)
* Business name
* Phone Number
* Address
* Street name
* City
* Neighborhood
* LAT/LON coordinates
* Zip code
* Borough
* School name (Abbreviations and full names)
* Color
* Car make
* City agency (Abbreviations and full names)
* Areas of study (e.g., Architecture, Animal Science, Communications)
* Subjects in school (e.g., MATH A, MATH B, US HISTORY)
* School Levels (K-2, ELEMENTARY, ELEMENTARY SCHOOL, MIDDLE)
* College/University names
* Websites (e.g., ASESCHOLARS.ORG)
* Building Classification (e.g., R0-CONDOMINIUM, R2-WALK-UP)
* Vehicle Type (e.g., AMBULANCE, VAN, TAXI, BUS)
* Type of location (e.g., ABANDONED BUILDING, AIRPORT TERMINAL, BANK,
CHURCH, CLOTHING/BOUTIQUE)
* Parks/Playgrounds (e.g., CLOVE LAKES PARK, GREENE PLAYGROUND)


![Viz-2](https://github.com/gandalf1819/NYCOpenData-Profiling-Analysis/blob/master/Visualizations/task2-viz.png)


![Viz-2.1](https://github.com/gandalf1819/NYCOpenData-Profiling-Analysis/blob/master/Visualizations/task2-viz2.png)

### Task 3 : Data Analysis

* Identify the three most frequent 311 complaint types by borough. 
* Are the same complaint types frequent in all five boroughs of the City? 
* How might you explain the differences? 
* How does the distribution of complaints change over time for certain neighborhoods and how could this be explained?
### Data Visualizations
#### Types of complaints across the different boroughs
![Viz-3.1](https://github.com/gandalf1819/NYCOpenData-Profiling-Analysis/blob/master/Visualizations/Task-3-1.png)
#### Distribution of "closed-dates" across the different boroughs
![Viz-3.2](https://github.com/gandalf1819/NYCOpenData-Profiling-Analysis/blob/master/Visualizations/Task-3-2.png)
#### Heat Map Representing Status of Complaints Across The Different Boroughs
![Viz-3.3](https://github.com/gandalf1819/NYCOpenData-Profiling-Analysis/blob/master/Visualizations/Task-3-3.png)
#### Heat Map Representing Count Of Complaints Across The Different Boroughs
![Viz-3.4](https://github.com/gandalf1819/NYCOpenData-Profiling-Analysis/blob/master/Visualizations/Task-3-4.png)
#### Distribution of Complaint Types and their resolution dates
![Viz-3.5](https://github.com/gandalf1819/NYCOpenData-Profiling-Analysis/blob/master/Visualizations/Task-3-5.png)
#### Types of complaints across various different locations
![Viz-3.6](https://github.com/gandalf1819/NYCOpenData-Profiling-Analysis/blob/master/Visualizations/Task-3-6.png)
#### Heat Map representing the Types of complaints that are open in the Brooklyn region
![Viz-3.7](https://github.com/gandalf1819/NYCOpenData-Profiling-Analysis/blob/master/Visualizations/Task-3-7.png)


# Team 

* [Chinmay Wyawahare](https://github.com/gandalf1819)
* [Vineet Viswakumar](https://github.com/vineet247)
* [HemanthTeja Yanambakkam](https://github.com/HemanthTejaY)
