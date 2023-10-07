# Big Data Management Backbone

This project is divided into two main parts. Data Ingestion and Storage, done with [@amartorell98](https://github.com/amartorell98), and Data Transformation, Modeling, and Visualization, done with [@PimSchoolkateUPC](https://github.com/PimSchoolkateUPC).

## Part 1: Data Ingestion and Storage

In the first part of the project, we focus on data extraction and storage. Data is loaded into **Apache Hadoop** for initial storage and then subsequently moved to **MongoDB** for further processing and analytics. 

- **Detailed Information**: For a comprehensive understanding of this part, please refer to [report-1](reports/report-1.pdf).
  
- **Code Location**: All the code related to data extraction and storage can be found in the folder named [ingestion_and_storage](src/ingestion_and_storage).

## Part 2: Data Transformation, Modeling, and Visualization

The second part of the project leverages **Apache Spark** for data transformation using **Resilient Distributed Datasets** (RDD). Here, we calculate various Key Performance Indicators (KPIs) and train a machine learning model. Streaming is also added on top of this for real-time predictions. Finally, the KPIs are uploaded to **Tableau** for visualization and better business understanding.

- **Detailed Information**: For more details, please refer to [report-2](reports/report-2.pdf).

- **Code Location**: All the code for data transformation, machine learning, and visualization is in the folder named [loading_and_modeling](src/loading_and_modeling).

- **Tableau Results**: The results from Tableau can be found in the folder named [visualization](src/visualization).
