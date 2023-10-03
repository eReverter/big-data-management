# Big Data Management Backbone

This project is divided into two main parts. Data Ingestion and Storage, done with @alex, and Data Transformation, Modeling, and Visualization, done with @pim.

## Part 1: Data Ingestion and Storage

In the first part of the project, we focus on data extraction and storage. Data is loaded into Apache Hadoop for initial storage and then subsequently moved to MongoDB for further processing and analytics. 

- **Detailed Information**: For a comprehensive understanding of this part, please refer to `Report 1`.
  
- **Code Location**: All the code related to data extraction and storage can be found in the folder named `extract`.

## Part 2: Data Transformation, Modeling, and Visualization

The second part of the project leverages Apache Spark for data transformation using Resilient Distributed Datasets (RDD). Here, we calculate various Key Performance Indicators (KPIs) and train a machine learning model. Streaming is also added on top of this for real-time predictions. Finally, the KPIs are uploaded to Tableau for visualization and better business understanding.

- **Detailed Information**: For more details, please refer to `Report 2`.

- **Code Location**: All the code for data transformation, machine learning, and visualization is in the folder named `loading_and_modeling`.

- **Tableau Results**: The results from Tableau can be found in the folder named `visualization`.