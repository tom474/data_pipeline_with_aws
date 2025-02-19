# Data Pipeline with AWS  

This project builds a **data pipeline** using **AWS services** for real-time and batch data processing, integrating **machine learning** for predictive analytics.

## Tech Stack  

- Python  
- AWS S3  
- AWS EC2  
- AWS Kinesis  
- AWS Lambda  
- AWS SageMaker  
- MongoDB  

## System Architecture  

![System Architecture Diagram](https://github.com/tom474/data_pipeline_with_aws/blob/main/assets/system-architecture-diagram.png?raw=true)  

## Features  

### Streaming Data ETL Pipeline  

- Real-time weather data ingestion from **OpenWeatherMap API**.  
- Data processing using **AWS EC2, Kinesis, and Lambda**.  
- Data storage in **MongoDB** for real-time analytics.  

### Historical Data & Testing Data ETL Pipeline  

- Integration of **U.S. Civil Flights & Weather Meteo datasets**.  
- Data cleaning and transformation using **AWS SageMaker**.  
- Processed data stored in **AWS S3** for analysis.  


### Prediction Model Data Pipeline  

- Training dataset creation using **historical flight & weather data**.  
- Model training in **AWS SageMaker** using **Gradient Boosting, Decision Tree, and Random Forest**.  
- Predictions generated for real-time flight delay forecasting.  

### Visualization Data Pipeline  

- Processed data stored in **MongoDB** for visualization.  
- Dashboards built using **MongoDB Charts** for insights:
  - [LAX Overview Dashboard](https://charts.mongodb.com/charts-big-data-for-engineering-lzggfsp/public/dashboards/2b54cb6f-21e0-4cba-b024-a898e3606f0f?fbclid=IwZXh0bgNhZW0CMTAAAR3SBTXZwQnorRUTwApbq4Z5FwMAzy2O3D82_Zm7hFOw6NYDMW2UijQFqeI_aem_TsqlkYcRpEPGmuLCP3pdYw)
  - [LAX Weather Impact Dashboard](https://charts.mongodb.com/charts-big-data-for-engineering-lzggfsp/public/dashboards/2240afae-d905-4c0e-ad54-959e0f0a83ab?fbclid=IwZXh0bgNhZW0CMTAAAR3H2n8Hnx7prUGv0Ty__CRx8lBfiO88N0Sa-wPp-ThXvG1m6vdTFWIknnU_aem_ZSH8xQzG7SYU3poAiOjNiw)
  - [LAX Performance Benchmark Dashboard](https://charts.mongodb.com/charts-big-data-for-engineering-lzggfsp/public/dashboards/40528c4b-1ef1-4a0b-ae93-d3e20911d42b?fbclid=IwZXh0bgNhZW0CMTAAAR3H2n8Hnx7prUGv0Ty__CRx8lBfiO88N0Sa-wPp-ThXvG1m6vdTFWIknnU_aem_ZSH8xQzG7SYU3poAiOjNiw)
  - [LAX Time Series Dashboard](https://charts.mongodb.com/charts-big-data-for-engineering-lzggfsp/public/dashboards/5c7d6348-0dd8-4b77-a39b-18e75e3c3b44?fbclid=IwZXh0bgNhZW0CMTAAAR0kTTF7D3HchOVbwhk-3JTARp5I6U-lBYCHiZ-hWSpHinfgTprVWfLzAy8_aem_oH6mf1EOlemXOgAUZePIPg)
  - [LAX Geographical Insights Dashboard](https://charts.mongodb.com/charts-big-data-for-engineering-lzggfsp/public/dashboards/b8929560-fa61-454e-95da-6ecc6190b764?fbclid=IwZXh0bgNhZW0CMTAAAR3lp61_82Zx_S2QHsDcdZRLtbtkJ9FRlw94QLAJRo69vY73N3NN-adq_BA_aem_qhm9l3CXJibgx7RiAskkxg)
  - [LAX Aircraft Analysis Dashboard](https://charts.mongodb.com/charts-big-data-for-engineering-lzggfsp/public/dashboards/9f1b8081-1940-4b85-b5f1-84120b0c31c1?fbclid=IwZXh0bgNhZW0CMTAAAR3lp61_82Zx_S2QHsDcdZRLtbtkJ9FRlw94QLAJRo69vY73N3NN-adq_BA_aem_qhm9l3CXJibgx7RiAskkxg) 
