# Week 02 - Data Engineering Project for Collecting Weather Data

🚀 Introduction:

Welcome back to my data engineering journey! This weekend, I delved into the realm of weather data collection and analysis, utilising the power of OpenWeatherAPI, Python, Airflow, AWS Glue, AWS Redshift and Tableau Cloud.

🔍 Project Overview:

In this project, my focus shifts towards weather data, a crucial aspect in numerous organisations from agriculture to transportation. Leveraging Python and Airflow, I've constructed a robust pipeline for automated weather data collection. This pipeline efficiently extracts weather data, stores it in an S3 repository and transforms it using AWS Glue. Finally, the transformed data finds its home in AWS Redshift a data warehouse, ready for in-depth analysis and visualization through Tableau.

🛠️ Technical Details:

Python and Airflow form the backbone of this project, orchestrating the data pipeline effortlessly. By integrating AWS Glue into the workflow, I ensure efficient data transformation, enabling integration with AWS Redshift. With AWS Redshift as the destination, data is stored in a scalable and performant manner, ready to support analytical queries. Additionally, establishing a Tableau connection enhances the accessibility and visual appeal of the analysed data.

📈 Next Steps:

While this project marks a milestone in my data engineering journey, there's always room for improvement. With more time at hand, I would aim to further enhance automation across the entire data pipeline, extending beyond just the data extraction phase handled by Airflow. By automating additional aspects such as data quality checks, error handling and dynamic scaling of resources based on workload demands, I'd strive to create a truly self-sustaining and efficient data ecosystem.

🙏 Thank You

Happy coding! 🌟

## Project Architecture
![Untitled Diagram(2)](https://github.com/andreisacal/W02-DE-Weather-Report/assets/166915179/8ae2daf3-ffb4-4b1c-891e-c2a3f716b2cf)

## How the Pipeline Works

1. The pipeline initiates with a scheduled trigger from Airflow, launching the ETL process defined in "twitter_etl.py".
2. Utilising "OpenWeatherAPI", the run_open_weather_etl() function sends a request to retrieve weather data for a specified city, using a scraper API key and designated search query.
3. The retrieved weather data is formatted into a JSON string and stored in an S3 bucket as a JSON file named "open_weather_2024_04_21_15_56_22.json".
4. With the help of an AWS Glue Job (glue_etl.py), the JSON files undergoes conversion into a structured format for further analysis and manipulation. This process includes flattening JSON data, renaming columns and converting data types.
5. The transformed data then is loaded into AWS Redshift, a data warehouse enabling seamless integration with other analytical tools and services.
6. Data stored in Redshift becomes readily accessible for in-depth analysis and visualisation using tools like Tableau Cloud.

![1](https://github.com/andreisacal/W02-DE-Weather-Report/assets/166915179/7f3509cd-2b67-4311-bd03-55f27f5343e2)
![3](https://github.com/andreisacal/W02-DE-Weather-Report/assets/166915179/f784aa4a-4eaa-48bb-8c64-9583dce3642b)
![2](https://github.com/andreisacal/W02-DE-Weather-Report/assets/166915179/30a70daf-8e18-40b3-a170-da73493e5f3f)
![4](https://github.com/andreisacal/W02-DE-Weather-Report/assets/166915179/48b01a6a-c139-4724-afcb-e6b6e5f61069)
![5](https://github.com/andreisacal/W02-DE-Weather-Report/assets/166915179/bf181848-048c-4203-aed4-bacd2c1f403c)
![6](https://github.com/andreisacal/W02-DE-Weather-Report/assets/166915179/889449db-ebbe-4b82-8f86-bacb57acaab2)
![7](https://github.com/andreisacal/W02-DE-Weather-Report/assets/166915179/9a61ba23-3a71-4d8d-83ea-61a94671d9b8)

## References

- #### APIs: [OpenWeatherAPI]([https://www.scraperapi.com/](https://openweathermap.org/))
- #### Data Analysis: [Tableau](https://www.tableau.com/)
- #### Cloud Computing: [AWS](https://aws.amazon.com/)
  - #### [Redshift](https://aws.amazon.com/glue/)
  - #### [S3](https://aws.amazon.com/s3/)
  - #### [ATHENA](https://aws.amazon.com/athena/)
  - #### [MWAA](https://aws.amazon.com/managed-workflows-for-apache-airflow/)
- #### Blogs: [Flattening JSON objects in Python](https://towardsdatascience.com/flattening-json-objects-in-python-f5343c794b10)
