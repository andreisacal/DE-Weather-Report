# Week 02 - Data Engineering Project for Collecting Weather Data

🚀 Introduction:

Welcome to my journey in the world of data engineering! As I embark on this exciting endeavour, I am thrilled to share with you my passion for mastering the skill of transforming raw data into valuable insights. With each passing weekend, I am committed to challenging myself to complete a new data engineering project in order to push the boundaries of my skills and knowledge.

Through this series of projects, I aim to not only deepen my understanding of fundamental data engineering concepts but also to improve my practical abilities in designing robust data pipelines, optimising data workflows and leveraging the power of tools and technologies.

As I embark on this journey into the world of data engineering, I've decided to start with something simple yet foundational. Over the coming weeks, I'll be delving into more complex pipelines, but for now, let me walk you through what I've accomplished.

🔍 Project Overview:

For this initial project, I've focused on extracting data from Twitter using Python. Leveraging libraries and APIs, I've developed code that scrapes tweets and stores it as an CSV file. The real magic happens when this code is deployed onto an EC2 machine configured with Ubuntu. Here, I've set up Airflow to orchestrate the entire process. 

🛠️ Technical Details:

Airflow, with its intuitive interface and robust scheduling capabilities, plays a central role in this project. By running the code developed in Python through Airflow, I automated the extraction process. The extracted data is then seamlessly stored in an AWS S3 folder, ready for further analysis or utilisation in downstream processes.

📈 Next Steps:

While this project represents a solid foundation, it's only the beginning. In the coming weeks, I'll be exploring more intricate pipelines, diving deeper into data manipulation, and leveraging the full potential of tools like Airflow and AWS.

🙏 Thank You:

Thank you for joining me on this journey! I'm excited to share my progress and learnings as I go. Feel free to explore the code, provide feedback, or reach out with any questions or ideas.

Happy coding! 🌟

## Project Architecture
![Untitled Diagram(1)](https://github.com/andreisacal/W01-DE-Twtitter-Scraping/assets/166915179/9a1ab56b-c312-44ab-8709-0b4dab061113)

## How the Pipeline Works

### Data Pipeline

1. Scheduler Trigger: Airflow triggers the execution of the ETL process defined in `twitter_etl.py`.
2. Twitter Data Collection: `twitter_etl.py` sends a request to the Twitter using a scraper API key and a specified search query. It collects Twitter data, processes the response, and formats it into a list.
3. Data Formatting: The collected Twitter data is converted into a pandas DataFrame for better manipulation and analysis.
4. CSV File Creation: The DataFrame is exported as a CSV file named `AWS_twitter_data.csv`.
5. Data Upload to S3: The CSV file is uploaded to an Amazon S3 bucket using the s3fs library, making it accessible for storage and further processing.
6. Data Query and Display: After the data is stored in the desired location (S3 bucket), it can be queried and displayed using various AWS services like AWS Glue, Amazon Athena, or Amazon Redshift.

This pipeline automates the process of collecting Twitter data, storing it in a reliable and scalable storage solution (Amazon S3), and sets the stage for further analysis and visualization using AWS services.

![image](https://github.com/andreisacal/W01-DE-Twtitter-Scraping/assets/166915179/8ed80ce7-efe6-4401-b631-a30023d9c418)

![image](https://github.com/andreisacal/W01-DE-Twtitter-Scraping/assets/166915179/57ad291b-cd9c-4ff5-a1d7-1289fc57b567)

![image](https://github.com/andreisacal/W01-DE-Twtitter-Scraping/assets/166915179/52f3a0a6-1859-4510-bc59-ea9d33ec851f)

![image](https://github.com/andreisacal/W01-DE-Twtitter-Scraping/assets/166915179/68de744a-0268-449d-9bf9-4f35c0754c6b)

## Services Used

- #### APIs: [ScraperAPI](https://www.scraperapi.com/)
- #### AWS: [AWS](https://aws.amazon.com/)
  - #### EC2: [EC2](https://aws.amazon.com/ec2/)
  - #### S3: [S3](https://aws.amazon.com/s3/)
  - #### ATHENA: [ATHENA](https://aws.amazon.com/athena/)
