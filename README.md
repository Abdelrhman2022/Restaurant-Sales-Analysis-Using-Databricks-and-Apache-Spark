# Restaurant Sales Analysis Using Databricks and Apache Spark

## Overview
This project aims to analyze BreadcrumbsRestaurant sales data using Apache Spark within Databricks, integrated with Amazon S3. The primary goal is to derive insights into sales trends, customer behavior, and product performance, facilitating data-driven decision-making for the e-commerce platform.

## Table of Contents
- [Overview](#overview)
- [Features](#features)
- [Data Source](#data-source)
- [Architecture](#architecture)
- [Installation](#installation)

## Features
- **Data Integration:** Import product and order data from S3 into Databricks.
- **Data Transformation:** Clean and transform data, adding time-based columns for enhanced analysis.
- **Descriptive Analytics:** Aggregate sales data by customer, product, month, quarter, and year.
- **Visualization:** Generate visualizations for key metrics using Databricks' built-in functions.

## Data Source
- **Product Data:** `product.txt` containing product IDs, names, and prices.
- **Order Data:** `order.txt` with order information including product IDs, customer IDs, order dates, and locations.

## Architecture
- **Amazon S3:** Serves as the data lake for storing raw data files.
- **Databricks:** The cloud-based platform used for data processing and analysis.
- **Apache Spark:** The core engine for distributed data processing.
- **Python:** The scripting language used for data manipulation and analysis.

## Installation
1. **Set Up Databricks:**
   - Create a Databricks account and workspace.
   - Set up a cluster with appropriate resources.

2. **Connect to S3:**
   - Ensure you have the necessary permissions to access the S3 bucket.
   - Configure Databricks to connect to S3 by setting up AWS credentials.

3. **Clone the Repository:**
   ```bash
   git clone https://github.com/Abdelrhman2022/Restaurant-Sales-Analysis-Using-Databricks-and-Apache-Spark
   
© Copyright Abdelrahman Ragab Nady
