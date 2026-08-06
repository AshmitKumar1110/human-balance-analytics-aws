# STEDI Human Balance Analytics – AWS Data Engineering Project

An end-to-end AWS Data Engineering project that builds a scalable ETL pipeline using **AWS Glue Studio**, **Amazon S3**, **AWS Glue Data Catalog**, **Amazon Athena**, **PySpark**, and **SQL**. The project transforms raw sensor and customer data into curated datasets for machine learning while maintaining customer privacy.

---

## 📖 Project Overview

The STEDI Human Balance Analytics project demonstrates the implementation of a cloud-based data engineering pipeline using AWS services.

The pipeline ingests raw customer, accelerometer, and step trainer data into Amazon S3, processes the data through multiple transformation stages, and stores it in a multi-zone Data Lake architecture consisting of:

- Landing Zone
- Trusted Zone
- Curated Zone

The final curated dataset is prepared for machine learning applications.

---

## 🚀 Technologies Used

- AWS Glue Studio
- Amazon S3
- AWS Glue Data Catalog
- Amazon Athena
- PySpark
- SQL
- JSON
- AWS Cloud

---

# 🏗️ Data Pipeline Architecture

```text
Customer Landing
        │
        ▼
Customer Trusted
        │
        ▼
Customer Curated
        │
        ├──────────────┐
        │              │
        ▼              ▼
Step Trainer       Accelerometer
Landing              Landing
        │              │
        ▼              ▼
Step Trainer      Accelerometer
Trusted              Trusted
        \              /
         \            /
          \          /
           ▼        ▼
   Machine Learning Curated
```

---

## 📂 ETL Workflow

### 1. Customer Landing → Trusted

- Reads raw customer data from Amazon S3.
- Filters customers who have agreed to share research data.
- Stores trusted customer records.

**Output**

```
customer_trusted
```

---

### 2. Accelerometer Landing → Trusted

- Reads accelerometer landing data.
- Reads trusted customer records.
- Joins datasets using customer email.
- Removes personally identifiable information (PII).
- Stores trusted accelerometer data.

**Output**

```
accelerometer_trusted
```

---

### 3. Customer Trusted → Curated

- Identifies customers with accelerometer activity.
- Removes duplicate customer records.
- Creates curated customer dataset.

**Output**

```
customer_curated
```

---

### 4. Step Trainer Landing → Trusted

- Reads step trainer landing data.
- Joins with curated customers.
- Stores trusted step trainer records.

**Output**

```
step_trainer_trusted
```

---

### 5. Machine Learning Curated

- Joins Step Trainer Trusted with Accelerometer Trusted.
- Produces the final dataset required for machine learning.

**Output**

```
machine_learning_curated
```

---

# 📊 Dataset Summary

| Dataset | Records |
|---------|---------:|
| Customer Landing | 956 |
| Customer Trusted | 482 |
| Accelerometer Landing | 81,273 |
| Accelerometer Trusted | 40,981 |
| Step Trainer Landing | 28,680 |
| Step Trainer Trusted | 14,460 |
| Machine Learning Curated | 43,681 |

---

# 📁 Repository Structure

```
stedi-human-balance-analytics-aws/
│
├── README.md
├── LICENSE
├── .gitignore
│
├── glue-jobs/
│   ├── customer_landing_to_trusted.py
│   ├── accelerometer_landing_to_trusted.py
│   ├── customer_trusted_to_curated.py
│   ├── step_trainer_trusted.py
│   └── machine_learning_curated.py
│
├── sql/
│   ├── customer_landing.sql
│   ├── customer_trusted.sql
│   ├── customer_curated.sql
│   ├── accelerometer_landing.sql
│   ├── accelerometer_trusted.sql
│   ├── step_trainer_landing.sql
│   ├── step_trainer_trusted.sql
│   └── machine_learning_curated.sql
│
├── screenshots/
│
├── report/
│   └── STEDI_Human_Balance_Analytics_Report.pdf
│
└── docs/
```

---

# ☁️ AWS Services Used

- Amazon S3
- AWS Glue Studio
- AWS Glue Data Catalog
- Amazon Athena
- PySpark

---

# 💡 Skills Demonstrated

- Data Engineering
- ETL Pipeline Development
- AWS Glue Studio
- Amazon Athena
- Amazon S3
- PySpark
- SQL
- Data Lake Architecture
- Cloud Data Engineering
- Data Transformation
- Data Integration

---

# 📸 Screenshots

Include screenshots for:

- Customer Landing → Trusted
- Accelerometer Landing → Trusted
- Customer Trusted → Curated
- Step Trainer Trusted
- Machine Learning Curated
- Athena Query Results

---

# 🎯 Learning Outcomes

Through this project, I gained practical experience in:

- Building end-to-end ETL pipelines
- Designing cloud-based data lakes
- Processing data using PySpark
- Creating AWS Glue workflows
- Querying datasets with Amazon Athena
- Preparing datasets for machine learning
- Working with AWS cloud services

---

## 👨‍💻 Author

**Ashmit Kumar**

- GitHub: https://github.com/<your-username>
- LinkedIn: https://linkedin.com/in/<your-linkedin>

---

## ⭐ If you found this project useful, consider giving it a star!
