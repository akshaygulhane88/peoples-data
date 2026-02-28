# Peoples Data Project

## 📌 Overview
This is a config-driven data engineering project that reads configuration from AWS S3, processes data, and integrates with Snowflake.

## 🏗 Project Structure

```bash
Peoples_data/
│
├── cdp/
├── pyframe/
├── main.py
├── requirements.txt
└── .gitignore
```

## ⚙️ Tech Stack
- Python
- AWS S3 (boto3)
- Snowflake Connector
- JSON Config Framework

## 🚀 How to Run

```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
.\venv\Scripts\Activate.ps1

# Install dependencies
pip install -r requirements.txt

# Run project
python main.py
```

## 🔐 Note
Do not store AWS keys or credentials directly in code.