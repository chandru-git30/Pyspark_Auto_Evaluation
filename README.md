# pyspark_Auto_Evaluation🚀✨
This repository contains code to automatically evaluate PySpark applications. The code is efficient, scalable, and easy to use. It is also open source, so you can modify it to fit your needs.

## Prerequisites 🛠️
Before you begin, make sure you have the following installed on your machine:

- Python:https://www.python.org/downloads/ 🐍 💻
- pyspark:https://pypi.org/project/pyspark/ 🐍💥 (If you need only pyspark) or
- spark:https://spark.apache.org/downloads.html (If you need the complete spark)💥
- pytest:https://pypi.org/project/pytest/ 🔬🧪
- Git: https://git-scm.com/downloads 📦 (Used to clone the repository)

## Getting Started

1. **Clone the Repository:**
   ```bash
   git clone https://github.com/chandru-git30/Pyspark_Auto_Evaluation.git
   cd Pyspark_Auto_Evaluation
   ```
2. **Execute test cases:**
   ```bash
   pytest test.py -v
   ```
   **You will get output similar to this**
   ![pytest](https://github.com/chandru-git30/Pyspark_Auto_Evaluation/assets/82560086/ede8fad8-cec0-461a-84c8-8f6918193270)

## Working

- Write pyspark script that need to be autoevaluated
- Write a test file with correct possible way of output functions for the pyspark script with asserting functions
- Execute pytest command ***pytest file_name*** to get the evaluation resule
- For detailed evaluation resule execute ***pytest file_name -v*** with verbouse attribute
