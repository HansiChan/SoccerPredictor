# SoccerPredictor
A goals prediction model based on Big-data and machine learning.

## Project Introduction

SoccerPredictor is a football match prediction system based on the XGBoost machine learning algorithm, which can predict the outcome of the match and the over/under result.

## Main Functions

- **Data Collection**: Crawl match data and odds information from sports websites
- **Model Training**: Train prediction models for wins and losses and over/under based on historical data
- **Result Prediction**: Use trained models to predict future matches
- **Data Storage**: Support Impala/Kudu database storage

## Project Structure

```
SoccerPredictor/
├── DAO/                    # Data Access Layer
│   └── ImpalaCon.py       # Impala/Kudu database connection
├── Spider/                 # Web Crawler
│   └── GameSpider.py      # Match data crawling
├── XGBoost/               # Machine Learning Model
│   └── Predictor.py       # Model training and prediction
├── Models/                # Pre-trained model storage directory
├── SQL/                   # Database table structure
│   └── DDL_SQL           # Table creation statement
├── config.py              # Configuration file
├── requirements.txt       # List of dependent packages
└── README.md             # Project description
```

## Code Optimization Description (2025-11)

This optimization mainly includes the following aspects:

### 1. Security Improvements
- ✅ **Fixed SQL injection vulnerability**: Use parameterized queries instead of string concatenation
- ✅ **Table name whitelist verification**: Prevent illegal table name injection
- ✅ **Input validation**: Add data type and boundary checks

### 2. Compatibility Improvements
- ✅ **Cross-platform path support**: Removed hard-coded Windows paths and used `os.path.join()`
- ✅ **Updated Selenium 4 API**: Replaced the deprecated `find_element_by_*` method
- ✅ **Environment variable configuration**: Support for configuring database connections through environment variables

### 3. Code Quality Improvements
- ✅ **Improved logging system**: Added detailed logging for all modules
- ✅ **Unified error handling**: Use try-except to catch and record exceptions
- ✅ **Fixed function parameter bug**: Fixed the missing parameter issue in `get_data_df()`
- ✅ **Code refactoring**: Extracted the `get_top10()` method to reduce duplicate code

### 4. Performance Optimization
- ✅ **Optimized loop efficiency**: Use `enumerate()` instead of `list.index()` to avoid O(n²) complexity
- ✅ **Optimized string operations**: Use `join()` instead of loop concatenation
- ✅ **Reduced redundant calculations**: Cached `split()` results to avoid repeated calls

### 5. Configuration Management
- ✅ **Centralized configuration management**: Added `config.py` to manage all configuration parameters uniformly
- ✅ **Support for environment variables**: Database connection, model parameters, etc. can be configured through environment variables
- ✅ **Parameterized model hyperparameters**: Facilitates tuning and experimentation

### 6. Code Cleanup
- ✅ **Deleted unused files**: Removed empty files and irrelevant sample code
- ✅ **Added dependency list**: Created `requirements.txt` to facilitate environment setup

## Installation and Use

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure the database

Edit `config.py` or set environment variables:

```bash
export IMPALA_HOST=your_host
export IMPALA_PORT=21050
export IMPALA_UID=your_username
export IMPALA_PWD=your_password
```

### 3. Data Collection

```python
from Spider.GameSpider import GameSpider

spider = GameSpider()
# Get team list
spider.get_team_ids('2019-2020', '36')
# Get game records
spider.get_game_record('19', 54)
# Get odds data
spider.get_odds('19', 0)
spider.get_overunder('19', 0)
```

### 4. Model Training

```python
from XGBoost.Predictor import Predictor

predictor = Predictor()
# Train the win/loss model
predictor.train_flat('19', 0)
# Train the over/under model
predictor.train_ou('19', 0)
```

### 5. Prediction

```python
# Predict win/loss
predictor.predict_flat()
# Predict over/under
predictor.predict_overunder()
```

## Configuration Parameter Description

The main configuration parameters are in `config.py`:

- **DATABASE**: Database connection configuration
- **TABLES**: Data table names
- **MODEL**: Model hyperparameters (test set ratio, tree depth, learning rate, etc.)
- **SPIDER**: Crawler configuration (headless mode, timeout settings, etc.)
- **LOGGING**: Log level and format

## Technology Stack

- **Python 3.x**
- **XGBoost**: Gradient boosting tree algorithm
- **Pandas/Numpy**: Data processing
- **Selenium**: Web crawler
- **PyODBC**: Database connection
- **Impala/Kudu**: Big data storage

## Notes

1. Chrome browser and ChromeDriver need to be installed
2. Make sure the network can access the target sports website
3. The database needs to create the corresponding table structure in advance (see SQL/DDL_SQL)
4. Model training requires sufficient historical data

## License

MIT License
