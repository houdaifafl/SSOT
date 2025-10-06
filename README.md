# FinalProdukt Project 
(Nyrstar x FH Aachen interdisciplinary project WS 24/25 - Single Source of Truth)

## Overview

This project is a data processing and reporting tool that integrates with various data sources and performs complex 
calculations to generate comprehensive reports. The project is primarily written in Python and includes modules for 
database connections, schema creation, data loading, calculations, and report generation.

## Project Structure

```
Python_Project/        # Main project directory
│
├── .venv/             # Virtual environment folder
│
├── File_Paths/        # Directory for file path configurations
│
├── Interface1WT/  
│   ├── src/        
│   │   ├── calculations.py
│   │   ├── data_loader.py
│   │   ├── schema_creator.py
│   │   └── main.py
│
├── interface2_IST/ 
│   ├── src/        
│   │   ├── CalculationIST.py
│   │   ├── data_loader.py
│   │   ├── schema_creator.py
│   │   └── main.py
│
├── interface30M/   
│   ├── JsonFile/       # Directory for JSON files
│   ├── src/        
│   │   ├── data_loader.py
│   │   ├── schema_creator.py
│   │   └── main.py
│
├── db_config.py        # Database configuration file
├── file_paths.py       # File paths variables
├── FinalReport2.py     # Final report generation script
├── main_creator.py     # Main creator script
├── main_loader.py      # Main data loading script
│
├── README.md           # Project documentation and usage guide (This file)
│
└── External Libraries/ # External libraries used in the project

```
## Interface Descriptions
- Interface 1: Interface for processing and loading budget and forcast data
- Interface 2: Interface for processing and loading actual data
- Interface 3: Interface for processing and loading availability data

## File Descriptions
- .venv/: Contains the virtual environment for the project, managing dependencies and packages.

- File_Paths/: Directory containing file path configurations used throughout the project.

- Interface1WT/

  - src/: Contains source files for processing budget and forecast data.
    - calculations.py: Performs calculations related to budget and forecast data.
    - data_loader.py: Loads budget and forecast data into the database.
    - schema_creator.py: Creates the database schema for budget and forecast data.
    - main.py: Main script for running the budget and forecast interface.

- interface2_IST/

  - src/: Contains source files for processing actual data.
    - CalculationIST.py: Performs calculations related to actual data.
    - data_loader.py: Loads actual data into the database.
    - schema_creator.py: Creates the database schema for actual data.
    - main.py: Main script for running the actual data interface.

- interface3OM/

  - JsonFile/: Contains JSON files used in the processing of availability data.
  - src/: Contains source files for processing availability data.
    - data_loader.py: Loads availability data into the database.
    - schema_creator.py: Creates the database schema for availability data.
    - main.py: Main script for running the availability data interface.

- db_config.py: Database configuration file containing connection settings.

- file_paths.py: Defines file path configurations for accessing required data.

- FinalReport2.py: Handles the generation and export of combined reports to Excel files.

- main_creator.py: Script to manage and organize main project components.

- main_loader.py: Script responsible for loading all data interfaces.

## Installation

1. **Python 3.x**: Ensure you have Python 3.x installed on your system.

2. **Install Required Python Packages**:
   - Install the dependencies using the following command:
     ```sh
     pip install -r requirements.txt
     ```

## FH Aachen Team Members
Eggert, Mathias |
Lanjri, Houdaifa |
Kirchhoff, Luca |
Springer, Markus |
Chang, Wei-Ting |
Faiz, Omar
