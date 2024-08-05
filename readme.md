# IndexIDH - Human Development Index Calculation

This Python script calculates the Human Development Index (HDI) for Puerto Rico using health, income, and education indices. The HDI is a summary measure of a nation's average achievements in three basic dimensions of human development: health, education, and income.

## Prerequisites

Before running the script, make sure you create the virtual enviroment and activate it:

```bash
git clone https://github.com/unclearcoder/IDH-index.git
cd IDH-index
conda env create -f environment.yml
conda env activate research_env
```
> [!IMPORTANT]  
> This project uses pola.rs as une of its dependencies. This libary requires Rust to be installed on your system. You can install Rust from the [official Rust website](https://www.rust-lang.org/)

This will create a new conda environment with the required libraries.

## Screenshots
![record](assets/Kooha-2024-03-05-17-49-41.gif)

## Usage

### IDH Index module

```python
from IndexIDH import IndexIDH

# Create an instance of the IndexIDH class
idh = IndexIDH()

# Calculate and save the health index
idh.health_index()

# Calculate and save the income index
idh.income_index()

# Calculate and save the education index
idh.edu_index()

# Calculate and save the overall Human Development Index
idh.idh_index()
```

### Description

- **Health Index**: Calculates the health index based on life expectancy.
- **Income Index**: Calculates the income index using Atlas GNI per capita and GNI constant.
- **Education Index**: Calculates the education index based on mean years of schooling and expected years of schooling.
- **Human Development Index (HDI)**: Combines the health, income, and education indices to calculate the overall HDI.

### fetch.py

The data retrieval module (`fetch.py`) fetches data from the following sources:

- [World Bank](https://data.worldbank.org/): Health, Income, and Education Indicators
- [GNI](https://data.worldbank.org/indicator/NY.GNP.PCAP.PP.CD): GDP per Capita, GNI Constant
- [Census](https://www2.census.gov/programs-surveys/acs/data/pums/): Population, Housing Units, and Income by Sex, Race, and Ethnicity for 5-Year
- [Informe del Gobernador](https://jp.pr.gov/informe-economico-al-gobernador/): GDP for Puerto Rico

## Folder Structure

```
├── LICENSE
├── main.py            <- Main script
├── README.md          <- Description of the project and usage instructions
├── data
│   ├── external       <- Data from third party sources.
│   ├── processed      <- The final, processed data sets in CSV format.
│   └── raw            <- The original, immutable data dump.
│
├── docs               <- A default 
│
├── notebooks          <- Jupyter notebooks. Naming convention is a number (for ordering),
│                         the creator's initials, and a short `-` delimited description, e.g.
│                         `1.0-jqp-initial-data-exploration`.
│
├── src                <- Source code for use in this project.
│   ├── __init__.py    <- Makes src a Python module
│   │
│   ├── data           <- Scripts to download or generate data
│   │   └── make_dataset.py
│   │
│   └── visualization  <- Scripts to create exploratory and results oriented visualizations
│       └── visualize.py
```

## Running the Script

1. Instantiate the `IndexIDH` class.
2. Call the individual index calculation methods: `health_index`, `income_index`, `edu_index`, and `idh_index`.
3. The calculated indices are saved in the `data/processed/` folder.

## Note

- The script assumes specific data files are available in the `data/raw/` and `data/external/` folders.
- Adjustments to the calculation methodology can be made by modifying the code as needed.
