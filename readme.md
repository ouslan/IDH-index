# IndexIDH - Human Development Index Calculation

This Python script calculates the Human Development Index (HDI) for Puerto Rico using health, income, and education indices. The HDI is a summary measure of a nation's average achievements in three basic dimensions of human development: health, education, and income.

## Prerequisites

Before running the script, make sure you create the virtual enviroment and activate it:

```bash
git clone https://github.com/ouslan/IDH-index.git
cd IDH-index
```
### With Conda

I have prorvided a `environment.yml` file to create a new conda environment with the required libraries. To create the environment, run:

```bash
conda env create -f environment.yml
```

### With pip

As well as the conda environment, you can install the required libraries with pip:

```bash
pip install -r requirements.txt
```

> [!IMPORTANT]  
> This project uses pola.rs as une of its dependencies. This libary requires Rust to be installed on your system. You can install Rust from the [official Rust website](https://www.rust-lang.org/)

## Usage

To run the replication package, run the following command:
```bash
python main.py
```
This command will generate the `data/processed/index_idh.csv` file.

## Docker

You can also run the website localy using Docker. To do so, run the following command:

```bash
docker-compose up --build
```

This will host a dash application on http://localhost:7050. As well as the documentation on http://localhost:8005.


## Sponsors
<p align="center">
  <a href="https://www.uprm.edu/portada/">
    <img src='https://www.uprm.edu/wdt/resources/seal-rum-uprm.svg' alt="RUM UPRM Seal" style="width: 150px; height: auto; margin: 0 10px;" />
  </a>
  <a href="https://www.uprm.edu/economia/">
    <img src='https://external-content.duckduckgo.com/iu/?u=https%3A%2F%2Fwww.uprm.edu%2Feconomia%2Fwp-content%2Fuploads%2Fsites%2F367%2F2021%2F10%2FLOGO-FINAL-FINAL-FINAL-1-e1633713475703.png&f=1&nofb=1&ipt=7fca3d03e26f9ed10ea68617f5de316e5a36aab7f16c821a9c7c295157bf8a2f&ipo=images' alt="Economia Logo" style="width: 150px; height: auto; margin: 0 10px;" />
  </a>
</p>
