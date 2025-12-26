# Seattle Neighborhood Social & Health Disparities

This project analyzes neighborhood-level social, demographic, and health indicators across Seattle to better understand patterns of disadvantage and inequality. Using publicly available data, the analysis explores how socioeconomic factors, demographic composition, and disability prevalence relate to key health outcomes.

The project combines data cleaning, exploratory analysis, multivariate modeling, and an interactive Shiny web application to support data-driven insight at the neighborhood level.

## Project Goals
- Examine relationships between social metrics and health outcomes across Seattle neighborhoods
- Identify underlying dimensions of disadvantage using multivariate analysis
- Communicate findings through clear visualizations and an interactive web application

## Methods & Analysis
- Data cleaning and integration of multiple public datasets
- Exploratory data analysis and visualization
- Principal Component Analysis (PCA) to identify dominant dimensions of variation
- Statistical modeling to explore relationships between social factors and health outcomes

## Shiny Web Application
An interactive Shiny application allows users to:
- Explore neighborhood-level indicators
- Filter and compare health and social metrics
- View modeled relationships and summaries
- Interact with visualizations and data tables

The Shiny app code is located in the `shiny-app/` directory.

## Repository Structure
```text
├── analysis/        # Data preparation and exploratory analysis
├── data/            # Analysis-ready dataset
├── shiny-app/       # Interactive Shiny application
└── README.md
