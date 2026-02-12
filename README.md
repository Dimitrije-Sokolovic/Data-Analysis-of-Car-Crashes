# Data-Analysis-of-Car-Crashes
## File name: Prikupljanje i transformacije.py - Data Transformation Notebook (Databricks)

This notebook performs **data cleaning, normalization, and transformation** for a big data analytics project focused on **traffic crash data from New York City and Serbia**.  
All transformations are implemented using **Apache Spark (PySpark)** in **Databricks Community Edition**.

---

### Data Sources

- **New York City Car Crashes**  
  Kaggle dataset containing detailed records of traffic accidents in NYC.

- **Serbia Traffic Accidents**  
  Open government dataset describing traffic accidents across Serbian municipalities.

---

### Key Transformations

#### Common Processing
- Data is loaded from **DBFS** into Spark DataFrames.
- Datasets are transformed to ensure a **consistent schema, data types, and formatting**.
- Final transformed datasets are saved back to **DBFS as CSV files** for reuse in downstream notebooks.

#### NYC Dataset Transformations
- Column names normalized to **snake_case**.
- Date and time fields converted to **Date** and **Timestamp** types.
- Missing and invalid values handled using Spark `fill()` operations.
- Numeric columns cast to appropriate data types (integer, float, long).
- Custom **Spark UDF** used to standardize street and borough name formatting.
- New analytical features created:
  - `Total_injuries`
  - `Total_fatalities`
- Unnecessary columns (e.g. ZIP code) removed.

#### Serbia Dataset Transformations
- Default Spark column names renamed to meaningful domain-specific names.
- Date and time strings converted to **Date** and **Timestamp** types.
- Geolocation and identifier columns cast to correct numeric types.
- Reuse of the same custom **UDF** for consistent text formatting.
- Descriptive text columns
