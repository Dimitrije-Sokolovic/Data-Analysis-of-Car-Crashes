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

---
## File name: Vizualizacija.ipynb - Data Visualization and Exploratory Analysis

This notebook focuses on **visualizing and analyzing traffic accident data** from **New York City** and **Serbia** using the transformed datasets produced earlier in the project.  
The analysis is performed in **Google Colab** using **Pandas** and interactive visualization libraries.

---

### Data Input
- Transformed CSV datasets loaded from local storage:
  - `dfny.csv` – New York City traffic accidents
  - `dfsrb.csv` – Serbia traffic accidents

---

### Visualization Tools
- **Matplotlib**
- **Seaborn**
- **Plotly Express & Plotly Graph Objects**

Interactive Plotly visualizations are used extensively to explore temporal, spatial, and categorical patterns.

---

### Exploratory Analysis

#### Temporal Analysis
- Number of crashes analyzed by:
  - **Date**
  - **Quarter of the year**
  - **Month**
  - **Week of the year**
  - **Day of the month**
  - **Day of the week**
- Identification of seasonal and weekly trends in accident frequency.

#### Time-of-Day Analysis
- Accidents grouped into custom time periods:
  - Midnight
  - Dawn
  - Morning
  - Noon
  - Evening
  - Night
- Comparison of crash distribution by time of day for both regions.
- Visualization using **pie charts** and **scatter plots**.

#### Location-Based Analysis
- NYC accidents analyzed by:
  - **Borough**
  - **Street names** (Top 10 most frequent streets)
- Identification of high-risk locations.

---

### Comparative Analysis (NYC vs Serbia)
- Side-by-side comparison of:
  - Seasonal trends
  - Weekly and daily crash distributions
  - Time-of-day crash patterns

---

### Hypothesis Testing

The notebook includes **multiple hypotheses**, each evaluated through data visualization:

- Increase in NYC crashes around **Independence Day (July 4th)**.
- Increase in Serbia crashes during **New Year period**.
- Comparison of crash frequency during **weekdays vs weekends**.
- Analysis of most common **causes of crashes** in NYC.
- Relationship between **time of day** and crash frequency.
- Comparison of crashes:
  - With injuries vs material damage only
  - Based on number of vehicles involved

Each hypothesis is clearly marked as:
- **Confirmed** or
- **Rejected**,  
based on visual evidence from the data.

---

### Key Insights
- Strong seasonal and weekly patterns exist in both datasets.
- Peak accident frequency occurs during **afternoon traffic hours**, not late night.
- Weekends show higher accident frequency despite fewer days.
- Driver inattention is the leading cause of crashes in NYC.
- Most accidents involve **two or more vehicles**.

---

### Technologies Used
- **Python**
- **Pandas**
- **Matplotlib**
- **Seaborn**
- **Plotly**
- **Google Colab**
