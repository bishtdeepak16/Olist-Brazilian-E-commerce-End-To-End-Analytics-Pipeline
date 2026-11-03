# Olist Brazilian E-commerce Analytics Project

End-To-End analytics project on real Olist Brazilian e-commerce data (2017–2018). This project blends technical expertise with clear business understanding to uncover what truly drives online retail performance and customer value.

Here, intricate and complex data is transformed into straightforward, actionable insights making the journey accessible and valuable for decision-makers, technical teams, and business leaders alike. From intelligent data cleaning and enrichment to user-friendly dashboards and practical recommendations, every element is designed to inform and inspire, fostering smarter conversations about growth, retention, and customer experience in the evolving world of e-commerce.

---

## Project Goals

- Tackle huge, messy and complex e-commerce data
- Deliver insights that drive business action, not just reports

---

## Repository Structure

- **sql/** – Automated cleaning, schema, business SQL queries
- **python/** – Deep cleaning and enrichment: fuzzy matching, Brazilian to English translations of review titles, review categorisation by NLP
- **powerbi_dashboards/** – Dashboards, including business findings and strategy
- **reports/** – Insight_Report: the full story, including critical recommendations and strategies for business growth
- **data/** – All raw data csv files

---

## Data Model Overview

The project uses a star schema for clear and efficient analytics.
![Data Model - Star Schema](Data%20Model%20-%20Star%20Schema.png)

---

## Power BI Dashboards Overview

- **Executive Summary**  
  *One-page overview of total revenue, orders, ratings, and top-performing cities and products, key for quick insights by leaders.*

- **Sales Performance**  
  *Breaks down revenue, average order value, payment types, average installments and revenue trends over time, helping spot sales drivers, seasonal patterns and overall growth.*

- **Delivery Performance**  
  *Shows average delivery times, delay rates, delivery days distribution, delivery status and where deliveries are slow or face bottlenecks, enabling focused operational improvements.*

- **Review and Ratings Analysis**  
  *Visualizes customer ratings, identifies causes for poor reviews, and shows which categories or regions have the most positive reviews and satisfied customers while diving deep into lacking areas also.*

- **Geographic Trends**  
  *Interactive map showing total revenue, orders, and delivery times by state/city, and state-to-state route visualization showcasing order and revenue volumes and delivery delays on the most trafficked logistics paths.*

- **Insight Report & Strategic Recommendations**  
  *Highlights root business reports and problems, summarizes key analytical findings, and presents strategic actions to boost retention and sales.*

---

## Executive Summary & Strategic Action (Highlights)

### Key Problems Identified
- Customer retention rate is only 3%
- Average delivery days (12.48) exceed market benchmarks
- Consistent negative correlation between delivery time and customer satisfaction
- Major delivery delays affect top revenue routes
- Spike of negative reviews in Feb & March 2018

> For complete insights and business strategic recommendations, see Olist Insight Reports & Strategic Recommendations Folder

---

## Project Highlights

- **SQL pipeline:** Automated profiling, robust cleaning of >1 million records, hybrid ETL
- **Python enrichment:** Fuzzy matching for typos, NLP review categorization, Azure review translation
- **Power BI dashboards:** Visual, executive-ready reporting: sales, customer, delivery, and review trends, each with clear business context and recommended actions

---

## Tech Used

- PostgreSQL, Python (pandas, fuzzywuzzy, Azure Translator, NLP), Power BI

---

## How to Use

- Run SQL and Python scripts in order shown
- Explore dashboards (png/pdf), or load .pbix for deeper analytics
- Read the insight report for high-level and strategic recommendations

---

<div align="center">

## 📸 Dashboard Screenshots

<sub>Power BI dashboards built on the Olist dataset. The editable file is in the same folder: <a href="3%20-%20Power%20BI%20Dashboards/olist%20e-commerce%20project.pbix">olist e-commerce project.pbix</a></sub>

<br><br>

### 1️⃣ Executive Summary
<sub>Revenue, orders, ratings, top cities and products at a glance</sub>

<img src="3%20-%20Power%20BI%20Dashboards/1-%20Executive%20Summary%20Dashboard.png" width="92%" alt="Executive Summary Dashboard">

<br><br>

---

### 2️⃣ Sales Performance
<sub>Revenue, average order value, payment types and trends over time</sub>

<img src="3%20-%20Power%20BI%20Dashboards/2-%20Sales%20Performance%20Report.png" width="92%" alt="Sales Performance Report">

<br><br>

---

### 3️⃣ Delivery Performance
<sub>Delivery times, delay rates and logistics bottlenecks</sub>

<img src="3%20-%20Power%20BI%20Dashboards/3-%20Delivery%20Performance%20Report.png" width="92%" alt="Delivery Performance Report">

<br><br>

---

### 4️⃣ Review & Ratings
<sub>Customer ratings, causes of poor reviews and category-wise satisfaction</sub>

<img src="3%20-%20Power%20BI%20Dashboards/4-%20Review%20%26%20Ratings%20Report.png" width="92%" alt="Review and Ratings Report">

<br><br>

---

### 5️⃣ Geographic Trends
<sub>State and city revenue, orders, delivery times and key logistics routes</sub>

<img src="3%20-%20Power%20BI%20Dashboards/5-%20Geographic%20Report.png" width="92%" alt="Geographic Report">

<br>

</div>

---

<div align="center">

## 📑 Insight Report & Strategic Recommendations

<sub>Business findings from the analysis and the strategic actions recommended to boost retention, delivery performance and sales</sub>

<br><br>

### 📊 Olist Insights Report
<sub>Root business problems and key analytical findings</sub>

<img src="4%20-%20Olist%20Insight%20Reports%20%26%20Strategic%20Recommendations/1%20-%20Olist%20Insights%20Report.png" width="92%" alt="Olist Insights Report">

<br><br>

---

### 🎯 Key Problems & Strategic Recommendations · Part 1
<sub>Critical issues identified and the first set of recommended actions</sub>

<img src="4%20-%20Olist%20Insight%20Reports%20%26%20Strategic%20Recommendations/Key%20Problems%20%26%20Strategic%20Recommendations-1.png" width="92%" alt="Key Problems and Strategic Recommendations part 1">

<br><br>

---

### 🎯 Key Problems & Strategic Recommendations · Part 2
<sub>Remaining recommendations to improve retention, delivery and revenue</sub>

<img src="4%20-%20Olist%20Insight%20Reports%20%26%20Strategic%20Recommendations/Key%20Problems%20%26%20Strategic%20Recommendations-2.png" width="92%" alt="Key Problems and Strategic Recommendations part 2">

<br>

</div>
