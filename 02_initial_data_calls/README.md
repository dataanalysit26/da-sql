# 📊 Digital Marketing Campaign Performance Analysis (SQL)

This project was developed to provide input for budget optimization by deeply analyzing the monthly performance of a company's digital advertising campaigns, calculating core marketing metrics, and determining the month-over-month percentage change of these metrics.

## 🎯 Project Objectives

* To calculate key campaign performance metrics on a monthly basis (ROMI, CTR, CPC, CPM).
* To analyze the month-over-month percentage change of critical metrics for each `utm_campaign` using the **`LAG`** SQL window function.

## 🛠️ Technologies Used

* **SQL:** Data manipulation, CTEs (Common Table Expressions), and Advanced Window Functions (`LAG`).
* **Metrics:** ROMI (Return on Marketing Investment), CTR (Click-Through Rate), CPC (Cost Per Click), CPM (Cost Per Mille).

## 🚀 Key Query Steps

1.  **Core Metric Calculation (CTE 1):** Essential fields such as `total cost`, `conversion value`, `impressions`, and `clicks` were defined for each month (`ad_month`) and campaign (`utm_campaign`) using advertising data.
2.  **Time-Series Comparison (CTE 2):** Utilizing the results of the first CTE, previous month's metrics were placed alongside the current month's values for each campaign using the **`LAG`** SQL Window Function.
3.  **Percentage Change Calculation:** The monthly percentage changes for **`CPM`**, **`CTR`**, and **`ROMI`** were calculated based on the comparison between the previous and current month's metrics.

## 📈 Outcome and Deliverables

This analysis provides a **data-driven and automated** reporting mechanism that enables the Marketing team to quickly identify which campaigns are losing effectiveness (ROMI decrease) or increasing in cost (CPM increase).

## 🔍 Key Findings / Insights (To be added later)

[Insert your key quantifiable findings and business impact here.]

**[Live Dashboard Link (Optional)]**
