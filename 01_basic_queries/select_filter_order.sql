-- File Name: select_filter_order.sql
-- Purpose: Demonstrates basic SELECT, WHERE, and ORDER BY commands on customer data.
-- Dataset: A non-public dataset. 
-- Author: dataanalysit26
-- Date: 2025-12-07
--------------------------------------------------------------

-- OBJECTIVE: To establish the fundamental grouping dimensions for subsequent metric aggregation.
--            This query lists the unique daily (ad_date) combinations available for each campaign (campaign_id)
--            within the Facebook ad data.

-- BUSINESS LOGIC: In Data Analysis, it is essential to define the dimensions
--                 before applying aggregation functions like SUM().
--                 This query forms the foundation for the subsequent aggregation query
--                 which will group metrics based on 'campaign_id' and 'ad_date'.

SELECT
	campaign_id,
	ad_date 
FROM 
	facebook_ads_basic_daily
GROUP BY 
	campaign_id,
	ad_date
ORDER BY 
	ad_date ASC 

--------------------------------------------------------------
  
-- OBJECTIVE: To calculate the fundamental Key Performance Indicators (KPIs) for each campaign on a daily basis.
--            This query aggregates 'spend', 'impressions', 'clicks', and 'total value' using the defined campaign and date dimensions.

-- BUSINESS LOGIC: This step serves as the core data transformation for dashboarding and high-level performance tracking.
--                 It converts granular row-level data into essential summary metrics, enabling analysts to monitor
--                 resource allocation (Spend) against campaign reach (Impressions), user engagement (Clicks), and profitability (Total Value/Revenue).

SELECT
	campaign_id,
	ad_date,
	sum(spend) AS total_cost,
	sum(impressions) AS total_campaigns_impressions,
	sum(clicks) AS total_clicks,
	sum(value) AS total_value
FROM 
	facebook_ads_basic_daily
WHERE
	campaign_id IS NOT NULL
GROUP BY 
	campaign_id,
	ad_date
ORDER BY
	ad_date ASC
  
--------------------------------------------------------------

-- OBJECTIVE: To calculate the four core campaign efficiency metrics—CPC, CPM, CTR, and ROMI—for each daily campaign segment.
--            These metrics are essential for performance evaluation and benchmarking across different media sources.

-- BUSINESS LOGIC: This step moves beyond simple aggregation to derive key ratios that directly measure campaign effectiveness.
--                 - CPC (Cost Per Click) and CPM (Cost Per Mille) assess expenditure efficiency.
--                 - CTR (Click-Through Rate) measures user engagement.
--                 - ROMI (Return on Marketing Investment) determines profitability.
--                 The resulting table forms the definitive source for strategic marketing decision-making and optimization.

/*CPC*/
  
SELECT 
	campaign_id,
	ad_date,
	COALESCE (
		ROUND(
			SUM(spend) / NULLIF (SUM(clicks), 0),
			2
			),
		0
		) AS CPC
FROM
	facebook_ads_basic_daily
GROUP BY 
	campaign_id,
	ad_date
ORDER BY
	cpc DESC 
	
/*CPM*/

SELECT 
	DISTINCT campaign_id,
	ad_date,
	ROUND(
		1000.0 * SUM(spend) / NULLIF(SUM(impressions), 0),
		2
		) AS CPM
FROM 
	facebook_ads_basic_daily
GROUP BY
	campaign_id, 
	ad_date
ORDER BY
	CPM DESC  

/*CTR*/
	
SELECT 
	campaign_id,
	ad_date,
	COALESCE (
		ROUND(100 * SUM(clicks) / NULLIF(SUM(reach), 0), 2),
		0
	) AS CTR 
FROM 
	facebook_ads_basic_daily
GROUP BY
	campaign_id, 
	ad_date
ORDER BY
	CTR DESC 
	
/*ROMI*/

SELECT 
	campaign_id,
	ad_date,
	COALESCE (
			ROUND(
				((SUM(value) - SUM(spend)) * 100.0) / NULLIF(SUM(spend), 0),
				2
			),
			0
	) AS ROMI
FROM 
	facebook_ads_basic_daily
GROUP BY 
	campaign_id,
	ad_date
ORDER BY 
	ROMI ASC

--------------------------------------------------------------
	
-- OBJECTIVE: To identify the single best-performing campaign (highest ROMI) among those with substantial financial investment (Total Spend > 500,000).
--            This addresses a common business need: prioritizing high-budget campaigns for maximum profitability.

-- BUSINESS LOGIC: This analysis uses two critical SQL steps:
--                 1. The GROUP BY clause is used to aggregate all metrics by 'campaign_id'.
--                 2. The HAVING clause is applied *after* aggregation to filter the results, ensuring only campaigns exceeding the $500,000 spend threshold are considered.
--                 Finally, the result is ordered and limited to identify the single most profitable campaign (MAX ROMI) within the high-spend group.

SELECT 
	campaign_id,
	SUM(spend) AS total_cost,
	COALESCE(
		ROUND(
			(100.0 * (SUM(value) - SUM(spend))) / NULLIF(sum(spend), 0),
			2					
		),
		0
	) AS ROMI
FROM 
	facebook_ads_basic_daily
GROUP BY 
	campaign_id
HAVING 
 	SUM(spend) > 500000
ORDER BY
	ROMI DESC
LIMIT
	1
	

	
