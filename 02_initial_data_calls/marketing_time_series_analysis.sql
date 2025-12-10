-- File Name: marketing_time_series_analysis.sql
-- Purpose: Executes a full ETL and Analysis pipeline: 1. Merges data from multiple ad sources (Facebook/Google). 2. Cleans URL parameters using a custom PL/pgSQL function. 3. Calculates core marketing KPIs (CTR, CPC, CPM, ROMI). 4. Determines Month-over-Month (MoM) percentage change using SQL Window Functions (LAG).
-- Dataset: Assumes access to 'facebook_ads_basic_daily' and 'google_ads_basic_daily' tables.
-- Author: dataanalysit26
-- Date: 2025-12-11
--------------------------------------------------------------------------------

-- OBJECTIVE: To provide a unified, clean, and comparative view of multi-channel advertising performance,
--            focusing on identifying efficiency trends (MoM) for strategic optimization.

-- BUSINESS LOGIC: This script demonstrates advanced SQL skills by handling data integration (UNION ALL),
--                 data cleaning (Custom Function), complex metric derivation (ROMI, CTR), and time-series analysis (LAG).

-- ##########################################################################
-- # 0. Custom Function Definition: URL Decoding (PL/pgSQL)
-- ##########################################################################
-- Description: Decodes URL-encoded strings (e.g., handling '%' symbols) within the utm_campaign parameter to ensure accurate grouping.
-- This custom function enhances data quality before aggregation.

CREATE OR REPLACE FUNCTION url_decode_utm(p_text text)
RETURNS text AS $$
{
CREATE OR REPLACE FUNCTION url_decode_utm(p_text text)
RETURNS text AS $$
DECLARE
    v_result bytea := ''::bytea;
    i int := 1;
    hex text;
    decoded_byte bytea;
BEGIN
    WHILE i <= length(p_text) LOOP
        IF substr(p_text, i, 1) = '%' AND i + 2 <= length(p_text) THEN
            hex := substr(p_text, i + 1, 2);

            IF hex ~ '^[0-9A-Fa-f]{2}$' THEN
                decoded_byte := decode(hex, 'hex');
                v_result := v_result || decoded_byte;
                i := i + 3;
            ELSE
                -- '%' karakterini bytea'ya ASCII olarak ekle
                v_result := v_result || substr(p_text, i, 1)::bytea;
                i := i + 1;
            END IF;
        ELSE
            -- Diğer karakterleri ASCII bytea olarak ekle
            v_result := v_result || substr(p_text, i, 1)::bytea;
            i := i + 1;
        END IF;
    END LOOP;

    RETURN convert_from(v_result, 'UTF8');
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;
}
-- ##########################################################################
-- # 1. ETL & KPI Calculation (CTEs)
-- ##########################################################################

WITH cte_all_adds AS (
-- Adım 1.1: Facebook ve Google verilerini birleştir ve temel alanları seç. (Data Integration)
-- Step 1.1: UNION ALL data from Facebook and Google sources, selecting core fields.
SELECT 
	f.ad_date,
	f.url_parameters,
	f.spend,
	f.impressions,
	f.reach,
	f.clicks,
	f.leads,
	f.value
FROM 
	facebook_ads_basic_daily AS f
UNION ALL
SELECT 
	g.ad_date,
	g.url_parameters,
	g.spend,
	g.impressions,
	g.reach,
	g.clicks,
	g.leads,
	g.value
FROM 
	google_ads_basic_daily AS g
),
cte_ad_month_calc AS (
-- Adım 1.2: URL temizliği yap, aylık bazda grupla ve tüm KPI'ları hesapla.
-- Step 1.2: Clean URL, group by month, and calculate all core marketing KPIs (CTR, CPC, CPM, ROMI).
SELECT 
	date(date_trunc('month', ad_date)) AS ad_month,
	url_decode_utm(substring(lower(url_parameters) from 'utm_campaign=([^&]+)')) AS utm_campaign_value,
	SUM(spend) AS total_cost,
	SUM(impressions) AS impressions,
	SUM(clicks) AS clicks,
	COALESCE(
		ROUND(
			SUM(leads) :: NUMERIC / NULLIF(SUM(clicks), 0) ,2
			) 
			,0) AS click_conversion_rate, -- Yeni KPI eklendi: Leads/Clicks
	COALESCE(
		ROUND( 
			100 * SUM(clicks) :: NUMERIC / NULLIF(SUM(impressions), 0) ,2
			)
			,0) AS CTR,
	COALESCE(
		ROUND(
			SUM(spend) :: NUMERIC / NULLIF(SUM(clicks), 0) ,2 
			)
			,0) AS CPC,
	COALESCE(
		ROUND( 
			 1000 * SUM(spend) :: NUMERIC / NULLIF(SUM(impressions), 0) ,2
			)
			,0) AS CPM,
	COALESCE(
		ROUND(
			100 * (SUM(value) - SUM(spend)) :: NUMERIC / NULLIF(SUM(spend), 0), 2
			)
			,0) AS ROMI	
FROM 
	cte_all_adds 
GROUP BY 
	ad_month,
	utm_campaign_value
),
cte_lag_data_calc AS (
-- Adım 2: LAG fonksiyonu ile bir önceki ayın değerlerini al. (Time-Series Prep)
-- Step 2: Retrieve the previous month's KPI values using the LAG Window Function.
SELECT 
	ad_month,
	utm_campaign_value,
	total_cost,
	impressions AS number_of_impressions,
	clicks AS number_of_clicks,
	click_conversion_rate,
	cpm,
	lag(cpm) OVER(PARTITION BY utm_campaign_value ORDER BY ad_month) AS lag_cpm,
	ctr,
	lag(ctr) OVER(PARTITION BY utm_campaign_value ORDER BY ad_month) AS lag_ctr,
	romi,
	lag(romi) OVER(PARTITION BY utm_campaign_value ORDER BY ad_month) AS lag_romi
FROM 
	cte_ad_month_calc
)
-- ##########################################################################
-- # 3. Final Output: Calculate MoM % Change
-- ##########################################################################
SELECT 
	ad_month,
	utm_campaign_value,
	total_cost,
	number_of_impressions,
	number_of_clicks,
	click_conversion_rate,
	cpm,
	-- CPM Aylık Yüzde Değişimi (CPM MoM % Change)
	COALESCE(
		ROUND(
			100 * (cpm - lag_cpm) :: NUMERIC / NULLIF(lag_cpm, 0) ,2
			)
			,0) AS month_prev_pct_change_CPM,
	ctr,
	-- CTR Aylık Yüzde Değişimi (CTR MoM % Change)
	COALESCE(
		ROUND(
			100 * (ctr - lag_ctr) :: NUMERIC / NULLIF(lag_ctr, 0) ,2
			)
			,0) AS month_prev_pct_change_CTR,
	romi,
	-- ROMI Aylık Yüzde Değişimi (ROMI MoM % Change)
	COALESCE(
		ROUND(
			100 * (romi - lag_romi) :: NUMERIC / NULLIF(lag_romi, 0) ,2 
			)
			,0) AS month_prev_pct_change_ROMI
FROM 
	cte_lag_data_calc
ORDER BY 
	1,2
