
/*
 * 1. Rostou v prùbìhu let mzdy ve všech odvìtvích, nebo v nìkterých klesají?
 */
USE `engeto-2022-10`;

WITH cte_prev_year_payroll AS 
	(
	SELECT 
		payroll_year,
		avgvalue_payroll,
		industry_name,
		lag(avgvalue_payroll) OVER (PARTITION BY industry_name ORDER BY payroll_year) AS prev_year_payroll
	FROM t_veronika_dolezalova_project_SQL_primary_final
	ORDER BY industry_name, payroll_year 
	)
SELECT 
	*,
	CASE WHEN avgvalue_payroll > prev_year_payroll THEN 0
	ELSE 1
	END AS flag_is_decrease
FROM cte_prev_year_payroll
WHERE prev_year_payroll IS NOT NULL
;


/*
 * 2. Kolik je možné si koupit litrù mléka a kilogramù chleba za první a poslední srovnatelné období v dostupných datech cen a mezd?
*/

WITH cte_payroll AS
(
SELECT 
	round(avg(avgvalue_payroll),0) AS avg_payroll,
	payroll_year 
FROM t_veronika_dolezalova_project_sql_primary_final
GROUP BY payroll_year
)
SELECT 
	groceries.price_year,
 	groceries.name,
 	groceries.round_price,
 	groceries.price_unit,
 	cte_payroll.avg_payroll,
 	round (cte_payroll.avg_payroll/groceries.round_price, 0) AS sum_to_buy,
 	concat(groceries.price_unit,' / CZK') AS unit 
FROM t_veronika_dolezalova_project_SQL_secondary_final AS groceries
JOIN 
	(
	SELECT 
	min(price_year) AS min_year,
 	max(price_year) AS max_year
	FROM t_veronika_dolezalova_project_sql_secondary_final 
	) AS min_max_year
ON min_max_year.min_year = groceries.price_year OR min_max_year.max_year = groceries.price_year
JOIN cte_payroll ON cte_payroll.payroll_year = groceries.price_year 
WHERE name IN ('Mléko polotuèné pasterované','Chléb konzumní kmínový')
;

/*
 * 3. Která kategorie potravin zdražuje nejpomaleji (je u ní nejnižší percentuální meziroèní nárùst)?
*/
WITH cte_prices AS 
	(
	SELECT 
		groceries.price_year,
 		groceries.name,
 		groceries.round_price,
 		groceries.price_unit,
 		concat(groceries.price_unit,' / CZK') AS unit,
 		lag(round_price) OVER (PARTITION BY name ORDER BY price_year) AS prev_year_price
	FROM t_veronika_dolezalova_project_SQL_secondary_final AS groceries
	)
	 ,
cte_percent_rise AS 
	 (
	SELECT *, round((((round_price-prev_year_price)/prev_year_price)*100),2) AS percent_rise	
	FROM cte_prices
	)
	,
cte_avg_percent_rise AS
	(
	SELECT 
		name, 
		avg(percent_rise) AS avg_percent_rise
	FROM cte_percent_rise
	GROUP BY name
)
SELECT 
	name, 
	avg_percent_rise
FROM cte_avg_percent_rise
WHERE avg_percent_rise = (
	SELECT min(avg_percent_rise)
	FROM cte_avg_percent_rise)
;


/*
 * 4. Existuje rok, ve kterém byl meziroèní nárùst cen potravin výraznì vyšší než rùst mezd (vìtší než 10 %)?
 */

WITH cte_prev_year_price AS 
	(
	SELECT 
		price_year,
		round(avg(round_price),2) AS average_price,
		lag(round(avg(round_price),2)) OVER (ORDER BY price_year) AS prev_year_price
	FROM t_veronika_dolezalova_project_sql_secondary_final  
	GROUP BY price_year 
	)
,
cte_percent_rise_price AS 
	(
	SELECT 
		*, 
		round((((average_price-prev_year_price)/prev_year_price)*100),2) AS percent_rise_price
	FROM cte_prev_year_price
	)
,
	cte_avg_payroll AS 
	(
	SELECT 
		payroll_year,
		round(avg(avgvalue_payroll),2) AS avg_payroll,
		lag(round(avg(avgvalue_payroll),2)) OVER (ORDER BY payroll_year) AS prev_year_payroll
	FROM t_veronika_dolezalova_project_sql_primary_final 
	GROUP BY payroll_year 
	)
,
	cte_percent_rise_payroll AS 
	(
	SELECT 
		*,
		round((((avg_payroll-prev_year_payroll)/prev_year_payroll)*100),2) AS percent_rise_payroll
	FROM cte_avg_payroll
	)
SELECT 
	payroll_year,
	percent_rise_payroll,
	percent_rise_price,
	(percent_rise_price - percent_rise_payroll) AS difference
FROM cte_percent_rise_payroll
JOIN cte_percent_rise_price ON price_year = payroll_year
WHERE percent_rise_price IS NOT NULL 
;


/*
 * 5. Má výška HDP vliv na zmìny ve mzdách a cenách potravin? Neboli, pokud HDP vzroste výraznìji v jednom roce, 
 * projeví se to na cenách potravin èi mzdách ve stejném nebo násdujícím roce výraznìjším rùstem?
 */


WITH cte_prev_year_price AS 
	(
	SELECT 
		price_year,
		round(avg(round_price),2) AS average_price,
		lag(round(avg(round_price),2)) OVER (ORDER BY price_year) AS prev_year_price
	FROM t_veronika_dolezalova_project_sql_secondary_final  
	GROUP BY price_year 
	)
,
cte_percent_rise_price AS 
	(
	SELECT 
		*, 
		round((((average_price-prev_year_price)/prev_year_price)*100),2) AS percent_rise_price
	FROM cte_prev_year_price
	)
,
	cte_avg_payroll AS 
	(
	SELECT 
		payroll_year,
		round(avg(avgvalue_payroll),2) AS avg_payroll,
		lag(round(avg(avgvalue_payroll),2)) OVER (ORDER BY payroll_year) AS prev_year_payroll,
		GDP_cz_yearly,
		lag(GDP_cz_yearly) OVER (ORDER BY payroll_year) AS prev_year_GDP
	FROM t_veronika_dolezalova_project_sql_primary_final 
	GROUP BY payroll_year 
	)
,
	cte_percent_rise_payroll AS 
	(
	SELECT 
		*,
		round((((avg_payroll-prev_year_payroll)/prev_year_payroll)*100),2) AS percent_rise_payroll,
		round((((GDP_cz_yearly - prev_year_GDP)/prev_year_GDP)*100),2) AS percent_rise_GDP
	FROM cte_avg_payroll
	)
SELECT 
	payroll_year,
	percent_rise_payroll,
	percent_rise_price,
	percent_rise_GDP
FROM cte_percent_rise_payroll
JOIN cte_percent_rise_price ON price_year = payroll_year
WHERE percent_rise_price IS NOT NULL 
;