
/*
 * 1. Rostou v pr�b�hu let mzdy ve v�ech odv�tv�ch, nebo v n�kter�ch klesaj�?
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
 * 2. Kolik je mo�n� si koupit litr� ml�ka a kilogram� chleba za prvn� a posledn� srovnateln� obdob� v dostupn�ch datech cen a mezd?
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
WHERE name IN ('Ml�ko polotu�n� pasterovan�','Chl�b konzumn� km�nov�')
;

/*
 * 3. Kter� kategorie potravin zdra�uje nejpomaleji (je u n� nejni��� percentu�ln� meziro�n� n�r�st)?
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
 * 4. Existuje rok, ve kter�m byl meziro�n� n�r�st cen potravin v�razn� vy��� ne� r�st mezd (v�t�� ne� 10 %)?
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
 * 5. M� v��ka HDP vliv na zm�ny ve mzd�ch a cen�ch potravin? Neboli, pokud HDP vzroste v�razn�ji v jednom roce, 
 * projev� se to na cen�ch potravin �i mzd�ch ve stejn�m nebo n�sduj�c�m roce v�razn�j��m r�stem?
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