
-- Výchozí tabulka 1
CREATE TABLE t_veronika_dolezalova_project_SQL_primary_final as
SELECT 
	cp.payroll_year, 
	round(avg(cp.value),0) AS avgvalue_payroll,
	cpib.name AS industry_name,
	economies.GDP AS GDP_cz_yearly
FROM `engeto-2022-10`.czechia_payroll AS cp
JOIN czechia_payroll_industry_branch AS cpib ON cp.industry_branch_code = cpib.code 
JOIN economies ON cp.payroll_year = economies.`year` 
WHERE industry_branch_code IS NOT NULL
AND cp.value_type_code = '5958'
AND economies.country = 'Czech Republic'
GROUP BY payroll_year, industry_branch_code, value_type_code
;

SELECT *
FROM t_veronika_dolezalova_project_SQL_primary_final;

-- Výchozí tabulka 2
CREATE TABLE t_veronika_dolezalova_project_SQL_secondary_final as
SELECT 
	YEAR(cpr.date_from) AS price_year,
	cat.name,
	round(avg(cpr.value),0) AS round_price,
	cat.price_unit
FROM czechia_price AS cpr
	JOIN czechia_price_category AS cat
	ON  cpr.category_code = cat.code 
GROUP BY YEAR(cpr.date_from),cat.name, cat.price_unit;

SELECT *
FROM t_veronika_dolezalova_project_SQL_secondary_final;

-- Výchozí tablka 3
CREATE TABLE t_veronika_dolezalova_project_SQL_tertiary_final as
SELECT 
	economies.`year`,
	economies.country,
	economies.GDP,
	economies.population,
	economies.gini
FROM economies 
JOIN countries ON economies.country = countries.country
JOIN 
	(
	SELECT DISTINCT payroll_year
	FROM czechia_payroll) AS cp
ON cp.payroll_year = economies.`year` 	
WHERE countries.continent = 'Europe';

SELECT *
FROM t_veronika_dolezalova_project_SQL_tertiary_final;

