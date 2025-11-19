#BC2402 Group Project SQL Script File
#Seminar 4 Group 1

use bc2402_gp;

#1. [daily_intake] What are the countries with the greatest increase in carbohydrate intake over time?
select b.entity,
	round(cast(init.daily_calorie_carbohydrates as double), 2) as initial_carbs,
    round(cast(fin.daily_calorie_carbohydrates as double), 2 ) as final_carbs, 
    round(
		cast(fin.daily_calorie_carbohydrates as double) -
        cast(init.daily_calorie_carbohydrates as double), 2) as carb_increase
from 
	(select entity,
		min(cast(year as decimal)) as initial_year,
        max(cast(year as decimal)) as final_year
	from daily_intake
    group by entity) as b
join daily_intake as init
on init.entity = b.entity
	and cast(init.year as decimal) = b.initial_year
join daily_intake as fin
	on fin.entity = b.entity
    and cast(fin.year as decimal) = b.final_year
order by carb_increase desc;

/*logic: all columns are stored as text, so conversion is needed for comparisons and proper calculations; subquerty identifies the
earliest available year as well as the latest available year for each country; main table is joined twice to retrieve the carbohydrate
intake for the earliest and latest year respectively obtained from the subquery; difference (final-initial) gives the cahnge in carbohydrate 
intake over time; results are rounded to 2dp and then sorted in descending order of carb_increase*/


#2. [daily_intake] What are the average macronutrient intake for the last 10 years by country?
select a.entity,
	round(avg(cast(a.daily_calorie_animal_protein as double)),2) as avg_animal_protein,
    round(avg(cast(a.daily_calorie_vegetal_protein as double)),2) as avg_vegetal_protein,
    round(avg(cast(a.daily_calorie_fat as double)),2) as avg_fat,
    round(avg(cast(a.daily_calorie_carbohydrates as double)),2) as avg_carbs
from daily_intake a
join 
	(select entity, (max(cast(year as decimal))-9) as reference_year
	from daily_intake
	group by entity) as b
on a.entity = b.entity
where cast(a.year as decimal) >= b.reference_year
group by a.entity
order by a.entity;

/*logic: all columns are stored as text so conversion is needed for proper calculations; not all countries have data spanning
from 1961-2022, thus first identify most recent available year for each country before subtracting 9 to obtain the reference 
year that will mark the start of the last 10 years of available data for that country; filter dataset to include only records
where the year is greater than or equal to the reference year to ensure that the averaging is done over the country's latest 10
years of data; finally, calculate the average intake for each macronutrient within this 10 year window*/


#5.	[simulated_food_intake_2015_2020] List the average monthly intake by nutrient.
SELECT
CAST(Month AS UNSIGNED) AS Month, -- enforce numeric month for correct grouping/sorting
AVG(daily_calorie_animal_protein) AS avg_animal_protein, -- AVG over all rows in that month
AVG(daily_calorie_vegetal_protein) AS avg_vegetal_protein, -- same for vegetal protein
AVG(daily_calorie_fat) AS avg_fat, -- same for fat
AVG(daily_calorie_carbohydrates) AS avg_carbohydrates -- same for carbohydrates
FROM simulated_food_intake_2015_2020
GROUP BY CAST(Month AS UNSIGNED) -- group strictly by numeric month (1..12)
ORDER BY CAST(Month AS UNSIGNED) ASC; -- required sort: Month ascending 


#6.	[simulated_food_intake_2015_2020] Consider 'United States', 'India', 'Germany', 'Brazil', 'Japan'. Identify the corresponding seasonal spikes (month) in intake.
WITH base AS (
  SELECT
    Entity,
    CAST(Month AS UNSIGNED) AS Month, -- ensure numeric month
    daily_calorie_fat AS fat,
    daily_calorie_animal_protein AS animal,
    daily_calorie_vegetal_protein AS vegetal,
    daily_calorie_carbohydrates AS carbs
  FROM simulated_food_intake_2015_2020
  WHERE Entity IN ('United States','India','Germany','Brazil','Japan') -- restrict to the 5 countries
),
ranked AS (
  SELECT
    Entity,
    Month,
    ROW_NUMBER() OVER (PARTITION BY Entity ORDER BY fat DESC) AS rn_fat, -- month of max fat
    ROW_NUMBER() OVER (PARTITION BY Entity ORDER BY animal DESC) AS rn_animal, -- month of max animal protein
    ROW_NUMBER() OVER (PARTITION BY Entity ORDER BY vegetal DESC) AS rn_vegetal, -- month of max vegetal protein
    ROW_NUMBER() OVER (PARTITION BY Entity ORDER BY carbs DESC) AS rn_carb -- month of max carbs
  FROM base
)
SELECT
  Entity,
  MAX(CASE WHEN rn_fat = 1 THEN Month END) AS peak_month_fat,
  MAX(CASE WHEN rn_animal = 1 THEN Month END) AS peak_month_animal_protein,
  MAX(CASE WHEN rn_vegetal = 1 THEN Month END) AS peak_month_vegetal_protein,
  MAX(CASE WHEN rn_carb = 1 THEN Month END) AS peak_month_carbohydrates
FROM ranked
GROUP BY Entity
ORDER BY Entity ASC;


#9
#display item name, totalfat, protien and fat_to_protien_ratio as shown in the group project doc
select item, totalfat, protien, (totalfat/protien) as fat_to_protien_ratio
	from mcdonaldata
    #sort by the fat_to_protien_ratio with highest ratio appearing first
	order by fat_to_protien_ratio desc;


#10
select item, calories, totalfat, cholestrol, sodium,
	case 
    # We want to only flag the food as High Risk if it hits certain limits, and medium risk if it does not. To do this we define the positive case of one of the flags being hit
    # For every other case we just out Moderate
		when totalfat > 30 or sodium > 1000 or cholestrol > 30 then 'High Risk'
		else 'Moderate'
	end as health_flag
    from mcdonaldata
    order by health_flag asc;
    #sort by the fat_to_protien_ratio with highest ratio appearing first


# Q11. [burger_king_menu] Which Categories Are Most Weight Watchers-Friendly?

select Item from burger_king_menu; # 77 rows
select distinct(Item) from burger_king_menu; # 73 rows
# There are 4 duplicates in the burger_king_menu dataset

select *
from burger_king_menu
where Item in (
	select Item
	from burger_king_menu
	group by Item
	having count(*) > 1
)
order by Item;
# Identified the duplicated rows. Cheeseburger & Hamburger are completely duplicated. Chicken Nuggets (4pc & 6pc)
# have a copy of each but 1 category is Chicken while the other is Burgers, other nutritional value is the same.
# Action needed: Drop the duplicates for the 2 burgers & drop the duplicate for nuggets if the category is Burgers.

select distinct *
from burger_king_menu
where not (Item like '%Chicken Nuggets%' and Category = 'Burgers');
# This query does not include the duplicated rows and also the rows where Chicken Nuggets have the burgers category.

select Category,
	avg(cast(Weight_Watchers as float)) as Avg_WW_Score
from (
	select distinct *
	from burger_king_menu
	where not (Item like '%Chicken Nuggets%' and Category = 'Burgers')
) as cleaned
group by Category
order by Avg_WW_Score;
# This is the final query ordered by the average weight watcher score. Casted weight watcher score as float as its stored as text.


# Q12. [burger_king_menu] List the top 10 most caloric menu items.
select Item,
	Category,
    cast(Calories as float) as Calories
from (
	select distinct *
	from burger_king_menu
	where not (Item like '%Chicken Nuggets%' and Category = 'Burgers')
) as cleaned
order by Calories desc
limit 10;
# Casted calories as float as its stored as text. Without casting, the order by clause would sort alphabetically. We want it to sort
# numerically in descending order. No casting sort - 980, 900, 90, 1000. This is wrong if we are trying to find the top 10 caloric items.


-- q14: [starbucks] Compare Each Item to Average Calories in Its Type. Shows how far above or below the average each item is within 
-- its group.
select * from starbucks;

select 
	item,
    type,
    cast(calories as unsigned) as calories, 
    -- cast as unsigned required to convert data type to integer 
    round(avg(cast(calories as unsigned)) over (partition by type)) as avg_type_calories, 
    -- computation of average by type using window function
    round(abs(cast(calories as unsigned) - (avg(cast(calories as unsigned)) over (partition by type)))) as delta_from_avg 
    -- computation of absolute value of deviation from average calories by type
from
	starbucks
order by 
	delta_from_avg desc; -- sort by deviation to mean, descending, as stated in requirements

/* logic:
my query compares each item’s calories to the average calories of its own category (type) and ranks items by how far they deviate
from that within-type average. Because calories may be stored as text, i first convert it to a number (cast(calories as unsigned)) 
so aggregates work. i then use a window function to compute the category mean for every row: avg(cast(calories as unsigned)) 
over (partition by type) as avg_type_calories, which lets me keep one row per item rather than collapsing with group by. the 
deviation for each item is calculated as the absolute difference between its calories and its category’s 
average: abs(item_calories − avg_type_calories) and I round results for easy reading. finally, i sort the output in descending order 
using delta_from_avg, which will show the items that deviate most from their type's average. one assumption i made is that 
deviation from the mean should be positive, therefore the function abs( ) was used. */


-- q15: [starbucks] List the category-level (i.e., sandwich, bakery) Z-score for sugar.
select * from starbucks;

-- assuming Z-score for sugar is calculated using carb
-- i interpreted "category-level" z-score for sugar as sugar z-score by type (meaning each row from each type has the 
-- same output in the column)
-- z-score's formula: z-score = (type_mean - overall_mean) / overall_stddev
-- output is left as per row basis, meaning it will still show each item, type, carb but categorical z-score by type
-- *1 converts the data from text to numerical

select
	s.item,
    s.type,
    s.carb * 1 as carb,
    round(((avg(carb * 1) over (partition by type)) - overall.overall_mean) / overall.overall_sd, 3) as sugar_zscore 
    -- used window function and z-score formula above to calculate category-level z-score
from
	starbucks s
cross join (
	select
		avg(carb * 1) as overall_mean,
        stddev(carb* 1) as overall_sd
	from
		starbucks
			) as overall -- cross joined to retrieve overall_mean and overall_sd for z-score calculation
order by
	sugar_zscore asc; -- ordered by asc as specified in requirements
    
/* logic:
my query treats sugar as the carb field and computes a category-level z-score that shows how each category’s average sugar 
compares to the overall menu. i first cast carb to numeric (via * 1) so aggregates work, then use a window function avg(carb) 
over (partition by type) to get the mean carbs for each category. in a cross-joined subquery, i calculate the overall mean and 
overall standard deviation across all items once, so every row can reference them. 

the z-score is then (category_mean − overall_mean) / overall_sd, rounded to three decimals. i select item, type, and carb so the 
result stays at the item level, meaning the same category z-score repeats for all items in that category, and finally order by the 
z-score to rank categories from relatively low to high sugar */


# Q16. *Open-ended question [simulated_food_intake_2015_2020] and [happiness] Do countries with higher average protein/fat intake during winter months tend to report higher happiness?
select * from simulated_food_intake_2015_2020;
select * from happiness;
select count(distinct Entity) as no_countries from simulated_food_intake_2015_2020; -- 5 countries
select distinct Entity from simulated_food_intake_2015_2020; -- US, India, Germany, Brazil, Japan

# First we need to define what are winter months.
# Can split into Northern and Southern hemisphere.
# Winter months for Northern: Dec, Jan, Feb.
# Winter months for Southern: Jun, Jul, Aug.
# If classify this way, only Brazil falls under Southern.

-- Add hemisphere classification for each country
with food_with_hemi as (
	select *,
		if(Entity = 'Brazil', 'Southern', 'Northern') as Hemisphere -- Adds a hemisphere column
	from simulated_food_intake_2015_2020
),
-- Filter only winter months and calculate average daily intakes
food_winter as (
	select Entity,
		avg(Daily_calorie_animal_protein) as avg_winter_animal_protein,
        avg(Daily_calorie_vegetal_protein) as avg_winter_vegetal_protein,
        avg(Daily_calorie_fat) as avg_winter_fat
	from food_with_hemi
    where (hemisphere = 'Northern' and Month in (12,1,2)) -- Use Dec - Feb for Northern Hemisphere countries
		or (hemisphere = 'Southern' and Month in (6,7,8)) -- Use Jun - Aug for Southern Hemisphere countries
	group by Entity
 ),
 -- Merge winter averages with happiness data
 combined as (
	select h.Country,
		h.Happiness_Rank,
		h.Happiness_Score,
        w.avg_winter_fat,
		w.avg_winter_animal_protein,
		w.avg_winter_vegetal_protein
	from happiness h
    inner join food_winter w
		on h.Country = w.Entity
 )
 -- Final output
 select *,
	(avg_winter_animal_protein + avg_winter_vegetal_protein) as avg_winter_total_protein -- Add total protein column
 from combined
 order by Happiness_Score desc; -- Sort countries by happiness score (highest first)
 

#17. [daily_intake] and [happiness] Do long-term fat intake trends (from 1961–2020) correlate with happiness?
/*TIP: Imagine you need to prepare the data to plot a bar chart. For each country, a bar represents the level of happiness 
score, and another bar represents the level of average fat intake.*/

#order by happiness rank/score
select h.country, h.happiness_rank, h.happiness_score, t.avg_fat_intake
from happiness h 
join
	(select entity, 
		round(avg(cast(daily_calorie_fat as double)),2) as avg_fat_intake
    from daily_intake 
    group by entity) as t
on h.country = t.entity
order by cast(h.happiness_rank as decimal);

#order by avg fat intake
select h.country, h.happiness_rank, h.happiness_score, t.avg_fat_intake
from happiness h 
join
	(select entity, 
		round(avg(cast(daily_calorie_fat as double)),2) as avg_fat_intake
    from daily_intake 
    group by entity) as t
on h.country = t.entity
order by t.avg_fat_intake desc;

/*logic: all columns in both tables are all text type so conversion is required for accurate calculations; find the average
fat intake for each country/entity across all available years as not all countries have data from 1961-2020; join the result 
of the subquery with the happiness table on matching country/entity names; select country/entity, happiness rank, happiness 
score and avg fat intake from the resulting table; order the results by happiness rank/score as well as avg fat intake to
analyse the correlation between long-term fat intake and happiness

analysis: using a scatter plot, we plotted happiness score against average fat intake as well as obtained the correlation value 
between average fat intake and happiness score; analysis showed a moderately strong positive relationship (r=0.696) between average 
fat intake and happiness scores and is reflected in the upward-sloping trend of the scatterplot; countries with higher average fat 
intake typically report higher happiness levels, this pattern likely reflects broader socioeconomic factors where countries with 
greater wealth and food availability tend to consume richer diets and also score higher in well-being indicators; however, this 
relationship should be interpreted as correlation rather than causation, where higher fat intake itself does not cause happiness;
rather, both may stem from economic prosperity and quality of life*/


#18. Are countries with lower nutrient variation happier?
-- I measure variation as the standard deviation of yearly values (1961–2020 etc.)
-- across each nutrient for each country, then average those four std devs to a
-- single "variation index" and compare it with Happiness_Score.

WITH typed AS (
  SELECT
      Entity AS Country,
      CAST(Year AS UNSIGNED) AS Year, -- ensure numeric year
      CAST(daily_calorie_animal_protein  AS DECIMAL(12,6)) AS animal, -- coerce to numeric
      CAST(daily_calorie_vegetal_protein AS DECIMAL(12,6)) AS vegetal,
      CAST(daily_calorie_fat AS DECIMAL(12,6)) AS fat,
      CAST(daily_calorie_carbohydrates AS DECIMAL(12,6)) AS carb
  FROM daily_intake
),
var_by_country AS (
  SELECT
      Country,
      -- Year-to-year dispersion per nutrient
      STDDEV_SAMP(animal)  AS std_animal,
      STDDEV_SAMP(vegetal) AS std_vegetal,
      STDDEV_SAMP(fat)     AS std_fat,
      STDDEV_SAMP(carb)    AS std_carb
  FROM typed
  GROUP BY Country
),
with_index AS (
  SELECT
      Country,
      std_animal, std_vegetal, std_fat, std_carb,
      -- Simple composite index (equal weights)
      (std_animal + std_vegetal + std_fat + std_carb) / 4.0 AS avg_yearly_variation
  FROM var_by_country
)
SELECT
    wi.Country,
    wi.std_animal, wi.std_vegetal, wi.std_fat, wi.std_carb,
    wi.avg_yearly_variation,
    h.Happiness_Score
FROM with_index wi
JOIN happiness h
  ON h.Country = wi.Country
ORDER BY wi.avg_yearly_variation ASC, h.Happiness_Score DESC;


#19. Country Trends: Processed Food Intake vs. Fast Food Menu Health
#Are there any relationships between processed food intake and fast-food menu? 
#What can be the impact of processed food and fast-food consumption on happiness? 
#What about health outcomes?

#data exploration
select entity,
	round(avg(cast(daily_calorie_animal_protein as double)),2) as avg_calories_animal_protein,
    round(avg(cast(daily_calorie_vegetal_protein as double)),2) as avg_calories_vegetal_protein,
    round(avg(cast(daily_calorie_fat as double)),2) as avg_calories_fat,
    round(avg(cast(daily_calorie_carbohydrates as double)),2) as avg_calories_carbohydrates
from daily_intake
group by entity;

select entity, year, 
	round(avg(cast(daily_calorie_animal_protein as double)),2) as avg_calories_animal_protein,
    round(avg(cast(daily_calorie_vegetal_protein as double)),2) as avg_calories_vegetal_protein,
    round(avg(cast(daily_calorie_fat as double)),2) as avg_calories_fat,
    round(avg(cast(daily_calorie_carbohydrates as double)),2) as avg_calories_carbohydrates
from simulated_food_intake_2015_2020
group by entity, year;

select category, 
	round(avg(cast(calories as double)),2) as avg_calories,
	round(avg(cast(fat_g as double)),2) as avg_fat,
	round(avg(cast(total_carb_g as double)),2) as avg_carb,
	round(avg(cast(protein_g as double)),2) as avg_protein
from burger_king_menu
group by category;

select menu,
	round(avg(cast(calories as double)),2) as avg_calories,
	round(avg(cast(totalfat as double)),2) as avg_fat,
	round(avg(cast(carbs as double)),2) as avg_carb,
	round(avg(cast(protien as double)),2) as avg_protein
from mcdonaldata
group by menu;

select type,
	round(avg(cast(calories as double)),2) as avg_calorie,
	round(avg(cast(fat as double)),2) as avg_fat,
	round(avg(cast(carb as double)),2) as avg_carb,
	round(avg(cast(protein as double)),2) as avg_protein
from starbucks
group by type;

#are there any relationships between processed food intake and fast-food menu?
#finding the average macronutrients of a fast food meal across the fast food chains
select round(avg(cast(avg_calories as double)),2) as avg_calories,
	round(avg(cast(avg_fat as double)),2) as avg_fat,
    round(avg(cast(avg_carb as double)),2) as avg_carb,
    round(avg(cast(avg_protein as double)),2) as avg_protein
from
	#burger_king_menu
    #finding the average macronutrients by category 
	(select
		round(avg(cast(calories as double)),2) as avg_calories,
		round(avg(cast(fat_g as double)),2) as avg_fat,
		round(avg(cast(total_carb_g as double)),2) as avg_carb,
		round(avg(cast(protein_g as double)),2) as avg_protein
	from burger_king_menu
	group by category

	union all

	#mcdonaldata
    #finding the average macronutrients by menu
	select 
		round(avg(cast(calories as double)),2) as avg_calories,
		round(avg(cast(totalfat as double)),2) as avg_fat,
		round(avg(cast(carbs as double)),2) as avg_carb,
		round(avg(cast(protien as double)),2) as avg_protein
	from mcdonaldata
	where menu = "breakfast" or menu = "regular" or menu = "gourmet"  #only regarding items considered as meals
	group by menu

	union all

	#starbucks
    #finding the average macronutrients by type
	select 
		round(avg(cast(calories as double)),2) as avg_calorie,
		round(avg(cast(fat as double)),2) as avg_fat,
		round(avg(cast(carb as double)),2) as avg_carb,
		round(avg(cast(protein as double)),2) as avg_protein
	from starbucks
	where type = "bakery" or type = "bistro box" or type = "hot breakfast" or type = "sandwich"  #only regarding types considered as meals
	group by type) as temp;

/*findings from query: the average fast food meal across the fast food chains is calorie dense, with carbohydrates comprising the largest
proportion of the macronutrients, followed by fat; protein content is the lowest among the 3 macronutrients identified

next step: to explore potential relationships between processed food intake and fast food menu, I will identify countries with high processed
food intake using external data and analyse whetehr the macronutrien proportions align with those observed in an average fast-food meals*/

#source: https://www.bmj.com/content/383/bmj-2023-075294
#using external data, united states and united kingdom have the highest process food intake with 58% and 57% of the adult diet respectively
#finding the average macronutrient intake for united states and united kingdom
select entity,
	round(avg(cast(daily_calorie_carbohydrates as double)),2) as avg_calorie_carb,
    round(avg(cast(daily_calorie_fat as double)),2) as avg_calorie_fat,
    round(avg(cast(daily_calorie_animal_protein as double)),2) as avg_calorie_animal_protein
from daily_intake
where entity = "united states" or entity = "united kingdom" 
group by entity;

/*from the query above, the average calorie intake from carbohydrates is the highest, followed by calories from fat, with calories from animal 
protein being the lowest; this distribution aligns with the findings from the average marconutrient proportions in fast food meals across the 
fast food chains

hence, there appears to be a relationship between processed food intake and fast food menu, specifically there seems to be a positive relationship
between processed food intake and the macronutrient composition of fast food menu; the findings above suggest the countries with higher processed food
intake tend to have diets that mirror the macronutrient composition seen in fast food meals, which are higher carbohydrates and fat intake coupled with 
lower protein intake

overall, the findings suggest a positive relationship where higher processed food consumption is associated with a similar pattern of macronutient 
distribution in fast food meals*/

#What can be the impact of processed food and fast-food consumption on happiness? 
#source: https://www.bmj.com/content/383/bmj-2023-075294
#countries with highest processed/fast food intake: united states, united kingdom, canada, sweden, australia
#countries with low processed/fast food intake: romania, colombia, hungary, italy, estonia

#query for countries with highest intake
select avg(cast(happiness_rank as double)) as avg_rank,
	avg(cast(happiness_score as double)) as avg_score
from(
	select country, happiness_rank, happiness_score
	from happiness
	where country = "united states" or
		country = "united kingdom" or
		country = "canada" or
		country = "sweden" or
		country = "australia") as temp
        
union all 

#query for countries with low intake
select avg(cast(happiness_rank as double)) as avg_rank,
	avg(cast(happiness_score as double)) as avg_score
from(
	select country, happiness_rank, happiness_score
	from happiness
	where country = "romania" or 
		country = "colombia" or 
		country = "hungary" or 
		country = "italy" or
		country = "estonia") as temp;

/*from the queries above, it is observed that countries with higher processed and fast food intake tend to hold higher ranks on the happiness scale and
have higher happiness scores compared to countries with lower processed/fast food consumption; after averaging the happiness ranks and scores for each group,
countires with high processed/fast food intake have an average happiness rank of 11.8 and a happiness score of 7.21, which is significantly higher than the 
69.2 and 5.56 score for countries with low processed/fast food intake

this suggests that processed food and fast food consumption may have a positive impact on happiness; while other factors certainly contribute to happiness,
the findings imply that countries with higher consumption of processed and fast food benefit from greater overall happiness*/

#What about health outcomes?
#source: https://www.bmj.com/content/383/bmj-2023-075294
#countries with highest processed/fast food intake: united states, united kingdom, canada, sweden, australia
#countries with low processed/fast food intake: romania, colombia, hungary, italy, estonia

#query for countries with highest intake
select avg(cast(health_life_expectancy as double)) as avg_life_expectancy,
	avg(cast(economy_gdp_per_capita as double)) as avg_gdp_per_capita,
    avg(cast(family as double)) as avg_family,
    avg(cast(freedom as double)) as avg_freedom
from(
	select country, health_life_expectancy, economy_gdp_per_capita, family, freedom
	from happiness
	where country = "united states" or
		country = "united kingdom" or
		country = "canada" or
		country = "sweden" or
		country = "australia") as temp
        
union all

#query for countries with low intake
select avg(cast(health_life_expectancy as double)) as avg_life_expectancy,
	avg(cast(economy_gdp_per_capita as double)) as avg_gdp_per_capita,
    avg(cast(family as double)) as avg_family,
    avg(cast(freedom as double)) as avg_freedom
from(
	select country, health_life_expectancy, economy_gdp_per_capita, family, freedom
	from happiness
	where country = "romania" or 
		country = "colombia" or 
		country = "hungary" or 
		country = "italy" or
		country = "estonia") as temp;

/*findings: countries with higher processed/fast food intake have a higher average life expectancy (0.90) as compared to countries with lower intake (0.78); 
countries with higher intake have a higher average gdp per capita (1.33) as compared to countries with lower intake (1.09); countries with higher intake have a higher
average family score (1.29) as compared to countries with lower intake (1.15); countries with higher intake have a higher average freedom score (0.61) as compared to
countries with lower intake (0.38)

conclusion: although fast food and processed food in general are often associated with negative health reprecussions, countries with high intake actually have a higher 
average life expectancy as compared to those with lower intakes, possibly suggesting that perhaps the impact of fast food may not be as drastic as commonly believed

however, the difference in average life expectancy could also be justified by other factors; for instance, countries with higher processed food intake also tend to have a 
higher gdp per capita on average, which may translate into better access to healthcare facilities and lifestyle amenities that help offset the negative effects of fast food, 
ultimately contributing to an overall net benefit on health; additionally, factors such as family support and freedom may also play a role in enhancing overall well-being and
health outcomes, further epxlaining the higher average life expectancy observed in countries with higher intake*/


#20. Does fast-food consumption increase health risk? Could the risk be mitigated?
/* -----------------------------------------------------------
   Reproduce baseline + mitigated risk table using ONLY
   burger_king_menu, WITHOUT percentile_cont
------------------------------------------------------------- */

WITH base AS (
    /* Convert nutrients to per-100kcal so items are comparable */
    SELECT
        Item,
        Category,
        Sodium_mg * 100.0 / Calories  AS mgNa_per_100kcal,
        Saturated_Fat_g * 100.0 / Calories AS gFat_per_100kcal
    FROM burger_king_menu
),

/* ---------------------------  
   1. BASELINE RISK TAGGING  
   --------------------------- */
baseline AS (
    SELECT
        'baseline' AS scenario,
        Item,
        Category,

        /* Hardcoded cutoffs (from original analysis) */
        CASE WHEN mgNa_per_100kcal >= 180 THEN 1 ELSE 0 END AS high_na,
        CASE WHEN gFat_per_100kcal >=   6 THEN 1 ELSE 0 END AS high_fat
    FROM base
),

baseline_scored AS (
    SELECT
        scenario,
        Item,
        Category,
        (high_na + high_fat) AS risk_score
    FROM baseline
),

/* ---------------------------  
   2. MITIGATED VERSION  
   Reduce nutrients by 10% if item was high-risk  
   --------------------------- */
mitigated_calc AS (
    SELECT
        Item,
        Category,

        /* Reformulation reduces values ONLY for originally high items */
        CASE
            WHEN mgNa_per_100kcal >= 180 THEN mgNa_per_100kcal * 0.9
            ELSE mgNa_per_100kcal
        END AS mgNa_per_100kcal_new,

        CASE
            WHEN gFat_per_100kcal >= 6 THEN gFat_per_100kcal * 0.9
            ELSE gFat_per_100kcal
        END AS gFat_per_100kcal_new
    FROM base
),

/* Re-score mitigated items using SAME thresholds */
mitigated_scored AS (
    SELECT
        'mitigated' AS scenario,
        Item,
        Category,
        (CASE WHEN mgNa_per_100kcal_new >= 180 THEN 1 ELSE 0 END +
         CASE WHEN gFat_per_100kcal_new >=   6 THEN 1 ELSE 0 END) AS risk_score
    FROM mitigated_calc
),

/* ---------------------------  
   Combine baseline + mitigated  
   --------------------------- */
combined AS (
    SELECT * FROM baseline_scored
    UNION ALL
    SELECT * FROM mitigated_scored
),

/* Count risk levels */
counts AS (
    SELECT
        scenario,
        risk_score,
        COUNT(*) AS item_count
    FROM combined
    GROUP BY scenario, risk_score
),

totals AS (
    SELECT scenario, SUM(item_count) AS total_items
    FROM counts
    GROUP BY scenario
)

/* Final output */
SELECT
    c.scenario,
    c.risk_score,
    c.item_count,
    ROUND(c.item_count * 100.0 / t.total_items, 2) AS pct_items
FROM counts c
JOIN totals t ON c.scenario = t.scenario
ORDER BY
    FIELD(c.scenario, 'baseline', 'mitigated'),
    c.risk_score DESC;


#21
select 
h.country, h.Economy_GDP_per_Capita, Health_Life_Expectancy,  h.Family, t.avg_fat_intake, t.avg_animal_protein_intake, t.avg_vegetal_protein_intake, t.avg_carbohydrate_intake, round((avg_fat_intake + avg_animal_protein_intake + avg_vegetal_protein_intake + avg_carbohydrate_intake),2) as avg_total_calorie_intake
from happiness h 
join
  (select entity, round(avg(cast(daily_calorie_fat as double)),2) as avg_fat_intake, 
  round(avg(cast(daily_calorie_animal_protein as double)),2) as avg_animal_protein_intake, 
  round(avg(cast(daily_calorie_vegetal_protein as double)),2) as avg_vegetal_protein_intake,
  round(avg(cast(daily_calorie_carbohydrates as double)),2) as avg_carbohydrate_intake
    from daily_intake 
    group by entity) as t
on h.country = t.entity
order by avg_total_calorie_intake desc;
# After mapping creating a correlation matrix in python using this data, I found that there is a correlation coefficient of 0.821139 for animal protein intake and GDP per capita
# There is also a correlation coefficient of 0.804815 between fat intake and GDP per capita, I want to run a secondary analysis on this using yearly GDP per capita obtained from external sources
# I have found and decided to use the {Global GDP-PIB per Capita Dataset (1960-present)} data from kaggle, link is: https://www.kaggle.com/datasets/fredericksalazar/global-gdp-pib-per-capita-dataset-1960-present

select 
    Year,
    entity,
    Daily_calorie_animal_protein,
    Daily_calorie_vegetal_protein,
    Daily_calorie_fat,
    Daily_calorie_carbohydrates,
    (Daily_calorie_animal_protein + Daily_calorie_vegetal_protein + Daily_calorie_fat + Daily_calorie_carbohydrates) AS total_daily_calories
from daily_intake
where entity = 'United States'
order by Year desc;
#Preparing the dataset for comparison with the external, yearly global GDP dataset, will export as CSV and plot using pyplot, compare correlation coefficients to isolate trends exclusive to the USA, since that was the assignment


# Q22. *Blue-sky question What months should governments increase public awareness of unhealthy food spikes?
# For example, are there healthy fast-food options that can be promoted via public campaigns?
# What healthy fast-food options can be introduced? What makes these options suitable?

select * from mcdonaldata;
select * from simulated_food_intake_2015_2020;

-- Identify unhealthy food spike months
with calorie_composition as (
    select 
        Month,
        -- Calculate total average calorie intake for each month
        avg(Daily_calorie_animal_protein + Daily_calorie_vegetal_protein + Daily_calorie_fat + Daily_calorie_carbohydrates) as avg_total_calories,
        -- Average breakdown of calorie sources
        avg(Daily_calorie_animal_protein) as avg_animal_protein,
        avg(Daily_calorie_vegetal_protein) as avg_vegetal_protein,
        avg(Daily_calorie_fat) as avg_fat,
        avg(Daily_calorie_carbohydrates) as avg_carbs
    from simulated_food_intake_2015_2020
    group by Month
)
select *
from calorie_composition
order by avg_total_calories desc; -- Highest average calorie months appear first
-- Feb to Apr top 3 (Spring)

-- Identify healthier fast-food options to promote
-- Beverage
select *
from mcdonaldata
where menu in ('mccafe', 'beverage') -- Only drink categories
	and sugar = 0 -- Zero sugar drinks
order by calories desc; -- Higher-calorie zero-sugar drinks appear first
-- Water, Coke Zero

-- Main Meal
select *
from mcdonaldata
where menu in ('regular', 'breakfast', 'gourmet') -- Only main meal categories
    and calories <= 400  -- Reasonable calorie cap
    and cast(protien as float) > 15  -- Good protein content
    and cast(totalfat as float) < 20   -- Lower fat
    and cast(sodium as float) < 1000  -- Not too salty
    and cast(transfat as double) < 10 -- There was a value with 75, I believe its an error as transfat wouldnt reach that high
    -- Exclude deep-fried and unhealthier sides
    and item not like '%Nugget%'
    and item not like '%Fries%'
    and item not like '%Wedges%'
    and item not like '%Hash Brown%'
    and item not like '%Fried%'
order by menu, calories; -- Sort by menu category then calories
-- 2 breakfast, 2 regulars


-- q23. What is the impact of reduction of fat or sugar-based diets on happiness, health, and economic outcomes? 
-- Should countries explicitly regulate sugar consumption (e.g., mandatory reduced-sugar beverage, sugar tax)? 
-- Or, should countries employ implicit policies (e.g., awareness promotions, provision of healthy options)?

-- query 1: average fat intake vs happiness score by country
select 
    h.country,
    h.region,
    cast(h.happiness_score AS decimal(10,3)) as happiness_score,
    cast(h.economy_gdp_per_capita AS decimal(10,3)) AS gdp_per_capita,
    cast(h.health_life_expectancy AS decimal(10,3)) AS life_expectancy,
    round(avg(cast(d.daily_calorie_fat AS decimal(10,2))), 2) AS avg_fat_intake,
    round(avg(cast(d.daily_calorie_carbohydrates AS decimal(10,2))), 2) AS avg_carb_intake
from happiness h
left join
	daily_intake d on h.country = d.entity
where
	d.year >= '2010' and d.year <= '2020'
    and h.happiness_Score is not null
    and h.happiness_Score != ''
group by
	h.Country, h.Region, h.Happiness_Score, h.Economy_GDP_per_Capita, h.Health_Life_Expectancy
order by
	cast(h.happiness_Score as decimal(10,3)) desc;

/* logic: i used a left join between the happiness and daily_intake tables on country to combine well-being data with dietary intake. 
i used cast to convert happiness, gdp per capita and life expectancy into decimal values, and avg plus round to compute the average 
fat and carbohydrate calories for each country from 2010–2020. i then used group by on country, region and the happiness fields so 
that each row represents a single country, and order by happiness score (descending) to see which happier countries correspond to 
higher or lower fat intake. */

-- query 2: categorise countries by fat intake level and comprae happiness
with fat_categories as (
    select
        h.country,
        cast(h.happiness_score as decimal(10,3)) as happiness_score,
        cast(h.economy_gdp_per_capita as decimal(10,3)) as gdp_per_capita,
        round(avg(cast(d.daily_calorie_fat as decimal(10,2))), 2) as avg_fat_intake,
        case
            when avg(cast(d.daily_calorie_fat as decimal(10,2))) < 600 then 'low fat'
            when avg(cast(d.daily_calorie_fat as decimal(10,2))) between 600 and 800 then 'medium fat'
            else 'high fat'
        end as fat_category
    from happiness h
    left join 
		daily_intake d on h.country = d.entity
    where 
		d.year >= '2010' and d.year <= '2020'
        and h.happiness_score is not null
        and h.happiness_score != ''
    group by 
		h.country, h.happiness_score, h.economy_gdp_per_capita
)
select
    fat_category,
    count(*) as number_of_countries,
    round(avg(happiness_score), 3) as avg_happiness_score,
    round(avg(gdp_per_capita), 3) as avg_gdp_per_capita,
    round(min(avg_fat_intake), 2) as min_fat_intake,
    round(max(avg_fat_intake), 2) as max_fat_intake,
    round(avg(avg_fat_intake), 2) as avg_fat_intake
from fat_categories
group by 
	fat_category
order by 
	avg_happiness_score desc;

/* logic: i used a cte (fat_categories) to first calculate each country’s average fat intake using avg, cast, and round. 
inside the cte, i used a case expression to label each country as ‘low fat’, ‘medium fat’, or ‘high fat’ based on its 
average fat calories. after that, i used count, min, max, and avg to summarise happiness, gdp per capita and fat intake 
by fat category, and group by the category to compare outcomes across these groups. i finished with order by average happiness 
score to see which fat-intake group tends to be happiest.*/

-- query 3: analyse sugar from fast food menus
select
    'mcdonald' as restaurant,
    count(*) as total_items,
    round(avg(cast(sugar as decimal(10,2))), 2) as avg_sugar_g,
    round(max(cast(sugar as decimal(10,2))), 2) as max_sugar_g,
    round(min(cast(sugar as decimal(10,2))), 2) as min_sugar_g,
    round(stddev(cast(sugar as decimal(10,2))), 2) as stddev_sugar,
    count(case when cast(sugar as decimal(10,2)) > 50 then 1 end) as high_sugar_items
from mcdonaldata
where sugar is not null and sugar != '' and cast(sugar as decimal(10,2)) > 0

union all

select
    'burger king' as restaurant,
    count(*) as total_items,
    round(avg(cast(sugars_g as decimal(10,2))), 2) as avg_sugar_g,
    round(max(cast(sugars_g as decimal(10,2))), 2) as max_sugar_g,
    round(min(cast(sugars_g as decimal(10,2))), 2) as min_sugar_g,
    round(stddev(cast(sugars_g as decimal(10,2))), 2) as stddev_sugar,
    count(case when cast(sugars_g as decimal(10,2)) > 50 then 1 end) as high_sugar_items
from burger_king_menu
where 
	sugars_g is not null and sugars_g != '' 
    and cast(sugars_g as decimal(10,2)) > 0;
    
/* logic: i used two select statements (one for mcdonald’s and one for burger king) combined with union all to compare 
sugar levels across both restaurants. for each, i used count to get the total number of menu items, and avg, min, max, 
stddev, and round (after cast to decimal) to describe the sugar distribution. i used a case expression inside count to 
calculate how many items exceed 50g of sugar as high_sugar_items. i also applied a where filter to remove null, empty and zero 
sugar values so the summary reflects only valid menu items.*/

-- query 4: gdp vs dietary patterns
with gdp_categories as (
    select
        country,
        cast(economy_gdp_per_capita as decimal(10,3)) as gdp_per_capita,
        case
            when cast(economy_gdp_per_capita as decimal(10,3)) > 1.2 then 'high gdp'
            when cast(economy_gdp_per_capita as decimal(10,3)) between 0.6 and 1.2 then 'medium gdp'
            else 'low gdp'
        end as gdp_category,
        cast(happiness_score as decimal(10,3)) as happiness_score,
        cast(health_life_expectancy as decimal(10,3)) as life_expectancy
    from happiness
    where 
		economy_gdp_per_capita is not null and economy_gdp_per_capita != ''
)
select
    gc.gdp_category,
    count(distinct gc.country) as number_of_countries,
    round(avg(gc.gdp_per_capita), 3) as avg_gdp,
    round(avg(gc.happiness_score), 3) as avg_happiness,
    round(avg(gc.life_expectancy), 3) as avg_life_expectancy,
    round(avg(cast(d.daily_calorie_fat as decimal(10,2))), 2) as avg_fat_intake
from gdp_categories gc
left join 
	daily_intake d on gc.country = d.entity
where 
	d.year >= '2010' and d.year <= '2020'
group by 
	gc.gdp_category
order by 
	avg_gdp desc;
    
/* logic: i used a cte (gdp_categories) to classify countries into ‘high gdp’, ‘medium gdp’, and ‘low gdp’ using a case 
expression on economy_gdp_per_capita after converting it with cast. in the same cte, i casted happiness and life expectancy 
to decimals so they could be aggregated cleanly. i then joined this cte to daily_intake using a left join and used avg plus 
round to compute average gdp, happiness, life expectancy, and fat intake for each gdp category. i used group by gdp category 
and order by average gdp to observe how dietary fat patterns and well-being vary across income levels. */

-- query 5: find out which country needs help the most
select
    h.country,
    h.region,
    cast(h.happiness_score as decimal(10,3)) as happiness_score,
    cast(h.economy_gdp_per_capita as decimal(10,3)) as gdp_per_capita,
    round(avg(cast(d.daily_calorie_fat as decimal(10,2))), 2) as avg_fat_intake,
    case
        when avg(cast(d.daily_calorie_fat as decimal(10,2))) > 800
             and cast(h.happiness_score as decimal(10,3)) < 5.0
        then 'priority for intervention'
        when avg(cast(d.daily_calorie_fat as decimal(10,2))) > 700
             and cast(h.happiness_score as decimal(10,3)) < 6.0
        then 'consider intervention'
        else 'monitor'
    end as intervention_priority
from happiness h
left join daily_intake d on h.country = d.entity
where d.year >= '2010' and d.year <= '2020'
    and h.happiness_score is not null
    and h.happiness_score != ''
group by h.country, h.region, h.happiness_score, h.economy_gdp_per_capita
having avg(cast(d.daily_calorie_fat as decimal(10,2))) > 600
order by intervention_priority, cast(h.happiness_score as decimal(10,3)) asc;

/* logic: i used a left join between happiness and daily_intake to combine happiness, economic, and dietary 
data from 2010–2020. i used avg (with cast and round) to compute each country’s average fat intake. then, i applied a 
case expression to classify each country into an intervention_priority category (‘priority for intervention’, ‘consider 
intervention’, or ‘monitor’) based on high fat intake thresholds paired with low happiness score conditions. i used group by 
country, region, happiness, and gdp to aggregate values properly, and a having clause to filter out countries with average fat 
intake of 600 calories or less. finally, i used order by intervention priority and happiness score (ascending) so that the 
countries requiring the most attention appear first. */






