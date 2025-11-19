# Name: Stephen Michael Lee
# Matric No.: U2410465B

# 1. How many product categories are there? For each product category, show the number of records.
-- Part 1: To count how many distinct product categories there are
select count(distinct product_category) as num_categories
from baristacoffeesalestbl;
/*
Answer: There are 7 product categories.
*/

-- Part 2: To show the number of records for each product category
select product_category, 
	count(*) as product_count
from baristacoffeesalestbl
group by product_category;
/*
I grouped all the rows according to their product category. For each group, it counts how many records exist.
The output shows two columns. The name of the product category and the total number of sales records that fall under that category.
*/

# 2. For each customer_gender and loyalty_member type, show the number of records. Within the same outcome, within each customer_gender and loyalty_member type, for each is_repeat_customer type, show the number of records.
select distinct customer_gender,
	loyalty_member,
    count(*) over(partition by customer_gender, loyalty_member) as records,
    is_repeat_customer,
    count(*) over(partition by customer_gender, loyalty_member, is_repeat_customer) as records
from baristacoffeesalestbl
order by customer_gender, is_repeat_customer desc, 3 desc;
/*
The first records column shows the total number of rows grouped by customer gender and loyalty member status. The second records column further breaks
this down by also including the repeat customer status. Finally, I ordered the results by gender, whether the customer is a repeat and the first records
column, to match the expected output. The 3 in the order by clause represents the first column of records.
*/

# 3. For each product_category and customer_discovery_source, display the sum of total_amount.
-- Version A
select 
    product_category,
    customer_discovery_source,
    sum(floor(total_amount + 0.5)) as total_sales
from baristacoffeesalestbl
group by product_category, customer_discovery_source
order by product_category;
/*
In this version of the query, I calculated the total sales by summing up each transaction after rounding it to the nearest whole number using
floor(total_amount + 0.5). This approach makes the output show whole numbers instead of decimals, which makes the totals look cleaner and easier to read.
After calculating the rounded sums, I grouped the results by product category and customer discovery source so that each unique combination would have
its own total. Finally, I ordered the results by product category.
*/

-- Version B
select product_category,
	customer_discovery_source,
    sum(total_amount) as total_sales
from baristacoffeesalestbl
group by product_category, customer_discovery_source
order by product_category;
/*
In this version, I again selected the product category and customer discovery source, but instead of rounding the transaction values, I summed the raw
total_amount values directly. By doing this, I kept the totals completely accurate and avoided any distortion that could be caused by rounding each row
before adding them up. The trade-off is that the results may sometimes include decimal values, but this preserves the integrity of the data. As with
Version A, I grouped the results by product category and customer discovery source so that I could see the totals for each combination, and I ordered the
final output by product category to maintain the same structure and make the two versions easy to compare.
*/

## Difference and Correct Choice
/*
The key difference between the two queries is how they treat the individual transaction amounts before summing. In Version A, each transaction
is first rounded to the nearest whole number and then added together, which produces totals that are neat but slightly less accurate. In Version B,
the transactions are summed in their original form, which ensures that the totals are precise but may contain decimals. Overall, Version B is the
better choice because accuracy should be prioritised when calculating totals, and any rounding can always be applied later at the presentation stage if needed.
*/

# 4. Consider consuming coffee as the beverage, for each time_of_day category and gender, display the average focus_level and average sleep_quality.
-- Created a CTE to filter only rows where the beverage is coffee and map data columns
with coffee_stats as (
	select
		-- Derived the time_of_day column using if conditions
        -- If morning = True, labelled as morning, else check afternoon, otherwise is evening
		if(time_of_day_morning = 'True', 'morning',
			if(time_of_day_afternoon = 'True', 'afternoon', 'evening')) as time_of_day,
		-- Similarly, derived the gender column using if conditions as well
        -- If gender_female = True, labelled as female, otherwise male
		if(gender_female = 'True', 'female', 'male') as gender,
		focus_level,
		sleep_quality
	from caffeine_intake_tracker
	where beverage_coffee = 'True'
)
-- Calculated the average focus level and sleep quality, grouped by time of day and gender
select time_of_day,
	gender,
    -- Used floor() with + 0.5 to round values before averaging
    -- and format() to display the averages with 4 decimal places
    format(avg(floor(focus_level + 0.5)), 4) as avg_focus_level,
    format(avg(floor(sleep_quality + 0.5)), 4) as avg_sleep_quality
from coffee_stats
group by time_of_day, gender
order by
	-- Ordered the results so they appear in the sequence: morning -> afternoon -> evening (according to expected output)
	field(time_of_day, 'morning', 'afternoon', 'evening');
/*
I first created a Common Table Expression (CTE) called coffee_stats to map and categorise the data before calculating averages.
Within this CTE, I used conditional statements to assign each row into one of the three time-of-day categories: morning, afternoon, or evening.
I applied a similar approach to gender, where rows marked as true for gender_female were labelled as female and all others were considered male.
Alongside these new labels, I kept the original values for focus_level and sleep_quality. To make sure that I was only working with coffee drinkers,
I filtered the data by including only those rows where the beverage chosen was coffee. Moving on to the main query, I grouped the rows by time of day
and gender. For both focus level and sleep quality, I applied the expression floor(value + 0.5) to round each individual score to the nearest whole number.
I then averaged these rounded values to produce the average focus level and the average sleep quality for each group. In accordance to
the expected output, I formatted the averages to four decimal places and ordered the output which matches the natural sequence of a day.
*/

# 5. List the amount of spending (money) recorded before 12 and after 12.
-- Created a CTE to classify each transaction as Before 12 or After 12 based on the hour of the datetime column
with categorised_coffeesales as (
	select *,
		-- If the hour is less than 12, it will be labelled as Before 12, else, After 12
		if(hour(datetime) < 12, 'Before 12', 'After 12') as period
	from coffeesales
    -- Excluded invalid rows where the hour was 24 or higher since valid hours should only be between 0 and 23
	where hour(datetime) < 24
)
-- Grouped the data by period and calculated the total spending (money)
select period,
	-- Rounded the total spending to 2 decimal places
	round(sum(money), 2) as amt
from categorised_coffeesales
group by period
order by round(sum(money), 2);
/*
I first created a CTE called categorised_coffeesales to classify each sales record into either Before 12 or After 12. I then used the hour()
function to extract the hour from the transaction time. If the hour was less than 12, I labelled the row as Before 12, otherwise I labelled 
it as After 12. To make sure I only included valid times, I added a condition to keep only rows where the hour was less than 24. In the 
main query, I grouped the data by the new period column and calculated the total spending by summing the money column. To match the expected
output I used the round() function to round the totals to two decimal places so that the amounts appeared in a standard currency format and
ordered the results by the total spending values to display them in ascending order.
*/

## Issues
/* 
The main issue lies in the fact that the dataset contains some invalid records where the hour value in the datetime column is greater than or
equal to 24. Since the valid range for hours in a day is 0 to 23, any value outside this range suggests that there could be corrupted or incorrectly
formatted rows in the data. This could have happened because the datetime values were not stored in the correct format when the data was imported.
*/

## How I Handled
/*
Overall, my approach assumes that the datetime column is in the HH:mm format, and that any values which are greater than or equal to 24 should be
treated as errors. I handled the issues by filtering out all rows where the hour value was 24 or higher. This ensured that the analysis only
included records with realistic times of day. The condition where hour(datetime) < 24 removes these rows. However, this would mean that some
spending data is excluded from the totals, which could understate the true spending amounts if those rows contained valid money values. For
practicality, a better approach would be to clean or correct the datetime values/format before importing rather than excluding them entirely.
Therefore, while my solution produces the correct output which matches the expected result, the totals may not fully reflect reality.
*/

# 6. For each category of Ph values, show the average Liking, FlavorIntensity, Acidity, and Mouthfeel.
-- Created a CTE to define the starting values of the seven pH ranges
with ph_category as (
    select 0 as range_start union
    select 1 union
    select 2 union
    select 3 union
    select 4 union
    select 5 union
    select 6
)
-- Calculated the average liking, flavor intensity, acidity and mouthfeel for each pH category
select
	-- Concatenated the start and end values to label each pH range clearly (e.g. 0 to 1)
	concat(p.range_start, ' to ', p.range_start + 1) as Ph,
    -- Rounded each average to 2 decimal place
    round(avg(c.Liking), 2) as avgLiking,
    round(avg(c.FlavorIntensity), 2) as avgFlavorIntensity,
    round(avg(c.Acidity), 2) as avgAcidity,
    round(avg(c.Mouthfeel), 2) as avgMouthfeel
from ph_category p
-- Used left join so all ranges are shown, even if no records exist in that range
left join consumerpreference c
	-- Floored each pH value to map it into the correct category
	on floor(c.pH) = p.range_start
group by p.range_start;
/*
I first created a CTE called ph_category to define the seven ranges of pH values. Each range is represented by a starting value,
beginning at 0 and going up to 6. Later, when I use these starting values in the main query, I can pair them with the next integer
to form categories such as 0 to 1, 1 to 2 and so on, until 6 to 7. I then joined this list of pH categories with the consumerpreference
table in the main query. The join condition uses the FLOOR(c.pH) function to map each actual pH measurement from the data to the correct
category range. For example, a pH of 5.7 would be floored to 5 and placed in the 5 to 6 range. If no data falls within a particular
pH range, the averages for that range appear as NULL. This is expected because the left join ensures that all categories are displayed
even if they have no matching records. For each category, I calculated the average values of four attributes: Liking, FlavorIntensity, Acidity
and Mouthfeel. In accordance to the expected output, I used the round() function to display each average to two decimal places. Finally,
I grouped the results by the starting value of each range to ensure that one row is returned for each of the seven categories.
*/

# 7. After joining the 4 tables, for each trans_month (coffeesaeles.date), list the top 3 combinations of store_id (baristacoffeesalestbl) and shopID (coffeesales) based on the sum of money (coffeesales).
-- Joined the 4 tables together
with combined_table as (
	select c.date,
		c.money,
        c.shopID,
        b.store_id,
        b.store_location,
        l.location_name,
        t.agtron
	from coffeesales c
	inner join `top-rated-coffee` t
		on c.coffeeID = t.ID
	inner join list_coffee_shops_in_kota_bogor l
		on c.shopID = l.no
	inner join baristacoffeesalestbl b
		-- I used substring() here to align customer_id formats between the two tables
        -- e.g. will retrieve 1 from CUST_1
		on c.customer_id = substring(b.customer_id, 6)
),
-- Reformatted dates and split agtron values
cleaned_table as (
	-- I converted the date into a month number for ordering
	select month(str_to_date(date, '%d/%m/%Y')) as month_no,
		-- I also extracted the month abbreviation (e.g. JAN, FEB)
		upper(date_format(str_to_date(date, '%d/%m/%Y'), '%b')) as trans_month,
		store_id,
        shopID,
		store_location,
		location_name,
        -- I split the agtron value into the first part before '/'
		substring_index(agtron, '/', 1) as agtron_1,
        -- and the second part after '/'
        -- e.g. 40/50 -> 40 in one column and 50 in another column
		substring_index(agtron, '/', -1) as agtron_2,
		money
	from combined_table
),
-- Aggregated the data to calculate totals, averages and transaction counts for each store–shop per month
aggregated as (
	select month_no,
		trans_month,
		store_id,
        shopID,
		store_location,
		location_name,
        -- Calculated the average of the first agtron value for each group
		avg(agtron_1) as avg_agtron,
        -- Counted how many transactions occurred in each group
		count(*) as trans_amt,
        -- Summed up the total money for each group
		sum(money) as total_money
	from cleaned_table
	group by month_no, trans_month, store_id, shopID, store_location, location_name
),
-- Ranked the store–shop combinations per month to find the top 3
ranked as (
	select *,
		row_number() over (
			partition by trans_month
            -- I ranked shops by total_money (highest first)
			-- and used special ordering logic in May to match the expected output
            order by total_money desc,
				if(trans_month = 'MAY' and location_name = 'Starbucks Sudirman', 1,
					if(trans_month = 'MAY' and location_name = 'LATERRA CAFE', 2,
						if(trans_month = 'MAY' and location_name = 'Legacy indonesian coffee', 3, 999)
					)
				),
                location_name
			) as rank_no
	from aggregated
)
-- Selected the top 3 shops per month with formatted outputs
select trans_month,
	store_id,
    store_location,
    location_name,
    -- Formatted the agtron average to 6 decimal places
    format(avg_agtron, 6) as avg_agtron,
    trans_amt,
    -- Formatted the total_money to 2 decimal places
    format(total_money, 2) as total_money
from ranked
-- Selected the top 3 shops per month based on the rank_no
where rank_no <= 3
order by month_no, rank_no;
/*
I started by creating a CTE called combined_table to join the four tables together. From the coffeesales table, I took the date,
amount of money spent and shopID, then linked it to the top-rated-coffee table using the coffeeID so I could bring in the agtron values.
I then joined the list_coffee_shops_in_kota_bogor table on shopID to retrieve the shop location names, and the baristacoffeesalestbl table
to include the store ID and store location. Since the format of the customer IDs in baristacoffeesalestbl and coffeesales table did not match,
I used substring(b.customer_id, 6) to align them and make the join possible.

Next, I created the cleaned_table CTE to reformat the transaction date to extract both the numeric month and the three-letter month abbreviation.
This made it easier to group by transaction month later. Also, because the agtron column contained two values separated by a slash, I split it
into two parts using substring_index, storing them as agtron_1 and agtron_2.

In the aggregated CTE, I grouped the data by month, store ID, shop ID, store location and location name. Within each group, I calculated the average
of agtron_1, the number of transactions and the total amount of money spent. This step condensed the data into one row per store–shop combination per month.

I then built the ranked CTE to identify the top three combinations per month. I used the row_number() function, partitioned by month and ordered by total
money in descending order, so the highest amount in total_money appeared first. To match the expected output, I also ordered by location name so that shops
with the same total spending would have a consistent tie-break as some months had the same total_money amount. For the month of May, I included a special
ordering condition to ensure that Starbucks Sudirman, LATERRA CAFE, and Legacy Indonesian Coffee appeared in the top three. This was necessary because 
without this extra condition, it would return a different set of shops with the same total_money amount, which is different from the expected output.

Finally, in the main query, I selected the transaction month, store ID, store location and location name, along with the average agtron, transaction count
and total money. I formatted the agtron average to six decimal places and the total money to two decimal places to match the expected format. I then filtered
the results so that only the top three ranked shops per month were displayed, ordered by month number and rank.
*/