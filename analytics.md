# Analytical queries in Amazon Athena and their analyses 

### 1. Holiday vs. Average day Demand Analysis

showcases the exact percentage increase in gaming demand during holidays compared to average days.

```
WITH DailyAverages AS (
    SELECT 
        region,
        game,
        AVG(hours_played) as avg_normal_hours
    FROM "gamingdb"."raw_gaming_data"
    GROUP BY region, game
)
SELECT 
    g.region,
    g.game,
    g.date,
    h.name AS holiday_name,
    g.hours_played AS holiday_hours,
    da.avg_normal_hours,
    ROUND(((g.hours_played - da.avg_normal_hours) / da.avg_normal_hours) * 100, 2) AS pct_increase
FROM "gamingdb"."raw_gaming_data" g
JOIN "gamingdb"."api_holiday_data" h ON g.date = h.date AND g.region = h.countryCode
JOIN DailyAverages da ON g.region = da.region AND g.game = da.game
ORDER BY pct_increase DESC;
```

![holiday percentage increase](/images/holiday_percentage_increase.png)

*As it is seen in the table gaming hours on holidays can more than double compared to an average day, which is true for virtually any holiday, incuding Christmas, New years and also Labour day also*
### 2. Top 10 days (holidays) where gaming hours are increased

showcase the top holidays for gaming by region 

```
WITH HolidayEngagement AS (
    SELECT 
        h.countryCode AS region,
        h.name AS holiday_name,
        g.date,
        SUM(g.hours_played) AS total_daily_hours
    FROM "gamingdb"."api_holiday_data" h
    JOIN "gamingdb"."raw_gaming_data" g 
        ON h.date = g.date AND h.countryCode = g.region
    GROUP BY h.countryCode, h.name, g.date
),
RankedHolidays AS (
    SELECT 
        region,
        holiday_name,
        date,
        total_daily_hours,
        RANK() OVER (PARTITION BY region ORDER BY total_daily_hours DESC) as demand_rank
    FROM HolidayEngagement
)
SELECT 
    region,
    holiday_name,
    date,
    total_daily_hours
FROM RankedHolidays
WHERE demand_rank <= 10  -- Show the Top 10 holidays for each region
ORDER BY region, total_daily_hours DESC;
```
Top holidays for gaming in Germany:
![](/images/de_top_holidays.png)

Top holidays for gaming in Great Britain:
![](/images/gb_top_holiday.png)

Top holidays for gaming in the United States:
![](/images/us_top_holidays.png)

*when broken down by countries you can see here the top 10 gaming days where all the gaming hours are summed New Years for example is in the top 3 in all 3 countries* (the numbers are in this order because of the query)

### 3. Market-Specific Peak Detection in the United States

track KPI for Forecast Accuracy, query that identifies the top 10 highest-demand days in the US and checks if they were predicted holidays
```
SELECT 
    game,
    date,
    hours_played,
    region,
    (CASE WHEN date IN (SELECT date FROM "gamingdb"."api_holiday_data") THEN 'Holiday' ELSE 'Standard' END) as day_status
FROM "gamingdb"."raw_gaming_data"
WHERE region = 'US'
ORDER BY hours_played DESC
LIMIT 10;
```
![](/images/top_gaming_days_us.png)

*here the data is ordered by how may hours people played it, and each day is labeled if it is a holiday or not, as it is shown, all top 10 results are on holidays (in the US)*

### 4. Top Game per Holiday in Germany

calculates the total hours played for each game on every German holiday and determines the #1 game for each specific date

```
WITH GermanHolidayDemand AS (
    SELECT 
        h.name AS holiday_name,
        g.game,
        g.date,
        SUM(g.hours_played) AS total_hours
    FROM "gamingdb"."api_holiday_data" h
    JOIN "gamingdb"."raw_gaming_data" g 
        ON h.date = g.date AND h.countryCode = g.region
    WHERE h.countryCode = 'DE'  -- Filter specifically for Germany
    GROUP BY h.name, g.game, g.date
),
GameRankings AS (
    SELECT 
        holiday_name,
        game,
        date,
        total_hours,
        RANK() OVER (PARTITION BY holiday_name ORDER BY total_hours DESC) as game_rank
    FROM GermanHolidayDemand
)
SELECT 
    holiday_name,
    date,
    game AS top_game,
    total_hours
FROM GameRankings
WHERE game_rank = 1  -- Only show the most in-demand game per holiday
ORDER BY date ASC;
```

![](/images/games_on_days_germany.png)

*this quers shows us the top game on each holiday (in Germany) which we can see here is mostly Minecraft*

### 5. Total hours per game in Great Britain

the historical data for the 'GB' region, groups the results by game title, and calculates the total hours played across the entire dataset

```
SELECT 
    game, 
    SUM(hours_played) AS total_hours_played,
    COUNT(DISTINCT date) AS days_tracked
FROM "gamingdb"."raw_gaming_data"
WHERE region = 'GB'
GROUP BY game
ORDER BY total_hours_played DESC;
```

![](/images/uk_games_ranked.png)

*this here is pretty self-explanatory, the games are ranked by how many hours were played all year*

### 6. Gaming increase on holidays

calculates the average gaming hours for each game on a normal day versus a holiday in the UK. This is excellent for proving which specific titles are "holiday favorites."


```
SELECT 
    g.game,
    AVG(CASE WHEN h.name IS NULL THEN g.hours_played END) AS avg_normal_hours,
    AVG(CASE WHEN h.name IS NOT NULL THEN g.hours_played END) AS avg_holiday_hours,
    (AVG(CASE WHEN h.name IS NOT NULL THEN g.hours_played END) - 
     AVG(CASE WHEN h.name IS NULL THEN g.hours_played END)) / 
     AVG(CASE WHEN h.name IS NULL THEN g.hours_played END) * 100 AS percentage_lift
FROM "gamingdb"."raw_gaming_data" g
LEFT JOIN "gamingdb"."api_holiday_data" h ON g.date = h.date AND g.region = h.countryCode
WHERE g.region = 'GB'
GROUP BY g.game
ORDER BY percentage_lift DESC;
```

![](/images/game_holiday_percent_helpicantnamethemanymore.png)

*here we can see the difference in averages on holidays versus non-holidays ordered by the difference in percentage (in Great Britain again) *

### 7. Weekends vs. Weekdays

categorizes every day of the year into four distinct buckets. 


```
WITH CategorizedData AS (
    SELECT 
        g.hours_played,
        -- Determine if the day is a Weekend (6=Sat, 0=Sun)
        CASE 
            WHEN extract(day_of_week from cast(g.date as date)) IN (6, 7) THEN 'Weekend'
            ELSE 'Weekday'
        END AS day_type,
        -- Determine if the day is a Holiday
        CASE 
            WHEN h.name IS NOT NULL THEN 'Holiday'
            ELSE 'Normal'
        END AS holiday_status
    FROM "gamingdb"."raw_gaming_data" g
    LEFT JOIN "gamingdb"."api_holiday_data" h 
        ON g.date = h.date AND g.region = h.countryCode
)
SELECT 
    CASE 
        WHEN day_type = 'Weekday' AND holiday_status = 'Normal' THEN 'Average Weekday'
        WHEN day_type = 'Weekend' AND holiday_status = 'Normal' THEN 'Average Weekend'
        WHEN day_type = 'Weekday' AND holiday_status = 'Holiday' THEN 'Weekday coincides with Holiday'
        WHEN day_type = 'Weekend' AND holiday_status = 'Holiday' THEN 'Weekend coincides with Holiday'
    END AS category,
    ROUND(AVG(hours_played), 2) AS avg_hours,
    COUNT(*) AS sample_size_days
FROM CategorizedData
GROUP BY 1
ORDER BY avg_hours DESC;
```

![](/images/weekend_weekday_holiday.png)

*on weekdays that coincide with holidays we can see a significant increasse in average gaming hours*

## Correlation

### Holiday-gaming correlation by region
```
SELECT 
    region,
    -- CORR(y, x) calculates the Pearson coefficient
    -- y = gaming hours (target), x = holiday status (indicator)
    CORR(
        CAST(hours_played AS DOUBLE), 
        CAST(CASE WHEN h.name IS NOT NULL THEN 1 ELSE 0 END AS DOUBLE)
    ) AS holiday_correlation_score
FROM "gamingdb"."raw_gaming_data" g
LEFT JOIN "gamingdb"."api_holiday_data" h 
    ON g.date = h.date AND g.region = h.countryCode
GROUP BY region;
```
![](/images/holi_corr.png)

### Weekend-gaming correlation by region
```
SELECT 
    region,
    CORR(
        CAST(hours_played AS DOUBLE), 
        CAST(CASE WHEN extract(day_of_week from cast(date as date)) IN (6, 7) THEN 1 ELSE 0 END AS DOUBLE)
    ) AS weekend_correlation_score
FROM "gamingdb"."raw_gaming_data"
GROUP BY region;
```

![](/images/wknd_corr.png)

### 
```
WITH CategorizedData AS (
    SELECT 
        hours_played,
        -- Determine Weekend (6=Sat, 7=Sun)
        CASE WHEN extract(day_of_week from cast(g.date as date)) IN (6, 7) THEN 1 ELSE 0 END AS is_weekend,
        -- Determine Holiday
        CASE WHEN h.name IS NOT NULL THEN 1 ELSE 0 END AS is_holiday
    FROM "gamingdb"."raw_gaming_data" g
    LEFT JOIN "gamingdb"."api_holiday_data" h 
        ON g.date = h.date AND g.region = h.countryCode
)
SELECT 
    -- Correlation for Weekday Holidays
    CORR(hours_played, CASE WHEN is_weekend = 0 AND is_holiday = 1 THEN 1 ELSE 0 END) AS holiday_weekday_corr,
    -- Correlation for Weekend Holidays
    CORR(hours_played, CASE WHEN is_weekend = 1 AND is_holiday = 1 THEN 1 ELSE 0 END) AS holiday_weekend_corr,
    -- Correlation for Normal Weekends
    CORR(hours_played, CASE WHEN is_weekend = 1 AND is_holiday = 0 THEN 1 ELSE 0 END) AS normal_weekend_corr,
    -- Correlation for Normal Weekdays
    CORR(hours_played, CASE WHEN is_weekend = 0 AND is_holiday = 0 THEN 1 ELSE 0 END) AS normal_weekday_corr
FROM CategorizedData;
```
![](/images/hosszunev_corr.png)





### Interpretation

[Interpretation of the findings (link)](/interpretation.md)

