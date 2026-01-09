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

==very important analysis==

### 2. Top 10 days (holidays) where gaming hours are increased

showcase the top holidays by region based on your business goals , you can run a query that aggregates the total gaming hours for each holiday

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

==very important analysis==

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

==very important analysis==

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

==very important analysis==

### 5. 

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

==very important analysis==


### 6.

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

==very important analysis==


### 7.

categorizes every day of the year into four distinct buckets. This allows you to prove to your stakeholders exactly how much "extra" value a holiday adds on top of a normal weekday or weekend baseline.


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

==very important analysis==



