USE bc2402_gp;

/* Q1 */
SELECT 
    MIN(flags) AS flags,
    MIN(instruction) AS instruction,
    category,
    MIN(intent) AS intent,
    MIN(response) AS response
FROM bc2402_gp.customer_support
GROUP BY category;
-- Clearly shows that the flags without letter "B" are not part of the customer reviews

-- Clean the data by removing the names which don't fit the category
SET SQL_SAFE_UPDATES = 0;
DELETE FROM bc2402_gp.customer_support
WHERE flags NOT LIKE "B";

-- Check the categories after cleaning
SELECT DISTINCT category
FROM bc2402_gp.customer_support;

-- Count the number of categories after cleaning
SELECT COUNT(DISTINCT (category)) AS TotalCategories
FROM bc2402_gp.customer_support;

/* Q2 */
select distinct category
from customer_support
where category in ("ACCOUNT", "CANCEL", "CONTACT", "DELIVERY", "FEEDBACK", "INVOICE", "ORDER", "PAYMENT", "REFUND", 
"SHIPPING", "SUBSCRIPTION"); 
## hard code the category as it does not show the categories in dataset without WHERE clause


## there are 2 ways - to find flags with Q or W only, OR flags with Q and W
select category,
    count(case when flags like '%Q%' 
			and flags not like '%W%' then 1 
          end) as colloquial_only_count,                    ## count records that only have Q in flags
    count(case when flags like '%W%' 
			and flags not like '%Q%' then 1 
          end) as offensive_only_count,                     ## count records that only have W in flags
    count(case when flags like '%Q%' 
			and flags like '%W%' then 1 
          end) as both_colloquial_and_offensive_count       ## count records that have both QW in flags
from customer_support
where category in ("ACCOUNT", "CANCEL", "CONTACT", "DELIVERY", "FEEDBACK", "INVOICE", "ORDER", "PAYMENT", "REFUND", 
"SHIPPING", "SUBSCRIPTION")                                 ## hard code category as it is not cleaned
group by category;


/* Q3 */
SELECT 
    Airline,
    COUNT(*) AS Instances,
    'Cancellation' AS Type
FROM 
    Flight_delay
WHERE 
    Cancelled = 1
GROUP BY 
    Airline

UNION ALL

SELECT 
    Airline,
    COUNT(*) AS Instances,
    'Delay' AS Type
FROM 
    Flight_delay
WHERE 
    CarrierDelay > 0 
    OR WeatherDelay > 0 
    OR NASDelay > 0 
    OR SecurityDelay > 0 
    OR LateAircraftDelay > 0
GROUP BY 
    Airline
ORDER BY 
    Airline, Type;
    
/* Q4 */
WITH DelayCounts AS (
    SELECT
        DATE_FORMAT(STR_TO_DATE(Date, '%d-%m-%Y'), '%Y-%m') AS YearMonth,
        CONCAT(Origin, ' to ', Dest) AS Route,
        COUNT(*) AS DelayCount
    FROM
        flight_delay
    WHERE
        ArrDelay > 0
    GROUP BY
        YearMonth, Route
)
SELECT
    YearMonth,
    Route,
    DelayCount
FROM
    DelayCounts AS dc
WHERE
    (YearMonth, DelayCount) IN (
        SELECT
            YearMonth,
            MAX(DelayCount)
        FROM
            DelayCounts
        GROUP BY
            YearMonth
    )
ORDER BY
    YearMonth;


/* Q5 */
WITH cte AS (

    -- Calculate year, quarter, and stock metrics (maxHigh, minLow, avgPrice) grouped by year and quarter
    SELECT 
        YEAR(STR_TO_DATE(StockDate, '%m/%d/%Y')) AS year,
        CEILING(MONTH(STR_TO_DATE(StockDate, '%m/%d/%Y')) / 3) AS quarter,
        MAX(High) AS maxHigh,
        MIN(Low) AS minLow,
        AVG(Price) AS avgPrice
    FROM sia_stock
    WHERE STR_TO_DATE(StockDate, '%m/%d/%Y') >= '2023-01-01' -- Filter records for the year 2023
      AND STR_TO_DATE(StockDate, '%m/%d/%Y') < '2024-01-01'
    GROUP BY year, quarter
)
SELECT
    cte.year,
    cte.quarter,
    cte.maxHigh,
    cte.minLow,
    cte.avgPrice,
    -- Calculate QoQ changes using LAG function
    cte.maxHigh - LAG(cte.maxHigh) OVER (ORDER BY cte.year, cte.quarter) AS qoqHighChange,
    cte.minLow - LAG(cte.minLow) OVER (ORDER BY cte.year, cte.quarter) AS qoqLowChange,
    cte.avgPrice - LAG(cte.avgPrice) OVER (ORDER BY cte.year, cte.quarter) AS qoqAvgChange
FROM cte
ORDER BY cte.year, cte.quarter; -- Order results by year and quarter


/* Q6 */
/*Additional Analysis has been done in a jupyter notebook, link has been provided in the report */

-- For each sales_channel and each route, display the specified ratios
SELECT
    sales_channel,
    route,
    -- Calculate the ratios, handling division by zero
    CASE
        WHEN AVG(flight_hour) = 0 THEN NULL
        ELSE AVG(length_of_stay) / AVG(flight_hour)
    END AS avgLengthOfStayPerAvgFlightHour,
    CASE
        WHEN AVG(flight_hour) = 0 THEN NULL
        ELSE AVG(wants_extra_baggage) / AVG(flight_hour)
    END AS avgExtraBaggagePerAvgFlightHour,
    CASE
        WHEN AVG(flight_hour) = 0 THEN NULL
        ELSE AVG(wants_preferred_seat) / AVG(flight_hour)
    END AS avgPreferredSeatPerAvgFlightHour,
    CASE
        WHEN AVG(flight_hour) = 0 THEN NULL
        ELSE AVG(wants_in_flight_meals) / AVG(flight_hour)
    END AS avgInFlightMealsPerAvgFlightHour
FROM
    customer_booking
GROUP BY
    sales_channel,
    route;


/* Q7 */
SELECT Airline, Class,
    CASE 
        WHEN SUBSTRING_INDEX(MonthFlown, '-', 1) IN ('Jun', 'Jul', 'Aug', 'Sep') THEN 'Seasonal'
        ELSE 'Non-Seasonal'
    END AS Seasonality,
    AVG(SeatComfort) AS Avg_SeatComfort,
    AVG(FoodnBeverages) AS Avg_FoodnBeverages,
    AVG(InflightEntertainment) AS Avg_InflightEntertainment,
    AVG(ValueForMoney) AS Avg_ValueForMoney,
    AVG(OverallRating) AS Avg_OverallRating
FROM 
    airlines_reviews
GROUP BY Airline, Class, Seasonality
ORDER BY Airline, Class, Seasonality;


/* Q8 */
## Total no. of reviews
select count(*) as TotalNoOfReviews
from airlines_reviews
where Verified = "TRUE"
and Recommended = "no";

## Count of Passengers for each Airline and TypeofTraveller
select distinct Airline, TypeofTraveller, count(*) as CountOfPassengers
from airlines_Reviews
where Verified = "TRUE"
and Recommended = "no"
group by Airline, TypeofTraveller;

# Average Overall Rating 
select Airline, TypeofTraveller, avg(OverallRating) as AvgOverallRating
from airlines_reviews
where Verified = "TRUE"
and Recommended = "no"
group by TypeofTraveller, Airline;

## Average Rating across all rating components
select Airline, TypeofTraveller, 
	avg(SeatComfort) as ComfortRating, 
    avg(StaffService) as ServiceRating, 
    avg(FoodnBeverages) as MealRating,
    avg(InflightEntertainment) as EntertainmentRating,
    avg(ValueForMoney) as CostRating
from airlines_reviews
where Verified = "TRUE"
and Recommended = "no"
group by TypeofTraveller, Airline;

## Number of Complaints for Each Category
select Airline, TypeofTraveller,
	count(case when SeatComfort <= 3
			then 1
		end) as SeatIssue,
	count(case when StaffService <= 3
			then 1
		end) as ServiceIssue,
	count(case when FoodnBeverages <= 3
			then 1
		end) as MealIssue,
	count(case when InflightEntertainment <= 3
			then 1
		end) as EntertainmentIssue,
	count(case when ValueForMoney <= 3
			then 1
		end) as CostIssue
from airlines_reviews
where Verified = "TRUE"
and Recommended = "no"
group by TypeofTraveller, Airline;

### to find frequency of words associated with common airline complaints
### for Singapore Airlines -----
select TypeOfTraveller, "lost" as Word, count(case when lower(Reviews) like '%lost%' then 1 end) as Frequency
from airlines_reviews
where Airline = "Singapore Airlines"
group by TypeOfTraveller
union all
select TypeOfTraveller, "baggage" as Word, count(case when lower(Reviews) like '%baggage%' then 1 end) as Frequency
from airlines_reviews
where Airline = "Singapore Airlines"
group by TypeOfTraveller
union all
select TypeOfTraveller, "delay" as Word, count(case when lower(Reviews) like '%delay%' then 1 end) AS Frequency
from airlines_reviews
where Airline = "Singapore Airlines"
group by TypeOfTraveller
union all
select TypeOfTraveller, "uncomfortable" as Word, count(case when lower(Reviews) like '%uncomfortable%' then 1 end) AS Frequency
from airlines_reviews
where Airline = "Singapore Airlines"
group by TypeOfTraveller
union all
select TypeOfTraveller, "legroom" as Word, count(case when lower(Reviews) like '%legroom%' then 1 end) AS Frequency
from airlines_reviews
where Airline = "Singapore Airlines"
group by TypeOfTraveller
union all
select TypeOfTraveller, "leg room" as Word, count(case when lower(Reviews) like '%leg room%' then 1 end) AS Frequency
from airlines_reviews
where Airline = "Singapore Airlines"
group by TypeOfTraveller
union all
select TypeOfTraveller, "small" as Word, count(case when lower(Reviews) like '%small%' then 1 end) AS Frequency
from airlines_reviews
where Airline = "Singapore Airlines"
group by TypeOfTraveller
union all
select TypeOfTraveller, "curt" as Word, count(case when lower(Reviews) like '%curt%' then 1 end) AS Frequency
from airlines_reviews
where Airline = "Singapore Airlines"
group by TypeOfTraveller
union all
select TypeOfTraveller, "unfriendly" as Word, count(case when lower(Reviews) like '%unfriendly%' then 1 end) AS Frequency
from airlines_reviews
where Airline = "Singapore Airlines"
group by TypeOfTraveller
union all
select TypeOfTraveller, "rude" as Word, count(case when lower(Reviews) like '%rude%' then 1 end) AS Frequency
from airlines_reviews
where Airline = "Singapore Airlines"
group by TypeOfTraveller
union all
select TypeOfTraveller, "dirty" as Word, count(case when lower(Reviews) like '%dirty%' then 1 end) AS Frequency
from airlines_reviews
where Airline = "Singapore Airlines"
group by TypeOfTraveller
union all
select TypeOfTraveller, "refund" as Word, count(case when lower(Reviews) like '%refund%' then 1 end) AS Frequency
from airlines_reviews
where Airline = "Singapore Airlines"
group by TypeOfTraveller
union all
select TypeOfTraveller, "meal" as Word, count(case when lower(Reviews) like '%meal%' then 1 end) AS Frequency
from airlines_reviews
where Airline = "Singapore Airlines"
group by TypeOfTraveller
union all
select TypeOfTraveller, "food" as Word, count(case when lower(Reviews) like '%food%' then 1 end) AS Frequency
from airlines_reviews
where Airline = "Singapore Airlines"
group by TypeOfTraveller
union all
select TypeOfTraveller, "wi-fi" as Word, count(case when lower(Reviews) like '%wi-fi%' then 1 end) AS Frequency
from airlines_reviews
where Airline = "Singapore Airlines"
group by TypeOfTraveller
union all
select TypeOfTraveller, "wifi" as Word, count(case when lower(Reviews) like '%wifi%' then 1 end) AS Frequency
from airlines_reviews
where Airline = "Singapore Airlines"
group by TypeOfTraveller
union all
select TypeOfTraveller, "console" as Word, count(case when lower(Reviews) like '%console%' THEN 1 END) AS Frequency
from airlines_reviews
where Airline = "Singapore Airlines"
group by TypeOfTraveller
union all
select TypeOfTraveller, "tv" as Word, count(case when lower(Reviews) like '%tv%' then 1 end) AS Frequency
from airlines_reviews
where Airline = "Singapore Airlines"
group by TypeOfTraveller
union all
select TypeOfTraveller, "expensive" as Word, count(case when lower(Reviews) like '%expensive%' then 1 end) AS Frequency
from airlines_reviews
where Airline = "Singapore Airlines"
group by TypeOfTraveller
order by TypeOfTraveller asc, frequency desc;

### for Qatar Airways -- 
select TypeOfTraveller, "lost" as Word, count(case when lower(Reviews) like '%lost%' then 1 end) as Frequency
from airlines_reviews
where Airline = "Qatar Airways"
group by TypeOfTraveller
union all
select TypeOfTraveller, "baggage" as Word, count(case when lower(Reviews) like '%baggage%' then 1 end) as Frequency
from airlines_reviews
where Airline = "Qatar Airways"
group by TypeOfTraveller
union all
select TypeOfTraveller, "delay" as Word, count(case when lower(Reviews) like '%delay%' then 1 end) AS Frequency
from airlines_reviews
where Airline = "Qatar Airways"
group by TypeOfTraveller
union all
select TypeOfTraveller, "uncomfortable" as Word, count(case when lower(Reviews) like '%uncomfortable%' then 1 end) AS Frequency
from airlines_reviews
where Airline = "Qatar Airways"
group by TypeOfTraveller
union all
select TypeOfTraveller, "legroom" as Word, count(case when lower(Reviews) like '%legroom%' then 1 end) AS Frequency
from airlines_reviews
where Airline = "Qatar Airways"
group by TypeOfTraveller
union all
select TypeOfTraveller, "leg room" as Word, count(case when lower(Reviews) like '%leg room%' then 1 end) AS Frequency
from airlines_reviews
where Airline = "Qatar Airways"
group by TypeOfTraveller
union all
select TypeOfTraveller, "small" as Word, count(case when lower(Reviews) like '%small%' then 1 end) AS Frequency
from airlines_reviews
where Airline = "Qatar Airways"
group by TypeOfTraveller
union all
select TypeOfTraveller, "curt" as Word, count(case when lower(Reviews) like '%curt%' then 1 end) AS Frequency
from airlines_reviews
where Airline = "Qatar Airways"
group by TypeOfTraveller
union all
select TypeOfTraveller, "unfriendly" as Word, count(case when lower(Reviews) like '%unfriendly%' then 1 end) AS Frequency
from airlines_reviews
where Airline = "Qatar Airways"
group by TypeOfTraveller
union all
select TypeOfTraveller, "rude" as Word, count(case when lower(Reviews) like '%rude%' then 1 end) AS Frequency
from airlines_reviews
where Airline = "Qatar Airways"
group by TypeOfTraveller
union all
select TypeOfTraveller, "dirty" as Word, count(case when lower(Reviews) like '%dirty%' then 1 end) AS Frequency
from airlines_reviews
where Airline = "Qatar Airways"
group by TypeOfTraveller
union all
select TypeOfTraveller, "refund" as Word, count(case when lower(Reviews) like '%refund%' then 1 end) AS Frequency
from airlines_reviews
where Airline = "Qatar Airways"
group by TypeOfTraveller
union all
select TypeOfTraveller, "meal" as Word, count(case when lower(Reviews) like '%meal%' then 1 end) AS Frequency
from airlines_reviews
where Airline = "Qatar Airways"
group by TypeOfTraveller
union all
select TypeOfTraveller, "food" as Word, count(case when lower(Reviews) like '%food%' then 1 end) AS Frequency
from airlines_reviews
where Airline = "Qatar Airways"
group by TypeOfTraveller
union all
select TypeOfTraveller, "wi-fi" as Word, count(case when lower(Reviews) like '%wi-fi%' then 1 end) AS Frequency
from airlines_reviews
where Airline = "Qatar Airways"
group by TypeOfTraveller
union all
select TypeOfTraveller, "wifi" as Word, count(case when lower(Reviews) like '%wifi%' then 1 end) AS Frequency
from airlines_reviews
where Airline = "Qatar Airways"
group by TypeOfTraveller
union all
select TypeOfTraveller, "console" as Word, count(case when lower(Reviews) like '%console%' THEN 1 END) AS Frequency
from airlines_reviews
where Airline = "Qatar Airways"
group by TypeOfTraveller
union all
select TypeOfTraveller, "tv" as Word, count(case when lower(Reviews) like '%tv%' then 1 end) AS Frequency
from airlines_reviews
where Airline = "Qatar Airways"
group by TypeOfTraveller
union all
select TypeOfTraveller, "expensive" as Word, count(case when lower(Reviews) like '%expensive%' then 1 end) AS Frequency
from airlines_reviews
where Airline = "Qatar Airways"
group by TypeOfTraveller
order by TypeOfTraveller asc, frequency desc;


/* Q9 */
-- Filter data for Singapore Airlines and Verified="TRUE", then group by year
SELECT 
    RIGHT(MonthFlown, 2) AS Year, -- Extract the last 2 characters (year) from MonthFlown
    COUNT(*) AS TotalReviews, -- Count total reviews for each year
    SUM(CASE WHEN Recommended = 'yes' THEN 1 ELSE 0 END) AS YesCount, -- Count "yes" recommendations
    SUM(CASE WHEN Recommended = 'no' THEN 1 ELSE 0 END) AS NoCount, -- Count "no" recommendations
    ROUND(SUM(CASE WHEN Recommended = 'yes' THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS YesPercent, -- Percentage of "yes"
    ROUND(SUM(CASE WHEN Recommended = 'no' THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS NoPercent, -- Percentage of "no"
    AVG(OverallRating) AS AvgOverallRating, -- Average overall rating
    AVG(SeatComfort) AS AvgSeatComfortRating, -- Average seat comfort rating
    AVG(StaffService) AS AvgStaffServiceRating, -- Average staff service rating
    AVG(FoodnBeverages) AS AvgFoodBeveragesRating, -- Average food and beverage rating
    AVG(InflightEntertainment) AS AvgEntertainmentRating, -- Average entertainment rating
    AVG(ValueForMoney) AS AvgValueformoneyRating -- Average value for money rating
FROM bc2402_gp.airlines_reviews
WHERE Airline = 'Singapore Airlines' AND Verified = 'TRUE'
GROUP BY RIGHT(MonthFlown, 2) -- Group by the extracted year
ORDER BY Year;


/* Q10 */
SELECT 
	AVG(SeatComfort) AS seat_comfort, 
	AVG(StaffService) AS staff_service, 
	AVG(FoodnBeverages) AS food_and_beverages,
	AVG(InflightEntertainment) AS in_flight_entertainment,
AVG(ValueForMoney) AS value_for_money,
AVG(OverallRating) AS overall_rating,
COUNT(CASE WHEN Recommended = "yes" THEN 1 END) AS yes_count,
 COUNT(CASE WHEN Recommended = "no" THEN 1 END) AS no_count
FROM
	airlines_reviews
WHERE
	Verified = "TRUE";

SELECT 
	Reviews,
    	OverallRating
FROM
	airlines_reviews
WHERE
	Verified = "TRUE"
    	AND Airline = "Singapore Airlines"
--     	AND Reviews LIKE '%safety%';
--     	AND Reviews LIKE '%turbulence%';
    	AND Reviews LIKE '%compensation%';
 
 
/* QA1 */
-- Volume format is in both Millions and Thousands 
SELECT 
    YEAR(STR_TO_DATE(StockDate, '%m/%d/%Y')) AS Year,
    MONTH(STR_TO_DATE(StockDate, '%m/%d/%Y')) AS Month,
    MAX(High) AS Monthly_HighPrice,
    MIN(Low) AS Monthly_LowPrice,
    SUM(
        CASE 
            WHEN Vol LIKE '%M' THEN CAST(REPLACE(Vol, 'M', '') AS DECIMAL(10, 2)) * 1000000
            WHEN Vol LIKE '%K' THEN CAST(REPLACE(Vol, 'K', '') AS DECIMAL(10, 2)) * 1000
            ELSE CAST(Vol AS DECIMAL(10, 2))
        END
    ) AS Total_TransactionVolume,
    AVG(
        CASE 
            WHEN Vol LIKE '%M' THEN CAST(REPLACE(Vol, 'M', '') AS DECIMAL(10, 2)) * 1000000
            WHEN Vol LIKE '%K' THEN CAST(REPLACE(Vol, 'K', '') AS DECIMAL(10, 2)) * 1000
            ELSE CAST(Vol AS DECIMAL(10, 2))
        END
    ) AS Daily_Avg_TransactionVolume
FROM 
    sia_stock
GROUP BY 
    Year, Month
ORDER BY 
    Year, Month;
    
    
/* QA2 */
/* Step 1a: Understanding customer_booking individually */
SELECT * FROM bc2402_gp.customer_booking;

SELECT COUNT(DISTINCT route) AS UniqueRoutes
FROM customer_booking;

SELECT COUNT(DISTINCT booking_origin) AS UniqueBookingOrigins
FROM customer_booking;

SELECT DISTINCT trip_type AS UniqueTripTypes
FROM customer_booking;

SELECT DISTINCT sales_channel AS UniqueSalesChannels
FROM customer_booking;

SELECT COUNT(DISTINCT flight_hour) AS UniqueFlightHours
FROM customer_booking;

SELECT COUNT(DISTINCT flight_day) AS UniqueFlightDays
FROM customer_booking;

SELECT 
    MAX(num_passengers) AS MaxNumPassengers,
    MIN(num_passengers) AS MinNumPassengers,
    AVG(num_passengers) AS AvgNumPassengers
FROM customer_booking;

SELECT 
    MAX(purchase_lead) AS MaxPurchaseLead,
    MIN(purchase_lead) AS MinPurchaseLead,
    AVG(purchase_lead) AS AvgPurchaseLead
FROM customer_booking;

SELECT 
    MAX(length_of_stay) AS MaxLengthOfStay,
    MIN(length_of_stay) AS MinLengthOfStay,
    AVG(length_of_stay) AS AvgLengthOfStay
FROM customer_booking;

SELECT 
    MAX(flight_duration) AS MaxFlightDuration,
    MIN(flight_duration) AS MinFlightDuration,
    AVG(flight_duration) AS AvgFlightDuration
FROM customer_booking;

SELECT 
    route AS Route,
    COUNT(*) AS BookingCount
FROM customer_booking
GROUP BY route
ORDER BY BookingCount DESC
LIMIT 10;

SELECT 
    booking_origin AS BookingOrigin,
    COUNT(*) AS BookingCount
FROM customer_booking
GROUP BY booking_origin
ORDER BY BookingCount DESC
LIMIT 10;


/* Step 1b: Understanding airlines_reviews individually */
SELECT * FROM bc2402_gp.airlines_reviews;

SELECT COUNT(DISTINCT name) AS UniqueReviewers
FROM airlines_reviews;

SELECT 
    MIN(ReviewDate) AS EarliestReviewDate,
    MAX(ReviewDate) AS LatestReviewDate
FROM airlines_reviews;

SELECT DISTINCT airline AS UniqueAirlines
FROM airlines_reviews;

SELECT 
    verified, 
    COUNT(*) AS Count
FROM airlines_reviews
GROUP BY verified;

SELECT DISTINCT TypeofTraveller AS TypesOfTraveller
FROM airlines_reviews;

SELECT 
    MIN(MonthFlown) AS EarliestMonthFlown,
    MAX(MonthFlown) AS LatestMonthFlown
FROM airlines_reviews;

SELECT COUNT(DISTINCT route) AS UniqueRoutes
FROM airlines_reviews;

SELECT DISTINCT Class AS UniqueClass
FROM airlines_reviews;

SELECT 
    recommended,
    COUNT(*) AS Count
FROM airlines_reviews
GROUP BY recommended;

SELECT 
    airline AS Airline,
    class AS Class,
    TypeofTraveller,
    MIN(SeatComfort) AS MinSeatComfort,
    MAX(SeatComfort) AS MaxSeatComfort,
    AVG(SeatComfort) AS AvgSeatComfort,
    MIN(StaffService) AS MinStaffService,
    MAX(StaffService) AS MaxStaffService,
    AVG(StaffService) AS AvgStaffService,
    MIN(FoodnBeverages) AS MinFoodnBeverages,
    MAX(FoodnBeverages) AS MaxFoodnBeverages,
    AVG(FoodnBeverages) AS AvgFoodnBeverages,
    MIN(InflightEntertainment) AS MinInflightEntertainment,
    MAX(InflightEntertainment) AS MaxInflightEntertainment,
    AVG(InflightEntertainment) AS AvgInflightEntertainment,
    MIN(ValueForMoney) AS MinValueForMoney,
    MAX(ValueForMoney) AS MaxValueForMoney,
    AVG(ValueForMoney) AS AvgValueForMoney,
    MIN(OverallRating) AS MinOverallRating,
    MAX(OverallRating) AS MaxOverallRating,
    AVG(OverallRating) AS AvgOverallRating
FROM airlines_reviews
GROUP BY airline, class, TypeofTraveller
ORDER BY airline, class, TypeofTraveller;

SELECT 
    airline AS Airline,
    recommended AS Recommendation,
    class AS Class,
    TypeofTraveller,
    MIN(OverallRating) AS MinOverallRating,
    MAX(OverallRating) AS MaxOverallRating
FROM airlines_reviews
GROUP BY recommended, airline, class, TypeofTraveller
ORDER BY airline, recommended DESC, class, TypeofTraveller;


/* Step 3a: Building the MySQL Code */
WITH RECURSIVE numbers AS (
    SELECT 1 AS n
    UNION ALL
    SELECT n + 1 FROM numbers WHERE n < 200
)
SELECT word, COUNT(*) AS frequency
FROM (
    SELECT LOWER(TRIM(BOTH '.' FROM TRIM(BOTH ',' FROM TRIM(BOTH '!' FROM TRIM(BOTH '?' FROM LOWER(SUBSTRING_INDEX(SUBSTRING_INDEX(Title, ' ', n), ' ', -1))))))) AS word
    FROM airlines_reviews
    JOIN numbers ON n <= LENGTH(Reviews) - LENGTH(REPLACE(Reviews, ' ', '')) + 1
) AS words
WHERE word NOT IN ('a', 'about', 'above', 'after', 'again', 'against', 'all', 'am', 'an', 'and', 'any', 'are', 'aren', 'aren\'t', 'as', 'at', 'be', 'because', 'been', 'before', 'being', 'below', 'between', 'both', 'but', 'by', 'can', 'can\'t', 'cannot', 'could', 'couldn\'t', 'couldn\'t', 'did', 'didn\'t', 'do', 'does', 'doesn\'t', 'doing', 'don\'t', 'down', 'during', 'each', 'few', 'for', 'from', 'further', 'had', 'hadn\'t', 'has', 'hasn\'t', 'haven\'t', 'having', 'he', 'he\'s', 'her', 'here', 'here\'s', 'hereafter', 'hereby', 'herein', 'hereof', 'hereon', 'hers', 'herself', 'him', 'himself', 'his', 'how', 'how\'s', 'howsoever', 'i', 'i\'m', 'i\'ve', 'i\'ll', 'i\'d', 'i\'ve', 'if', 'in', 'insofar', 'into', 'is', 'isn\'t', 'is\'t', 'is\'re', 'is\'ve', 'it', 'it\'s', 'it\'ll', 'it\'d', 'it\'ve', 'itself', 'let', 'let\'s', 'me', 'more', 'moreover', 'most', 'much', 'must', 'mustn\'t', 'my', 'myself', 'myself', 'must', 'myself', 'no', 'nor', 'not', 'of', 'off', 'on', 'once', 'only', 'onto', 'or', 'other', 'ought', 'our', 'ours', 'ourselves', 'out', 'outside', 'over', 'own', 'same', 'shall', 'shan\'t', 'she', 'she\'s', 'should', 'shouldn\'t', 'should\'ve', 'so', 'some', 'so', 'such', 'than', 'that', 'that\'s', 'that\'ll', 'that\'d', 'that\'ve', 'the', 'theirs', 'theirselves', 'them', 'themself', 'themselves', 'then', 'there', 'there\'s', 'thereafter', 'therefore', 'therein', 'thereof', 'thereon', 'these', 'they', 'they\'re', 'they\'ve', 'this', 'this\'s', 'this\'ll', 'those', 'though', 'through', 'throughout', 'to', 'too', 'under', 'until', 'up', 'us', 'very', 'was', 'wasn\'t', 'weren\'t', 'we', 'we\'ve', 'we\'ll', 'we\'d', 'we\'re', 'we\'ve', 'well', 'we\'ve', 'what', 'what\'s', 'whatever', 'when', 'when\'s', 'whenever', 'where', 'where\'s', 'wherever', 'which', 'which\'s', 'whichever', 'while', 'while\'s', 'who', 'who\'s', 'whoever', 'whom', 'whom\'s', 'whomever', 'whose', 'why', 'why\'s', 'whyever', 'with', 'withhold', 'within', 'without', 'you', 'you\'re', 'you\'ve', 'you\'ll', 'you\'d', 'you\'ve', 'your', 'yours', 'yourself', 'yourselves', '')
GROUP BY word
HAVING frequency >= 236  -- Filter words with frequency 15% or higher
ORDER BY frequency DESC;

CREATE TABLE sentiment_dict (
    word VARCHAR(255),
    sentiment ENUM('positive', 'negative')
);

INSERT INTO sentiment_dict (word, sentiment)
VALUES    ('excellent', 'positive'),
    ('amazing', 'positive'),    ('good', 'positive'),
    ('fantastic', 'positive'),    ('helpful', 'positive'),
    ('friendly', 'positive'),    ('exceptional', 'positive'),
    ('attentive', 'positive'),    ('superb', 'positive'),
    ('better', 'positive'),    ('pleasant', 'positive'),
    ('perfect', 'positive'),    ('impressive', 'positive'),
    ('great', 'positive'),    ('improved', 'positive'),
    ('professional', 'positive'),    ('smile', 'positive'),
    ('caring', 'positive'),    ('efficient', 'positive'),
    ('comfortable', 'positive'),    ('enjoyable', 'positive'),
    ('impeccable', 'positive'),    ('top-notch', 'positive'),
    ('upgraded', 'positive'),    ('impressed', 'positive'),
    ('outstanding', 'positive'),    ('beyond', 'positive');



-- Sub-step 1a: Normalize routes in airlines_reviews
WITH AirlineReviewsRoutes AS (
    SELECT 
        ar.Route AS OriginalRoute,
        CONCAT(
            LEAST(origin_airport.Code, destination_airport.Code), 
            GREATEST(origin_airport.Code, destination_airport.Code)
        ) AS StandardizedRoute,
        ar.TypeofTraveller,
        ar.Title,
        ar.Reviews,
        ar.Class,
        ar.SeatComfort,
        ar.StaffService,
        ar.FoodnBeverages,
        ar.InflightEntertainment,
        ar.ValueForMoney,
        ar.OverallRating,
        ar.Recommended,
        ar.Verified,
        ar.MonthFlown,
        STR_TO_DATE(ar.ReviewDate, '%e/%c/%Y') AS ParsedReviewDate,
        ROW_NUMBER() OVER (
            PARTITION BY CONCAT(
                LEAST(origin_airport.Code, destination_airport.Code), 
                GREATEST(origin_airport.Code, destination_airport.Code)
            ), ar.TypeofTraveller 
            ORDER BY ar.Recommended DESC, ar.OverallRating DESC
        ) AS ReviewRowNum
    FROM 
        airlines_reviews ar
    INNER JOIN airport_codes origin_airport 
        ON ar.Route LIKE CONCAT('%', origin_airport.City, '%') 
        OR ar.Route LIKE CONCAT('%', origin_airport.Code, '%')
    INNER JOIN airport_codes destination_airport 
        ON ar.Route LIKE CONCAT('%', destination_airport.City, '%') 
        OR ar.Route LIKE CONCAT('%', destination_airport.Code, '%')
    WHERE 
        origin_airport.Code IS NOT NULL 
        AND destination_airport.Code IS NOT NULL
),

-- Sub-step 1b: Normalize routes in customer_booking
CustomerBookingRoutes AS (
    SELECT 
        cb.route AS OriginalRoute,
        CONCAT(
            LEAST(SUBSTRING(cb.route, 1, 3), SUBSTRING(cb.route, 4, 3)),
            GREATEST(SUBSTRING(cb.route, 1, 3), SUBSTRING(cb.route, 4, 3))
        ) AS StandardizedRoute,
        CASE
            WHEN cb.num_passengers = 1 AND cb.length_of_stay <= 7 THEN 'Business'
            WHEN cb.num_passengers = 1 AND cb.length_of_stay > 7 THEN 'Solo Leisure'
            WHEN cb.num_passengers = 2 THEN 'Couple Leisure'
            WHEN cb.num_passengers >= 3 THEN 'Family Leisure'
        END AS TypeofTraveller,
        cb.Flight_Hour,
        cb.Flight_Duration,
        cb.Purchase_Lead,
        ROW_NUMBER() OVER (
            PARTITION BY CONCAT(
                LEAST(SUBSTRING(cb.route, 1, 3), SUBSTRING(cb.route, 4, 3)),
                GREATEST(SUBSTRING(cb.route, 1, 3), SUBSTRING(cb.route, 4, 3))
            ), 
            CASE
                WHEN cb.num_passengers = 1 AND cb.length_of_stay <= 7 THEN 'Business'
                WHEN cb.num_passengers = 1 AND cb.length_of_stay > 7 THEN 'Solo Leisure'
                WHEN cb.num_passengers = 2 THEN 'Couple Leisure'
                WHEN cb.num_passengers >= 3 THEN 'Family Leisure'
            END ORDER BY cb.Purchase_Lead ASC
        ) AS BookingRowNum
    FROM 
        customer_booking cb
),
-- Sub-step 2: Allocate reviews to bookings based on Traveller Type and Route matches
MatchedData AS (
    SELECT 
        cb.StandardizedRoute AS Route,
        cb.TypeofTraveller,
        cb.Flight_Hour,
        cb.Flight_Duration,
        cb.Purchase_Lead,
        ar.Title,
        ar.Reviews,
        ar.Class,
        ar.SeatComfort,
        ar.StaffService,
        ar.FoodnBeverages,
        ar.InflightEntertainment,
        ar.ValueForMoney,
        ar.OverallRating,
        ar.Recommended,
        ar.Verified,
        ar.MonthFlown,
        ar.ParsedReviewDate
    FROM 
        CustomerBookingRoutes cb
    INNER JOIN AirlineReviewsRoutes ar
        ON cb.StandardizedRoute = ar.StandardizedRoute
        AND cb.TypeofTraveller = ar.TypeofTraveller
        AND cb.BookingRowNum = ar.ReviewRowNum
),
-- Sub-step 3: Add Sentiment Analysis
-- Special "Un-" prefix handling because using LIKE flags "Unpleasant" as both negative and positive ("Pleasant")
SentimentAnalysis AS (
    SELECT 
        md.*,
        GROUP_CONCAT(DISTINCT CASE 
            WHEN sd.word LIKE CONCAT('un', sd_base.word) THEN sd.word -- Prioritize "un-" prefixed word
            WHEN sd_base.word LIKE CONCAT('un', sd.word) THEN sd_base.word
            ELSE sd.word
        END) AS AdjustedSentimentWords,
        SUM(CASE 
            WHEN sd.word LIKE CONCAT('un', sd_base.word) THEN -1
            WHEN sd_base.word LIKE CONCAT('un', sd.word) THEN -1
            WHEN sd.sentiment = 'positive' THEN 1
            WHEN sd.sentiment = 'negative' THEN -1
            ELSE 0
        END) AS AdjustedSentimentScore
    FROM MatchedData md
    LEFT JOIN sentiment_dict sd 
        ON LOWER(md.Reviews) LIKE CONCAT('%', sd.word, '%')
    LEFT JOIN sentiment_dict sd_base 
        ON sd.word LIKE CONCAT('un', sd_base.word) OR sd_base.word LIKE CONCAT('un', sd.word)
    GROUP BY 
        md.Route, 
        md.TypeofTraveller, 
        md.Flight_Hour, 
        md.Flight_Duration, 
        md.Purchase_Lead, 
        md.Title, 
        md.Reviews, 
        md.Class, 
        md.SeatComfort, 
        md.StaffService, 
        md.FoodnBeverages, 
        md.InflightEntertainment, 
        md.ValueForMoney, 
        md.OverallRating, 
        md.Recommended, 
        md.Verified, 
        md.MonthFlown, 
        md.ParsedReviewDate
),
-- Sub-step 4: Add Relevance Score
RelevanceScores AS (
    SELECT 
        sa.*,
        CASE 
            WHEN TIMESTAMPDIFF(MONTH, STR_TO_DATE(CONCAT('01-', sa.MonthFlown), '%d-%b-%y'), sa.ParsedReviewDate) = 0 THEN 1
            ELSE TIMESTAMPDIFF(MONTH, STR_TO_DATE(CONCAT('01-', sa.MonthFlown), '%d-%b-%y'), sa.ParsedReviewDate)
        END AS TimeDifference,
        CASE 
            WHEN TIMESTAMPDIFF(MONTH, STR_TO_DATE(CONCAT('01-', sa.MonthFlown), '%d-%b-%y'), sa.ParsedReviewDate) >= 0 
                THEN CASE 
                        WHEN sa.Verified = 'TRUE' THEN 1 
                        ELSE 0.5 
                     END / 
                     CASE 
                        WHEN TIMESTAMPDIFF(MONTH, STR_TO_DATE(CONCAT('01-', sa.MonthFlown), '%d-%b-%y'), sa.ParsedReviewDate) = 0 THEN 1
                        ELSE TIMESTAMPDIFF(MONTH, STR_TO_DATE(CONCAT('01-', sa.MonthFlown), '%d-%b-%y'), sa.ParsedReviewDate)
                     END
            ELSE NULL
        END AS RelevanceScore
    FROM SentimentAnalysis sa
),
DeduplicatedData AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY Title ORDER BY RelevanceScore DESC) AS TitleRowNum
    FROM RelevanceScores
)
-- Final Output: Select first entry for each Title
SELECT 
    Route,
    TypeofTraveller,
    Flight_Hour,
    Flight_Duration,
    Purchase_Lead,
    Title,
    Reviews,
    ParsedReviewDate As ReviewDate,
    MonthFlown,
    Class,
    SeatComfort,
    StaffService,
    FoodnBeverages,
    InflightEntertainment,
    ValueForMoney,
    OverallRating,
    Recommended,
    RelevanceScore,
    AdjustedSentimentWords AS SentimentWords,
    AdjustedSentimentScore AS SentimentScore
FROM DeduplicatedData
WHERE TitleRowNum = 1
ORDER BY Route, TypeofTraveller, RelevanceScore DESC;