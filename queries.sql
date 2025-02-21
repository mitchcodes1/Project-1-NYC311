-- NYC 311 Data Analysis SQL Queries

-- Complaints by Year and Month (Seasonality Analysis)
SELECT 
    YEAR(created_date) AS year, 
    MONTH(created_date) AS month, 
    COUNT(*) AS complaint_count
FROM service_requests
GROUP BY year, month
ORDER BY year, month;

-- Top 10 Complaint Types
SELECT 
    complaint_type, 
    COUNT(*) AS complaint_count
FROM service_requests
GROUP BY complaint_type
ORDER BY complaint_count DESC
LIMIT 10;

-- Complaints by Borough
SELECT 
    borough, 
    COUNT(*) AS complaint_count
FROM service_requests
GROUP BY borough
ORDER BY complaint_count DESC;

-- Year-over-Year Complaint Trends (LAG)
WITH YearlyComplaints AS (
    SELECT
        YEAR(created_date) AS year,
        MONTH(created_date) AS month,
        COUNT(*) AS complaint_count,
        LAG(COUNT(*)) OVER (PARTITION BY MONTH(created_date) ORDER BY YEAR(created_date)) AS prev_year_count
    FROM service_requests
    GROUP BY year, month
)
SELECT
    year,
    month,
    complaint_count,
    prev_year_count,
    ROUND((complaint_count - prev_year_count) / prev_year_count * 100, 2) AS pct_change
FROM YearlyComplaints
WHERE prev_year_count IS NOT NULL
ORDER BY year, month;


-- Brooklyn vs. Bronx Complaint Type Comparison (Ensuring Top 10 for Each Borough)
WITH ComplaintStats AS (
    SELECT
        complaint_type,
        borough,
        COUNT(*) AS complaint_count,
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY borough) AS pct_of_borough_complaints
    FROM service_requests
    WHERE borough IN ('Bronx', 'Brooklyn')
    GROUP BY complaint_type, borough
)
SELECT * FROM (
    (SELECT * FROM ComplaintStats WHERE borough = 'Bronx' ORDER BY complaint_count DESC LIMIT 10)
    UNION ALL
    (SELECT * FROM ComplaintStats WHERE borough = 'Brooklyn' ORDER BY complaint_count DESC LIMIT 10)
) AS combined_results
ORDER BY borough, complaint_count DESC;
