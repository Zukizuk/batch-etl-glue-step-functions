INSERT INTO presentation.avg_listing_price_weekly (year, week, weekly_avg_price)
SELECT 
    DATE_PART('year', listing_created_on) AS year,
    DATE_PART('week', listing_created_on) AS week,
    AVG(price) AS weekly_avg_price
FROM curated.listings
WHERE is_active = 1
GROUP BY DATE_PART('year', listing_created_on), DATE_PART('week', listing_created_on)
ORDER BY year, week;

INSERT INTO presentation.occupancy_rate_monthly (year, month, booked_nights, total_bookings, occupancy_rate)
SELECT 
    DATE_PART('year', booking_date) AS year,
    DATE_PART('month', booking_date) AS month,
    SUM(ABS(DATEDIFF(day, checkin_date, checkout_date))) AS booked_nights,
    COUNT(*) AS total_bookings,
    (SUM(ABS(DATEDIFF(day, checkin_date, checkout_date))) / 30.0) AS occupancy_rate
FROM curated.bookings
WHERE booking_status = 'confirmed'
GROUP BY DATE_PART('year', booking_date), DATE_PART('month', booking_date)
ORDER BY year, month;

INSERT INTO presentation.most_popular_locations_weekly (year, week, cityname, booking_count)
SELECT 
    DATE_PART('year', b.booking_date) AS year,
    DATE_PART('week', b.booking_date) AS week,
    a.cityname,
    COUNT(*) AS booking_count
FROM curated.bookings b
JOIN curated.apartments a ON b.apartment_id = a.id
WHERE b.booking_status = 'confirmed'
GROUP BY DATE_PART('year', b.booking_date), DATE_PART('week', b.booking_date), a.cityname
ORDER BY year, week, booking_count DESC;

INSERT INTO presentation.top_performing_listings_weekly (year, week, apartment_id, total_revenue)
SELECT 
    DATE_PART('year', booking_date) AS year,
    DATE_PART('week', booking_date) AS week,
    apartment_id,
    SUM(total_price) AS total_revenue
FROM curated.bookings
WHERE booking_status = 'confirmed'
GROUP BY DATE_PART('year', booking_date), DATE_PART('week', booking_date), apartment_id
ORDER BY year, week, total_revenue DESC;

INSERT INTO presentation.total_bookings_per_user (user_id, total_bookings, confirmed_bookings)
SELECT 
    user_id, 
    COUNT(*) AS total_bookings,
    COUNT(CASE WHEN booking_status = 'confirmed' THEN 1 END) AS confirmed_bookings
FROM curated.bookings
GROUP BY user_id;

INSERT INTO presentation.avg_booking_duration (avg_stay_duration_days)
SELECT 
    AVG(ABS(checkout_date - checkin_date)) AS avg_stay_duration_days
FROM curated.bookings
WHERE booking_status = 'confirmed';

INSERT INTO presentation.repeat_customer_rate (repeat_customer_rate)
WITH bookings_with_prev AS (
    SELECT 
        user_id,
        booking_date,
        LAG(booking_date) OVER (PARTITION BY user_id ORDER BY booking_date) AS prev_booking_date
    FROM curated.bookings
    WHERE booking_status = 'confirmed'
),
repeat_customers AS (
    SELECT 
        user_id
    FROM bookings_with_prev
    WHERE (booking_date - prev_booking_date) <= 30
    AND prev_booking_date IS NOT NULL  
    GROUP BY user_id
)
SELECT 
    ((SELECT COUNT(*) FROM repeat_customers)::float / 
     (SELECT COUNT(DISTINCT user_id) FROM curated.bookings WHERE booking_status = 'confirmed')) * 100 AS repeat_customer_rate;