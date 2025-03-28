CREATE TABLE IF NOT EXISTS presentation.avg_listing_price_weekly (
    year INTEGER,
    week INTEGER,
    weekly_avg_price DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS presentation.occupancy_rate_monthly (
    year INTEGER,
    month INTEGER,
    booked_nights BIGINT,
    total_bookings BIGINT,
    occupancy_rate DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS presentation.most_popular_locations_weekly (
    year INTEGER,
    week INTEGER,
    cityname VARCHAR(255),
    booking_count BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS presentation.top_performing_listings_weekly (
    year INTEGER,
    week INTEGER,
    apartment_id BIGINT,
    total_revenue DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS presentation.total_bookings_per_user (
    user_id BIGINT,
    total_bookings BIGINT,
    confirmed_bookings BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS presentation.avg_booking_duration (
    avg_stay_duration_days DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS presentation.repeat_customer_rate (
    repeat_customer_rate DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);