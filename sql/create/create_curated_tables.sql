CREATE SCHEMA IF NOT EXISTS curated;

-- Apartments table
CREATE TABLE IF NOT EXISTS curated.apartments (
    id BIGINT,
    amenities VARCHAR(255),
    bathrooms BIGINT,
    bedrooms BIGINT,
    fee DOUBLE PRECISION,
    price_type VARCHAR(255),
    cityname VARCHAR(255)
);

-- User Views table
CREATE TABLE IF NOT EXISTS curated.user_views (
    user_id BIGINT,
    apartment_id BIGINT
);

-- Bookings table
CREATE TABLE IF NOT EXISTS curated.bookings (
    booking_id BIGINT,
    user_id BIGINT,
    apartment_id BIGINT,
    booking_date DATE,
    checkin_date DATE,
    checkout_date DATE,
    total_price DOUBLE PRECISION,
    currency VARCHAR(255),
    booking_status VARCHAR(255)
);

-- Listings table
CREATE TABLE IF NOT EXISTS curated.listings (
    id BIGINT,
    title VARCHAR(255),
    price DOUBLE PRECISION,
    listing_created_on DATE,
    is_active BIGINT,
    last_modified_timestamp DATE
);