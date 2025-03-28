-- Create Schema
CREATE SCHEMA IF NOT EXISTS raws;

-- Apartments table
CREATE TABLE IF NOT EXISTS raws.apartments (
    id INTEGER,
    category VARCHAR(50),
    body VARCHAR(255),
    amenities VARCHAR(255),
    bathrooms INTEGER,
    bedrooms INTEGER,
    fee DECIMAL(10,2),
    has_photo BOOLEAN,
    pets_allowed BOOLEAN,
    price_display VARCHAR(20),
    price_type VARCHAR(20),
    square_feet INTEGER,
    address VARCHAR(255),
    cityname VARCHAR(100),
    state VARCHAR(100),
    latitude DECIMAL(10,6),
    longitude DECIMAL(10,6)
);

-- User Views table
CREATE TABLE IF NOT EXISTS raws.user_views (
    user_id INTEGER,
    apartment_id INTEGER,
    viewed_at VARCHAR(20),
    is_wishlisted BOOLEAN,
    call_to_action VARCHAR(50)
);

-- Bookings table
CREATE TABLE IF NOT EXISTS raws.bookings (
    booking_id INTEGER,
    user_id INTEGER,
    apartment_id INTEGER,
    booking_date VARCHAR(20),
    checkin_date VARCHAR(20),
    checkout_date VARCHAR(20),
    total_price DECIMAL(10,2),
    currency VARCHAR(10),
    booking_status VARCHAR(20)
);

-- Listings table
CREATE TABLE IF NOT EXISTS raws.listings (
    id INTEGER,
    title VARCHAR(100),
    source VARCHAR(50),
    price DECIMAL(10,2),
    currency VARCHAR(10),
    listing_created_on VARCHAR(20),
    is_active BOOLEAN,
    last_modified_timestamp VARCHAR(20)
);