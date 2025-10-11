-- Seed Los Angeles ZIP codes with density classifications
-- This data is used for intelligent campaign coverage selection

-- Very High Density ZIP codes (Downtown, Beverly Hills, Hollywood)
INSERT INTO gmaps_scraper.zip_codes (zip_code, city, state, neighborhood, density_level, expected_businesses) VALUES
-- Downtown LA & Financial District
('90012', 'Los Angeles', 'CA', 'Downtown LA', 'very_high', 450),
('90013', 'Los Angeles', 'CA', 'Downtown LA', 'very_high', 425),
('90014', 'Los Angeles', 'CA', 'Downtown LA', 'very_high', 400),
('90015', 'Los Angeles', 'CA', 'Downtown LA', 'very_high', 380),
('90017', 'Los Angeles', 'CA', 'Downtown LA', 'very_high', 420),
('90071', 'Los Angeles', 'CA', 'Downtown LA', 'very_high', 390),
-- Beverly Hills & West LA
('90210', 'Los Angeles', 'CA', 'Beverly Hills', 'very_high', 380),
('90211', 'Los Angeles', 'CA', 'Beverly Hills', 'very_high', 350),
('90212', 'Los Angeles', 'CA', 'Beverly Hills', 'very_high', 360),
('90024', 'Los Angeles', 'CA', 'Westwood', 'very_high', 340),
('90025', 'Los Angeles', 'CA', 'West LA', 'very_high', 320),
('90064', 'Los Angeles', 'CA', 'West LA', 'very_high', 310),
-- Hollywood
('90028', 'Los Angeles', 'CA', 'Hollywood', 'very_high', 390),
('90038', 'Los Angeles', 'CA', 'Hollywood', 'very_high', 340),
('90046', 'Los Angeles', 'CA', 'West Hollywood', 'very_high', 330),
('90048', 'Los Angeles', 'CA', 'West Hollywood', 'very_high', 320),
('90069', 'Los Angeles', 'CA', 'West Hollywood', 'very_high', 310),
-- Century City & Culver City
('90067', 'Los Angeles', 'CA', 'Century City', 'very_high', 380),
('90230', 'Los Angeles', 'CA', 'Culver City', 'very_high', 300),
('90232', 'Los Angeles', 'CA', 'Culver City', 'very_high', 290)
ON CONFLICT (zip_code) DO UPDATE SET
    density_level = EXCLUDED.density_level,
    expected_businesses = EXCLUDED.expected_businesses,
    neighborhood = EXCLUDED.neighborhood;

-- High Density ZIP codes (Santa Monica, Venice, Koreatown, etc.)
INSERT INTO gmaps_scraper.zip_codes (zip_code, city, state, neighborhood, density_level, expected_businesses) VALUES
-- Santa Monica & Venice
('90401', 'Los Angeles', 'CA', 'Santa Monica', 'high', 320),
('90402', 'Los Angeles', 'CA', 'Santa Monica', 'high', 280),
('90403', 'Los Angeles', 'CA', 'Santa Monica', 'high', 290),
('90404', 'Los Angeles', 'CA', 'Santa Monica', 'high', 270),
('90405', 'Los Angeles', 'CA', 'Santa Monica', 'high', 260),
('90291', 'Los Angeles', 'CA', 'Venice', 'high', 280),
('90292', 'Los Angeles', 'CA', 'Marina del Rey', 'high', 270),
-- Brentwood & Pacific Palisades
('90049', 'Los Angeles', 'CA', 'Brentwood', 'high', 250),
('90272', 'Los Angeles', 'CA', 'Pacific Palisades', 'high', 240),
-- Mid-City
('90004', 'Los Angeles', 'CA', 'Hancock Park', 'high', 270),
('90005', 'Los Angeles', 'CA', 'Koreatown', 'high', 290),
('90006', 'Los Angeles', 'CA', 'Koreatown', 'high', 280),
('90019', 'Los Angeles', 'CA', 'Mid-City', 'high', 260),
('90020', 'Los Angeles', 'CA', 'Koreatown', 'high', 275),
('90036', 'Los Angeles', 'CA', 'Miracle Mile', 'high', 285),
-- Silver Lake & Los Feliz
('90026', 'Los Angeles', 'CA', 'Silver Lake', 'high', 250),
('90027', 'Los Angeles', 'CA', 'Los Feliz', 'high', 260),
('90029', 'Los Angeles', 'CA', 'Los Feliz', 'high', 245),
('90039', 'Los Angeles', 'CA', 'Silver Lake', 'high', 240),
-- Studio City & Sherman Oaks
('91604', 'Los Angeles', 'CA', 'Studio City', 'high', 265),
('91403', 'Los Angeles', 'CA', 'Sherman Oaks', 'high', 255),
('91423', 'Los Angeles', 'CA', 'Sherman Oaks', 'high', 250),
-- Pasadena Area
('91101', 'Los Angeles', 'CA', 'Pasadena', 'high', 280),
('91103', 'Los Angeles', 'CA', 'Pasadena', 'high', 260),
('91105', 'Los Angeles', 'CA', 'Pasadena', 'high', 250)
ON CONFLICT (zip_code) DO UPDATE SET
    density_level = EXCLUDED.density_level,
    expected_businesses = EXCLUDED.expected_businesses,
    neighborhood = EXCLUDED.neighborhood;

-- Medium Density ZIP codes (South LA, East LA, Valley)
INSERT INTO gmaps_scraper.zip_codes (zip_code, city, state, neighborhood, density_level, expected_businesses) VALUES
-- South LA
('90001', 'Los Angeles', 'CA', 'Florence', 'medium', 220),
('90002', 'Los Angeles', 'CA', 'Watts', 'medium', 210),
('90003', 'Los Angeles', 'CA', 'Southeast LA', 'medium', 215),
('90007', 'Los Angeles', 'CA', 'South LA', 'medium', 225),
('90008', 'Los Angeles', 'CA', 'Baldwin Hills', 'medium', 230),
('90016', 'Los Angeles', 'CA', 'West LA', 'medium', 235),
('90018', 'Los Angeles', 'CA', 'Jefferson Park', 'medium', 220),
('90037', 'Los Angeles', 'CA', 'South LA', 'medium', 205),
('90043', 'Los Angeles', 'CA', 'Hyde Park', 'medium', 200),
('90044', 'Los Angeles', 'CA', 'Athens', 'medium', 195),
('90047', 'Los Angeles', 'CA', 'South LA', 'medium', 190),
('90062', 'Los Angeles', 'CA', 'South LA', 'medium', 185),
-- East LA
('90022', 'Los Angeles', 'CA', 'East LA', 'medium', 210),
('90023', 'Los Angeles', 'CA', 'East LA', 'medium', 200),
('90031', 'Los Angeles', 'CA', 'Lincoln Heights', 'medium', 195),
('90032', 'Los Angeles', 'CA', 'El Sereno', 'medium', 190),
('90033', 'Los Angeles', 'CA', 'Boyle Heights', 'medium', 205),
('90063', 'Los Angeles', 'CA', 'East LA', 'medium', 185),
-- North Hollywood & Van Nuys
('91601', 'Los Angeles', 'CA', 'North Hollywood', 'medium', 230),
('91602', 'Los Angeles', 'CA', 'North Hollywood', 'medium', 220),
('91605', 'Los Angeles', 'CA', 'North Hollywood', 'medium', 210),
('91401', 'Los Angeles', 'CA', 'Van Nuys', 'medium', 225),
('91405', 'Los Angeles', 'CA', 'Van Nuys', 'medium', 215),
('91406', 'Los Angeles', 'CA', 'Van Nuys', 'medium', 210),
-- Burbank & Glendale
('91502', 'Los Angeles', 'CA', 'Burbank', 'medium', 240),
('91505', 'Los Angeles', 'CA', 'Burbank', 'medium', 230),
('91201', 'Los Angeles', 'CA', 'Glendale', 'medium', 235),
('91202', 'Los Angeles', 'CA', 'Glendale', 'medium', 225),
('91203', 'Los Angeles', 'CA', 'Glendale', 'medium', 220)
ON CONFLICT (zip_code) DO UPDATE SET
    density_level = EXCLUDED.density_level,
    expected_businesses = EXCLUDED.expected_businesses,
    neighborhood = EXCLUDED.neighborhood;

-- Low Density ZIP codes (Residential areas)
INSERT INTO gmaps_scraper.zip_codes (zip_code, city, state, neighborhood, density_level, expected_businesses) VALUES
-- Residential areas with fewer businesses
('90041', 'Los Angeles', 'CA', 'Eagle Rock', 'low', 180),
('90042', 'Los Angeles', 'CA', 'Highland Park', 'low', 175),
('90045', 'Los Angeles', 'CA', 'Westchester', 'low', 170),
('90056', 'Los Angeles', 'CA', 'Ladera Heights', 'low', 160),
('90057', 'Los Angeles', 'CA', 'Westlake', 'low', 165),
('90059', 'Los Angeles', 'CA', 'South LA', 'low', 155),
('90061', 'Los Angeles', 'CA', 'South LA', 'low', 150),
('90065', 'Los Angeles', 'CA', 'Mount Washington', 'low', 145),
('90066', 'Los Angeles', 'CA', 'Mar Vista', 'low', 185),
('90068', 'Los Angeles', 'CA', 'Hollywood Hills', 'low', 140),
-- San Fernando Valley residential
('91302', 'Los Angeles', 'CA', 'Calabasas', 'low', 175),
('91306', 'Los Angeles', 'CA', 'Winnetka', 'low', 165),
('91311', 'Los Angeles', 'CA', 'Chatsworth', 'low', 170),
('91316', 'Los Angeles', 'CA', 'Encino', 'low', 180),
('91324', 'Los Angeles', 'CA', 'Northridge', 'low', 160),
('91325', 'Los Angeles', 'CA', 'Northridge', 'low', 155),
('91331', 'Los Angeles', 'CA', 'Pacoima', 'low', 150),
('91335', 'Los Angeles', 'CA', 'Reseda', 'low', 165),
('91342', 'Los Angeles', 'CA', 'Sylmar', 'low', 145),
('91343', 'Los Angeles', 'CA', 'North Hills', 'low', 155),
('91344', 'Los Angeles', 'CA', 'Granada Hills', 'low', 160),
('91352', 'Los Angeles', 'CA', 'Sun Valley', 'low', 140),
('91356', 'Los Angeles', 'CA', 'Tarzana', 'low', 175),
('91364', 'Los Angeles', 'CA', 'Woodland Hills', 'low', 180),
('91367', 'Los Angeles', 'CA', 'Woodland Hills', 'low', 170),
('91402', 'Los Angeles', 'CA', 'Panorama City', 'low', 150)
ON CONFLICT (zip_code) DO UPDATE SET
    density_level = EXCLUDED.density_level,
    expected_businesses = EXCLUDED.expected_businesses,
    neighborhood = EXCLUDED.neighborhood;