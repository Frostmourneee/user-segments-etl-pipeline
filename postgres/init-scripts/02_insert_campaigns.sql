INSERT INTO marketing_campaigns (campaign_name, start_date, end_date) VALUES
('Summer Sale 2025', '2025-06-01', '2025-08-31'),
('Black Friday 2025', '2025-11-25', '2025-11-30'),
('New Year Campaign', '2025-12-15', '2026-01-15'),
('Spring Collection', '2025-03-01', '2025-05-31')
ON CONFLICT DO NOTHING;