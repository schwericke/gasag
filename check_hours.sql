SELECT Kraftwerk, date, total_hours, CASE WHEN total_hours != 24 THEN 'NOT 24' ELSE 'OK' END as status FROM `gasag-465208.analysis.daily_metrics` WHERE total_hours != 24 ORDER BY Kraftwerk, date;
