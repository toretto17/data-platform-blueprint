# Business Rules

Document your domain's business logic here (per mart / model). Template:

## <Table/Model Name>
- **Grain:** one row per (entity, date)
- **Key metrics:** X = SUM(daily_value) grouped by ...
- **Period aggregates:** MTD = cumulative SUM within month; YTD = since Jan 1
- **Rate formula:** rate = numerator / NULLIF(denominator, 0) — NOT SUM of daily rates
- **Zero-fill:** active entities with no data in a day get 0 (not excluded)
- **History:** SCD Type 2 for dimensions; append for facts
