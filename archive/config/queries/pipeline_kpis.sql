SELECT
  COUNT(*) AS total_opportunities,
  SUM(amount) AS total_pipeline,
  AVG(amount) AS avg_deal_size,
  SUM(CASE WHEN stagename = 'Closed Won' THEN 1 ELSE 0 END) AS closed_won,
  SUM(CASE WHEN stagename = 'Closed Lost' THEN 1 ELSE 0 END) AS closed_lost,
  SUM(CASE WHEN stagename NOT IN ('Closed Won', 'Closed Lost') THEN amount ELSE 0 END) AS open_pipeline,
  SUM(amount * probability / 100) AS weighted_pipeline
FROM vc_catalog.aibi_sales_pipeline_review.opportunity
