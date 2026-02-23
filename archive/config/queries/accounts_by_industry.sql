SELECT
  industry,
  COUNT(*) AS account_count,
  SUM(annualrevenue) AS total_revenue,
  AVG(annualrevenue) AS avg_revenue
FROM vc_catalog.aibi_sales_pipeline_review.accounts
GROUP BY industry
ORDER BY account_count DESC
LIMIT 15
