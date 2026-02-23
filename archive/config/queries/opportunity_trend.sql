SELECT
  DATE_TRUNC('month', createddate) AS month,
  COUNT(*) AS num_opportunities,
  SUM(amount) AS total_amount
FROM vc_catalog.aibi_sales_pipeline_review.opportunity
WHERE createddate >= CURRENT_DATE - INTERVAL 365 DAY
GROUP BY DATE_TRUNC('month', createddate)
ORDER BY month
