SELECT
  stagename,
  COUNT(*) AS deal_count,
  SUM(amount) AS total_amount,
  AVG(amount) AS avg_amount
FROM vc_catalog.aibi_sales_pipeline_review.opportunity
GROUP BY stagename
ORDER BY total_amount DESC
