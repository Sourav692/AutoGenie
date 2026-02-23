SELECT
  u.name AS rep_name,
  COUNT(o.opportunityid) AS deal_count,
  SUM(o.amount) AS total_amount,
  AVG(o.amount) AS avg_deal_size
FROM vc_catalog.aibi_sales_pipeline_review.opportunity o
JOIN vc_catalog.aibi_sales_pipeline_review.user u
  ON o.ownerid = u.id
GROUP BY u.name
ORDER BY total_amount DESC
LIMIT 10
