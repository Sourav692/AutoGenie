SELECT
  region__c AS region,
  COUNT(*) AS account_count,
  SUM(annualrevenue) AS total_revenue
FROM vc_catalog.aibi_sales_pipeline_review.accounts
WHERE region__c IS NOT NULL
GROUP BY region__c
ORDER BY total_revenue DESC
