SELECT
  dealstage,
  SUM(sumdealamount) AS total_deal_amount,
  COUNT(*) AS record_count
FROM vc_catalog.aibi_sales_pipeline_review.opportunityhistory_cube
GROUP BY dealstage
ORDER BY total_deal_amount DESC
