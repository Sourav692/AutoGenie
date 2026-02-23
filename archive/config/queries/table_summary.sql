SELECT 'accounts' AS table_name, COUNT(*) AS row_count FROM vc_catalog.aibi_sales_pipeline_review.accounts
UNION ALL
SELECT 'opportunity', COUNT(*) FROM vc_catalog.aibi_sales_pipeline_review.opportunity
UNION ALL
SELECT 'opportunityhistory_cube', COUNT(*) FROM vc_catalog.aibi_sales_pipeline_review.opportunityhistory_cube
UNION ALL
SELECT 'user', COUNT(*) FROM vc_catalog.aibi_sales_pipeline_review.user
