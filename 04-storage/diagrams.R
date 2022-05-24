DiagrammeR::mermaid("
graph LR
  import_ga4-->tidy_ga4
  tidy_ga4-->operations_dashboard
  import_crm-->tidy_crm
  tidy_crm-->join_data
  tidy_ga4-->join_data
  join_data-->marketing_data
  join_data-->sales_data
  join_data-->retention_data
  marketing_data-->web_enrichment
  web_enrichment-->user_api
  marketing_data-->marketing_dashboard_data
  marketing_data-->csuite_dashboard
  sales_data-->csuite_dashboard
  retention_data-->csuite_dashboard
")

