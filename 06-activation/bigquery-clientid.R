library(bigQueryR)
options(googleAuthR.scopes.selected = "https://www.googleapis.com/auth/cloud-platform")
googleAuthR::gar_gce_auth()

# the GA4 dataset
bqr_global_project("mark-edmondson-gde")
bqr_global_dataset("analytics_206670707")

query_client_id <- function(client_id, sql_file){
  
  # read in SQL file and interpolate client_id
  sql <- readChar(sql_file, file.size(sql_file))
  sql_client_id <- sprintf(sql, client_id)
  
  results <- tryCatch(bqr_query(
    query = sql_client_id,
    useLegacySql=FALSE
  ), error = function(err){
    message(sql_client_id)
    stop("Error in query:", results$error, results$message, call. = FALSE)
  })
  
  str(results)
  
  message("Writing ", nrow(results), " rows to bigquery_results.csv")
  write.csv(results, file = "/workspace/bigquery_results.csv", row.names = FALSE)

  
  TRUE
  
}

client_id <- Sys.getenv("CLIENT_ID")
if(nzchar(client_id)){
  query_client_id(client_id, "/workspace/06-activation/user-activity-ga4.sql")
} else {
  message("Could not find client_id")
}
