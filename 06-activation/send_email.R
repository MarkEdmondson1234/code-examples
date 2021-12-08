library(blastula)
library(formattable)

the_data <- read.csv("/workspace/bigquery_results.csv")

if(nrow(the_data) < 1){
  stop("Data only one row, stopping")
}

# Get a nicely formatted date/time string
date_time <- add_readable_time()
ga4_table <- format_table(the_data)

email <-
  compose_email(
    body = md(glue::glue(
      "Hello,
      
  You requested your GA4 browsing history from Mark Edmondson's website.  Here it is!
  {ga4_table}
  
")),
footer = md(glue::glue("Email sent on {date_time}."))
  )

the_email <- Sys.getenv("EMAIL")

if(nzchar(the_email)){
  email %>%
    smtp_send(
      to = the_email,
      from = "me@markedmondson.me",
      subject = "Your GA4 history for Mark Edmondson's blog",
      credentials = creds_file("/workspace/blastula_gmail_creds")
    )
} else {
  stop("Could not find email in EMAIL env var")
}


