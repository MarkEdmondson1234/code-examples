library(blastula)

the_data <- read.csv("/workspace/bigquery_results.csv")

# Get a nicely formatted date/time string
date_time <- add_readable_time()

# Create an image string using an on-disk
# image file
img_file_path <-
  system.file(
    "img", "pexels-photo-267151.jpeg",
    package = "blastula"
  )

img_string <- add_image(file = img_file_path)

email <-
  compose_email(
    body = md(glue::glue(
      "Hello,

This is a *great* picture I found when looking
for sun + cloud photos:

{img_string}
")),
footer = md(glue::glue("Email sent on {date_time}."))
  )

the_email <- Sys.getenv("EMAIL")

if(nzchar(the_email)){
  email %>%
    smtp_send(
      to = the_email,
      from = "ga4-example@markedmondson.me",
      subject = "Testing the `smtp_send()` function",
      credentials = creds_file("/workspace/blastula_gmail_creds")
    )
} else {
  message("Could not find email in EMAIL env var")
}


