library(charlatan)  # generate fake names
library(bigQueryR)

bqr_global_project("learning-ga4")
bqr_global_dataset("crm_imports_us")

# downloaded from GA4 BigQuery demo dataset
cids <- read.csv(file = "08-segmentation/crm_imports/ga4-demo-cids.csv",
                colClasses = "character")

# random order, only 50% of cids
fake_logins <- cids[sample(nrow(cids)), , drop = FALSE]
fake_logins <- head(fake_logins, nrow(fake_logins) / 2)
fake_logins$crm_id <- sprintf("CRM%06d", seq.int(nrow(fake_logins)))

# distinct user_ids

fake_people <- nrow(fake_logins)
ga4_last_date <- as.Date("2021-01-31")

fake <- ch_generate("name", "job", n = fake_people)

z <- DateTimeProvider$new()

fake$created_ts <- unlist(lapply(1:fake_people,
                              function(x){
                                z$date_time_between(start_date = as.Date("2001-03-05"),
                                                    end_date = ga4_last_date) 
                              }))
fake$created <- as.POSIXct(fake$created_ts, origin = "1970-01-01")

# make it more likely to transact if you have these jobs
fake$bias <- grepl("teacher|researcher|academic|school|engine|doctor|prof|surgeon|phd|dr|science", 
                   fake$job, ignore.case = TRUE)
fake$transactions <- as.numeric(difftime(ga4_last_date, fake$created)) %/% 
                                runif(fake_people, 10000,90000)
fake$transactions <- abs(ifelse(fake$bias, 
                            round(fake$transactions*runif(fake_people, 1.1, 2)), 
                            fake$transactions))

fake$revenue <- round(fake$transactions * runif(fake_people, 1,150),2)
fake$permission <- as.logical(round(runif(fake_people, min = 0.4, max = 1)))
fake$crm_id <- fake_logins$crm_id
fake$cid <- as.character(fake_logins$user_pseudo_id)
fake$bias <- NULL
fake$created_ts <- NULL

filename <- "08-segmentation/crm_imports/fake_crm.csv"
write.csv(fake, file = filename, row.names = FALSE)

# fake <- read.csv(filename,stringsAsFactors = FALSE, colClasses = "character")

bqr_auth(email = "me@markedmondson.me")
bqr_global_project("learning-ga4")
bqr_global_dataset("crm_imports_us")

bqr_delete_table(tableId = "fake_crm_transactions")
bqr_create_table(tableId = "fake_crm_transactions",
                 timePartitioning = TRUE)
bqr_upload_data(tableId = "fake_crm_transactions",
                upload_data = fake)
