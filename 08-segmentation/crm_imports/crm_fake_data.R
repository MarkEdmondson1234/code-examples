library(charlatan)  # generate fake names
library(bigQueryR)

bqr_global_project("learning-ga4")
bqr_global_dataset("crm_imports")

# downloaded from GA4 BigQuery demo dataset
cids <- read.csv(file = "08-segmentation/crm_imports/ga4-demo-cids.csv",
                colClasses = "character")

# random order, only 50% of cids
fake_logins <- cids[sample(nrow(cids)), , drop = FALSE]
fake_logins <- head(fake_logins, nrow(fake_logins) / 2)
fake_logins$crm_id <- sprintf("CRM%06d", seq.int(nrow(fake_logins)))

# distinct user_ids

fake_people <- nrow(fake_logins)

fake <- ch_generate("name", "job", n = fake_people)
fake$created <- as.POSIXct(unlist(ch_unix_time(fake_people), 
                                  recursive = FALSE), 
                           origin = "1970-01-01")
fake$transactions <- as.numeric(difftime(Sys.Date(), fake$created)) %/% runif(fake_people, 100,900)
fake$revenue <- round(fake$transactions * runif(fake_people, 100,1000),2)
fake$permission <- as.logical(round(runif(fake_people, min = 0.4, max = 1)))
fake$crm_id <- fake_logins$crm_id
fake$cid <- fake_logins$user_pseudo_id

filename <- "08-segmentation/crm_imports/fake_crm.csv"
write.csv(fake, file = filename, row.names = FALSE)

fake <- read.csv(filename, stringsAsFactors = FALSE)

bqr_upload_data(projectId = "learning-ga4",
                datasetId = "crm_imports",
                tableId = "fake_crm", 
                upload_data = fake)
