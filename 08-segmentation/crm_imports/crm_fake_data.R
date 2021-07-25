library(charlatan)

crm <- read.csv(file = "08-segmentation/crm_imports/userid_cid.csv",
                colClasses = "character")
# distinct user_ids

userids <- unique(crm$user_id)
fake_people <- length(userids)

fake <- ch_generate("name", "job", n = fake_people)
fake$user_id <- userids
fake$created <- as.POSIXct(unlist(ch_unix_time(fake_people), 
                                  recursive = FALSE), 
                           origin = "1970-01-01")
fake$transactions <- as.numeric(difftime(Sys.Date(), fake$created)) %/% runif(fake_people, 300,900)
fake$revenue <- round(fake$transactions * runif(fake_people, 100,1000),2)

write.csv(fake, file = "08-segmentation/crm_imports/fake_crm.csv", row.names = FALSE)
