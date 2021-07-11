library(googleAnalyticsR)
library(rtweet)

ga4s <- ga_account_list("ga4")

gaid <- 206670707
meta <- ga_meta("data", propertyId = gaid)

article_reads <- ga_data(gaid,
        metrics = "eventCount",
        date_range = c("2021-07-01",as.character(Sys.Date())),
        dimensions = c("date", "customEvent:category"),
        orderBys = ga_data_order(+date),
        dim_filters = ga_data_filter(!"customEvent:category" == c("(not set)","null")),
        limit = -1)

library(tidyr)
library(dplyr)

clean_cats <- article_reads |>
        rename(category = "customEvent:category",
               reads = "eventCount") |>
        mutate(category = tolower(category)) |>
        separate(category,
                 into = paste0("category_",1:6),
                 sep = "[^[:alnum:]-]+",
                 fill = "right", extra = "drop")

long_cats <- clean_cats |>
        pivot_longer(
                cols = starts_with("category_"),
                values_to = "categories",
                values_drop_na = TRUE
        )


agg_cats <- long_cats |>
        group_by(date, categories) |>
        summarise(category_reads = sum(reads), .groups = "drop_last") |>
        arrange(date, desc(category_reads))
