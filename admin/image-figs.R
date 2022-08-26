book_folder <- "../learning-google-analytics/"

chapters <- list.files(book_folder,pattern = "asciidoc$", full.names = TRUE)

book_text <- lapply(chapters, readLines)
names(book_text) <- basename(chapters)

book_images <- lapply(book_text, \(x) x[grepl("^image", x)])

images_df <- lapply(names(book_images), \(x){
  o <- book_images[[x]]
  if(length(o) == 0) return(NULL)
  reggy <- "^image::images/(.+?\\.(png|jpg))\\[(.+)\\]"
  filenames <- gsub(reggy, "\\1", o)
  captions <- gsub(reggy, "\\2", o)
  chapter <- substr(x, 1,2)
  the_df <- data.frame(filename = filenames, caption = captions)
  the_df$fig_num <- paste0(chapter, "-", gsub(" ", "0", sprintf("%2d", 1:nrow(the_df))))
  
  the_df
})

all_images <- Reduce(rbind, images_df)
write.csv(all_images, file = "figure-log.csv", row.names = FALSE)