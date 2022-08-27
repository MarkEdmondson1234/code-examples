# replace with the folder where your .asciidoc files are
book_folder <- "../learning-google-analytics/"

# list the .asciidoc files
chapters <- list.files(book_folder,pattern = "asciidoc$", full.names = TRUE)

# read in the chapters
book_text <- lapply(chapters, readLines)
names(book_text) <- basename(chapters)

# parse out only the image:: lines
book_images <- lapply(book_text, \(x) x[grepl("^image", x)])

# create a list of dfs with image info
images_df <- lapply(names(book_images), \(x){
  o <- book_images[[x]]
  if(length(o) == 0) return(NULL)
  reggy <- "^image::images/(.+?\\.(png|jpg))\\[(.*)\\]"
  filenames <- gsub(reggy, "\\1", o)
  captions <- gsub(reggy, "\\3", o)
  chapter <- substr(x, 1,2)
  the_df <- data.frame(filename = filenames, caption = captions)
  the_df$fig_num <- paste0(chapter, "-", gsub(" ", "0", sprintf("%2d", 1:nrow(the_df))))
  
  the_df
})

# turn it into one data.frame and write to csv
all_images <- Reduce(rbind, images_df)
write.csv(all_images, file = "figure-log.csv", row.names = FALSE)

# I then imported the CSV into GoogleSheets for review