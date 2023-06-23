### TM ID 120677
### option 1 json: https://boardgamegeek.com/thread/1109812/jsonjsonp-wrapper
### option 2 xml: https://api.geekdo.com/xmlapi2
### https://boardgamegeek.com/wiki/page/BGG_XML_API2#
### https://boardgamegeek.com/wiki/page/BGG_XML_API#
### option 3 https://boardgamegeek.com/wiki/page/XML_API_Enhancements
### tools list https://boardgamegeek.com/guild/1229

###############################################################
################# extracting game list ########################
###############################################################

library('rvest')
library('jsonlite')
library('stringr')
library('parallel')
library('doParallel')
library('foreach')
library('xml2')
library('magrittr')
library('RSelenium')

##############################################################
####################### backup folder ########################
##############################################################

files <- list.files("data", "*.[RData|csv]", include.dirs = FALSE)
extDate <- na.omit(str_extract(files, "\\d{4}-\\d{2}-\\d{2}"))[1]
new_dir <- paste0("data/", extDate)
dir.create(new_dir)

copyStatus <- file.copy(paste0("data/",files), 
                paste0(new_dir,"/",files))

if(!all(copyStatus)) {
  stop("failed copying files")
} else {
  file.remove(paste0("data/", files))
}

##############################################################
##############################################################
##############################################################

catch <- function(bgg_link){
  while(TRUE){
    html <- try(xml2::read_html(httr::GET(bgg_link)), silent=TRUE)
    if(!is(html, 'try-error')) break
  }
  return(html)
}

npages <- catch("http://boardgamegeek.com/browse/boardgame/page/1") |>
            html_element(".fr") |>
            html_element("a:nth-last-of-type(1)") |>
            html_text() |>
            str_extract("\\d+") |>
            as.numeric()
npages


catch_s <- function(bgg_link){
  
  remDr$navigate(bgg_link)
  pg <- remDr$getPageSource()[[1]]
  return(read_html(pg) )
}


core <- function(i){
  # print(paste0('P?gina ',i, ' de 974'))
  bgg_link <- paste0('http://boardgamegeek.com/browse/boardgame/page/',i)
  
  html <- catch_s(bgg_link)
  
  table <- html %>% 
    html_element("table.collection_table") %>%
    html_table(header = T)
  
  # if(is(table, 'try-error')) {
  #   print(paste("Error:", bgg_link))
  #   return(data.frame())
  # }
  
  table <- table[-seq(16, 100, 16),]
  
  table$link = html %>%
    html_elements('.collection_thumbnail a') %>%
    html_attr("href")
  
  table$link = paste0('https://boardgamegeek.com',table$link)
  
  return(table)
}

#### loop paralelizado links

ncores <- 4
cl <- snow::makeCluster(ncores)
doSNOW::registerDoSNOW(cl)

clusterExport(cl, c("core", "catch_s"), 
              envir=environment())

ports <- as.integer(sample(3000:4000, ncores))

if(length(unique(ports)) != ncores) {
  stop("Non unique ports used")
}
portsweb <- 7900:(7900+ncores-1)


images <- c()

for(i in 1:ncores){
  new_img <- system(glue::glue("docker run -d -p {ports[i]}:4444 -p {portsweb[i]}:7900 --shm-size 4g selenium/standalone-firefox:latest"), intern=FALSE, ignore.stdout=FALSE)
  print(glue::glue(("you can watch the extraction in the following link: localhost:{portsweb[i]}, password: secret")))
  images <- c(images, new_img)
}

username <- rstudioapi::showPrompt("BGG Username", "BGG username")
password <- rstudioapi::askForPassword()

clusterApply(cl, x = ports, username = username, password = password, function(x, username, password){
  # Here you load the libraries on each core
  library("RSelenium")
  library("dplyr")
  library("rvest") 
  library('jsonlite')
  library('stringr')
  
  # Pay attention to the use of the superassignment operator.
  
  # try(
  #   rD <<- RSelenium::rsDriver(
  #     browser = "chrome",
  #     port = x,
  #     verbose = T,
  #     check = FALSE
  #   )
  # )
  
  
  # Pay attention to the use of the superassignment operator.
  remDr <<- remoteDriver(
    remoteServerAddr = "localhost",
    port = x,
    browserName = "firefox"
  )
  

  
  #### bgg login
  
  remDr$open()
  
  remDr$navigate("http://boardgamegeek.com/login")
  
  webElem <- remDr$findElement(using = "css", ".form-control[type=text]")
  webElem$click()
  webElem$sendKeysToElement(list(username))
  
  webElem2 <- remDr$findElement(using = "css", ".form-control[type=password]")
  webElem2$click()
  webElem2$sendKeysToElement(list(password))
  
  webElem3 <- remDr$findElement(using = "css", ".btn-lg[type=submit]")
  webElem3$clickElement()
})

full <- data.frame()

outp <- parLapply(cl, 1:npages, function(x) {
  s <-  core(x)
  s
})


close_rselenium <- function(){
  clusterEvalQ(cl, {
    remDr$close()
    rD$server$stop()
  })
  
  for(img in images) {
    system(glue::glue("docker stop {img}"), 
           intern=FALSE, 
           ignore.stdout=FALSE)
    }
}

close_rselenium()


snow::stopCluster(cl)

full <- data.table::rbindlist(outp)

nrow(full)

full[,1] <- as.numeric(full$`Board Game Rank`)
full <- full[,-2]
full <- full[order(full$`Board Game Rank`),]

write.csv(full, glue::glue('data/boards_list_{Sys.Date()}.csv'), row.names=F)

###############################################################
############## extracting detailed info #######################
###############################################################

library("XML")
library("xml2")
library("data.table")
library("stringr")
library("jsonlite")

latestFiles <-list.files("data", "boards_list_*", full.names = T)
latestFile <- latestFiles[order(latestFiles)][1]

full <- read.csv(latestFile, header = T, stringsAsFactors = F)

full$year <- str_extract(full$Title, '(?![(])\\d+(?=[)])')
full$title <- str_extract(full$Title, '^.+?(?=\n\t)')
full$desc <- str_extract(full$Title, '[^\t]+$')
full$desc <- str_replace(full$desc, '^\\(\\d+\\)$', "")
full$id <- str_match(full$link, '/*(\\d+)/')[,2]

## 41114 resistance
## 120677 TM
## 12493 TI3
## 356035 TM exp
## 255668 trickerion collectors
## 120677,12493,356035,41114,255668

get_xml <- function(game_id, sleep_time = 20){
  xm <- try(xml2::read_xml(glue::glue("https://boardgamegeek.com/xmlapi2/thing?id={game_id}&stats=1&type=boardgame,boardgameexpansion")))
  if(is(xm, 'try-error')){
    print("Rate 429 rate error")
    Sys.sleep(sleep_time)
    xm <- get_xml(game_id)
  }
  return(xm)
}

xml_to_DT <- function(parsed) {
  
  #parsed <- gameList[[1]]
  #parsed <- XML::xmlParse(xm)
  #boardgame <- XML::xmlToList(parsed)
  boardgame <- fromJSON(toJSON(parsed)) ## usethis otherwise multiple equal keys
  
  infonames <- names(boardgame)
  uniqueInfo <- unique(gsub("[.]\\d+","", infonames))
  
  keep_info <- c('yearpublished', 'minplayers', 'maxplayers', 'playingtime', 
                 'minplaytime', 'maxplaytime', 'minage', 'description', 
                 'thumbnail', 'image', '.attrs')
  
  bg_treated <- boardgame[keep_info]
  bg_treated$name <- boardgame$name[3]
  bg_treated$type <- bg_treated$.attrs[1] 
  bg_treated$id <- bg_treated$.attrs[2]
  bg_treated$.attrs <- NULL
  
  bg_treated <- bg_treated[!is.na(names(bg_treated))]
  
  bg_treated <- Filter(function(x) length(x) > 0, bg_treated)
  
  print(glue::glue("{bg_treated$id}: {bg_treated$name}"))
  setDT(bg_treated)
  
  stats <- boardgame$statistics$ratings[names(boardgame$statistics$ratings) != "ranks"]
  bg_treated[,names(stats) := stats]
  #setnames(bg_treated, ".attrs", "id") XML1
  
  # bg_treated$title <- ?? 
  
  ##### polls
  polls <- infonames[grepl("poll", infonames)]
  
  ### poll 0
  if (length(boardgame$poll) > 2) {
  title <- boardgame$poll[length(boardgame$poll)][[1]][1]
  poll <- data.table::rbindlist(boardgame$poll[-length(boardgame$poll)], fill = T)
  p.names <- unlist(poll[1,])
  p.names[4] <- "numplayers"
  
  row_odd <- seq_len(nrow(poll)) %% 2
  poll <- poll[row_odd == 0, ] 
  setnames(poll,colnames(poll), p.names)
  bg_treated[, suggested_numplayers := list(poll) ]
  }
  
  ### API 1 poll.1 and 2 are inverted
  ### poll 2
  if (!is.null(boardgame$poll.2$results)) {
  title <- boardgame$poll.2[length(boardgame$poll.2)][[1]][1]
  poll.2 <- data.table::rbindlist(boardgame$poll.2[-length(boardgame$poll.2)], fill = T)
  poll.2 <- as.data.table(t(poll.2))
  if (all(poll.2$V1 != c("1", "2", "3", "4", "5"))) {
    poll.2$V1 <- c("1", "2", "3", "4", "5")
  }
  
  setnames(poll.2,colnames(poll.2), c("language_dependence_level", "language_dependence", "votes"))
  bg_treated[, language_dependence := list(poll.2) ]
  }
  
  ### poll 1
  if (length(boardgame$poll.1$results) > 2) {
  title <- boardgame$poll.1[length(boardgame$poll.1)][[1]][1]
  poll.1 <- data.table::rbindlist(boardgame$poll.1[-length(boardgame$poll.1)], fill = T)
  poll.1 <- as.data.table(t(poll.1))
  
  setnames(poll.1,colnames(poll.1), c("suggested_age", "votes"))
  bg_treated[, suggested_playerage := list(poll.1) ]
  }
  
  vec_to_DT <- function(vec) {
    frame <- rbind.data.frame(vec)
    colnames(frame) <- seq_along(colnames(frame))
    frame <- setDT(frame)
    frame
  }
  
  
  data <- data.table::rbindlist(lapply(boardgame[grepl("link", infonames)], vec_to_DT), fill = T)
  
  if (ncol(data) == 4 ){
    setnames(data, c("metadata", "id", "name", "reimplements"))
  } else if (ncol(data) > 0) {
    setnames(data, c("metadata", "id", "name"))
  }
  
  loopinfo <- unique(data$metadata)
  
  for(dataInfo in loopinfo) {
    
    partial <- data[metadata == dataInfo,]
    partial[, metadata := NULL ]
    
    dataInfo_name <- substr(dataInfo, 10, nchar(dataInfo))
    
    setnames(partial, c("id", "name"), c(paste0(dataInfo_name, "_id"), 
                                         paste0(dataInfo_name, "_name")))
             
    
    if (dataInfo == "boardgamecompilation") {
      if (ncol(partial) == 2) {
        partial[, contains := character() ]
      } else {
        setnames(partial, "reimplements", "contains")
      }
      
    }
    
    if (dataInfo == "boardgameimplementation" ) {
      if(ncol(partial) == 2) {
            partial[, reimplements := character() ]
        }    
    } else if ("reimplements" %in% colnames(partial)) {
      partial[, reimplements := NULL ]
    } 
    
    bg_treated[, I(dataInfo) :=  list(partial)]
  }
  
  ranks <- data.table::rbindlist(lapply(boardgame$statistics$ratings$ranks, vec_to_DT), fill = T)
  
  if(ncol(ranks) == 1) {
    ranks <- data.table(rank_info = "subtype",
                        rank_id = "1",
                        rank_family = "boardgame",
                        rank_name = "Board Game Rank",
                        rank = "Not Ranked",
                        bayesaverage = "Not Ranked")
    boardgame$statistics$ratings$ranks <- NULL
    boardgame$statistics$ratings$ranks$rank <- unlist(ranks, use.names = F)
  }
  
  setnames(ranks, c("rank_info", "rank_id", "rank_family", "rank_name", "rank", "bayesaverage"))
  bg_treated[, ranks := list(ranks)]
  
  bg_names <- data.table::rbindlist(lapply(boardgame[grepl("name", infonames)], vec_to_DT), fill = T)
  setnames(bg_names, c("type","value", "name"))
  bg_treated[, names := list(bg_names)]
  
  bg_treated$bgg_rank <- boardgame$statistics$ratings$ranks$rank[5][1]
  
  return(bg_treated)
}


extract_games <- function(game_id_list, pid = 0, tid = 0){
  
  xm <- get_xml(game_id_list, sleep_time = 60)
  
  gameList <- xmlToList(xmlParse(xm))
  gameList <- gameList[-length(gameList)]
  output <- lapply(gameList, xml_to_DT)  
  
  # for(i in 1:length(gameList)){
  #   print(i)
  #   xml_to_DT(gameList[[i]])
  #   #parsed <- gameList[[130]]
  # }
  
  finalDT <- rbindlist(output, fill = T)
  
  return(finalDT)
}

# saida <- extract_games("120677,12493,356035,41114,255668")
# saida <- extract_games(id_list$idList[1])

floor(length(full$id)/300) #N/call sugges max <500

id_list <- data.table(id = full$id, group = rep(1:300, length.out  = length(full$id)))
id_list <- id_list[, .(idList = paste0(id, collapse = ",")), by = group]

# saida <- extract_games(id_list$idList[1])

ncores <- 11
cl <- parallel::makeCluster(ncores, outfile="")

clusterExport(cl, c("extract_games", "get_xml", "xml_to_DT"), 
              envir=environment())

# clusterApply(cl, 1:ncores, function(x){
#   tid <<- x
# })

clusterEvalQ(cl, {
  library("XML")
  library("xml2")
  library("data.table")
  library("stringr")
  library("jsonlite")
  
  #pid <<- Sys.getpid()
  }
)   

out <- parLapply(cl, id_list$idList, extract_games)

parallel::stopCluster(cl)

final <- rbindlist(out, fill = T)

saveRDS(final, "data/bgg_data.Rdata")

# test<- readRDS("data/bgg_data.Rdata")

####

ctypes <- sapply(final, typeof)
cnames_list <- names(ctypes[ctypes == "list"])

for(i in cnames_list){

    print(glue::glue("saving {i} table"))
    fwrite(final[, rbindlist(get(i)), by = id], 
           paste0("data/", i, "_", Sys.Date(),  ".csv")
           )

}

fwrite(final[, .SD, .SDcols = !is.list], paste0("data/bgg_data_", Sys.Date(),  ".csv"))


### manipulating
a <- final[unlist(lapply(boardgamecompilation, \(x) !is.null(x))),]
