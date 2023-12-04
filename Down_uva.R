#' Download filtering
#'
#' @param date_start a character element indicating the date time to start thee download of all the selected devices. The format has to be %Y-%m-%d %h:%M:%S, e.g."2023-11-01 00:00:00"
#' @param date_end a character element indicating the date time to end thee download of all the selected devices. The format has to be %Y-%m-%d %h:%M:%S, e.g."2023-11-01 00:00:00"
#' @param PVC_list vector containing the "PVC" code of all the devices to download. It should be provided if device_id_list is not.
#' @param device_id_list vector containing the device numbre of all the devices to download. It should be provided if PVC_list is not.
#' @param file_s path to save the result. If it is not provided, the result is not saved on a csv.
#' @param db.file name chosen for your data source when setting up the ODBC connection according to https://wiki.e-ecology.nl/index.php/How_to_access_the_e-Ecology_database
#'
#' @return a dataframe with the downloaded data plus a csv file containing those data if file_s if used.
#' @export
#'
#' @examples DataDownloaded <- Down_uva(date_start = "2023-11-01 00:00:00",date_end = "2023-11-10 00:00:00",PVC_list = c("2X5","3FA"), file_s = "Todaydata.csv", db.file = "GPS_UvA")

Down_uva <- function(date_start, date_end, PVC_list, device_id_list, file_s, db.file){

  Sys.setenv(TZ = "UTC")

  if(missing(db.file))
    stop("You have to provide a name for the connection with the server")
  if(!missing(device_id_list) & !missing(PVC_list))
    stop("You have to provide either PVC_list or device_id_list, never both of them")

  if(missing(date_end))
    date_end <- as.character(Sys.time())

  if("plyr"%in%installed.packages()[,1] == FALSE) install.packages("plyr")
  library(plyr)
  if("RODBC"%in%installed.packages()[,1] == FALSE) install.packages("RODBC")
  library(RODBC)

  db <- odbcConnect(db.file)

  query_PVC <- paste0("SELECT t.device_info_serial, s.remarks AS PVC, t.latitude,",
                      " t.longitude, t.date_time, t.altitude, t.speed_2d, i.sex, i.remarks AS born, s.track_session_id",
                      " FROM gps.ee_track_session_limited s JOIN gps.ee_individual_limited i",
                      " ON s.individual_id = i.individual_id",
                      " JOIN gps.ee_tracking_speed_limited t",
                      " ON t.device_info_serial = s.device_info_serial AND t.date_time BETWEEN '",date_start,"' AND '",date_end,
                      "' WHERE t.userflag=0 AND s.remarks <> 'INCORRECT' AND s.remarks = '")

  query_device <- paste0("SELECT t.device_info_serial, s.remarks AS PVC, t.latitude,",
                         " t.longitude, t.date_time, t.altitude, t.speed_2d, i.sex, i.remarks AS born, s.track_session_id",
                         " FROM gps.ee_track_session_limited s JOIN gps.ee_individual_limited i",
                         " ON s.individual_id = i.individual_id",
                         " JOIN gps.ee_tracking_speed_limited t",
                         " ON t.device_info_serial = s.device_info_serial AND t.date_time BETWEEN '",date_start,"' AND '",date_end,
                         "' WHERE t.userflag=0 AND s.remarks <> 'INCORRECT' AND t.device_info_serial = '")


  if(!missing(PVC_list)){
    id_to_down <- PVC_list
    query1 <- query_PVC
  }

  if(!missing(device_id_list)){
    id_to_down <- device_id_list
    query1 <- query_device
  }

  Res <- NULL

  for(i in 1:length(id_to_down)){
    Tloop <- sqlQuery(db, paste0(query1,id_to_down[i],"'"))
    Res <- rbind(Res,Tloop)
    print(paste(i,"de",length(id_to_down)))
  }

  Res <- subset(Res, date_time < Sys.time())
  Res <- plyr::rename(Res,c("pvc"="PVC"))
  Res$track_session_id <- NULL

  if(!missing(file_s))
    write.csv(Res, file = file_s,row.names = F)

  return(Res)

}
