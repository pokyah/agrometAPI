#----
#' Retrieve data from the [AGROMET API](https://app.pameseb.be/fr/pages/api_call_test/).
#' @importFrom httr GET
#' @importFrom httr add_headers
#' @importFrom httr content
#' @importFrom purrr imap
#' @importFrom purrr map2_df
#' @importFrom jsonlite fromJSON
#' @importFrom magrittr %>%
#' @importFrom dplyr bind_cols
#' @importFrom dplyr select
#' @importFrom dplyr mutate_at
#' @importFrom dplyr left_join
#' @importFrom dplyr funs
#' @importFrom dplyr vars
#' @importFrom dplyr one_of
#' @param user_token A character specifying your user token. Default reads your .Renviron file
#' @param table_name A character specifying the database table you want to query.
#' @param user_token A character specifying your user token
#' @param sensors A character vector containing the names of the sensors you want to query
#' @param sid A character vector containing the ids of the stations you want to query
#' @param dfrom A character specifying the data from which you want to query data (YYYY-MM-DD)
#' @param dto A character specifying the data to which you want to query data (YYYY-MM-DD)
#' @param month_day A character specifying the month and day from which you want TMY data (MM-DD)
#' @param api_v A character specifying the version of the API you want to use ("v1" or "v2")
#' @param test.bool A boolean, TRUE if you want to query the test server
#' @return A list containing the metadata in the first element and the queried data in the second element
#' @export
get_from_agromet_API <- function(
  user_token = Sys.getenv("AGROMET_API_V1_KEY"),
  table_name = "cleandata",
  sensors = "tsa",
  sid = "all",
  dfrom = Sys.Date()-2,
  dto = Sys.Date()-1,
  month_day = NULL,
  api_v = "v2",
  test.bool = FALSE
){

  # Defining the base URL for API calls
  baseURL.chr <- "https://app.pameseb.be/agromet/api"
  if (test.bool == TRUE){
    baseURL.chr <- "https://testapp.pameseb.be/agromet/api"
  }

  # Clean the eventual spaces in the sensors string
  if (!is.null(sensors)){
    sensors <- gsub(" ","",sensors)
  }

  # Build the proper table API call URL
  if (table_name == "get_rawdata_irm"){
    api_table_url.chr <- paste(baseURL.chr, api_v, table_name, sensors, sid, dfrom, dto, sep="/")
  }
  if (table_name == "station"){
    api_table_url.chr <- paste(baseURL.chr, api_v, table_name, sid,  sep="/")
  }
  if (table_name == "cleandata"){
    api_table_url.chr <- paste(baseURL.chr, api_v, table_name, sensors, sid, dfrom, dto, sep="/")
  }
  if (table_name == "get_tmy"){
    api_table_url.chr <- paste(baseURL.chr, api_v, table_name, sensors, sid, month_day, sep="/")
  }
  if (table_name == "get_daily"){
    api_table_url.chr <- paste(baseURL.chr, api_v, table_name, sensors, sid, dfrom, dto, sep="/")
  }
  if (table_name == "get_rawdata_dssf"){
    api_table_url.chr <- paste(baseURL.chr, api_v, table_name, dfrom, dto, "dailygeojson", sep="/")
  }
  cat(paste("your API URL call is : ", api_table_url.chr, " \n "))

  # Add your user token into the HTTP authentication header and call API (https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Authorization)
  api_table_req.resp <- httr::GET(api_table_url.chr, httr::add_headers("Authorization" = paste("Token", user_token, sep=" ")))

  if (api_table_req.resp$status_code!=200){
    stop(paste0("The API responded with an error ", api_table_req.resp$status_code, ". Function execution halted. \n Please check your token and the validity + order of the parameters you provided. API documentation available at https://app.pameseb.be/fr/pages/api_call_test/ " ))
  }
  cat(paste0("The API responded with a status code ", api_table_req.resp$status_code, ". \n Your requested data has been downloaded \n"))

  # Getting the JSON data from the API response
  api_results_json.chr <- httr::content(api_table_req.resp, as = "text", encoding = "UTF-8")

  # Transform the JSON response to R-friendly list format
  results.l <- jsonlite::fromJSON(api_results_json.chr)

  # Remove the terms of service and version info to only keep the data
  if (table_name != "get_rawdata_dssf" ) {
    results.df <- as.data.frame(results.l$results)
  }else{
    results.df <- as.data.frame(results.l$features)

    # make each feature properties a list element
    date_ens.l <- results.df$properties
    date_ens.l <- purrr::imap(date_ens.l, function(x, sid) cbind(x, sid))

    coords.l <- results.df$geometry$coordinates
    coords.l <- lapply(coords.l, function(x) as.data.frame(t(x)))
    coords.l <- purrr::imap(coords.l, function(x, sid) cbind(x, sid))

    # join each feature coordinates + properties
    # https://stackoverflow.com/questions/44703738/merge-two-lists-of-dataframes
    results.df <- as.data.frame(purrr::map2_df(date_ens.l, coords.l, dplyr::left_join, by = "sid"))
    colnames(results.df) <- c("mhour", "ens", "sid", "lat", "lon")
  }

  # check if we do not have results for this query, stop the execution
  if (class(results.df) != "data.frame") {
    stop(paste0("There are no records for this query. Function execution halted. \n Please provide another date Range and/or parameters input" ))
  }

  # Rename the column "station" to "id" for later clarity of the code only if the API returns results
  # colnames(results.df)[which(names(results.df) == "station")] <- "id"

  # Create a dataframe for the stations meta-information
  stations_meta.df <- results.l$references$stations

  # The query with table = station does not provide records but only metadata stored in records.df
  if (table_name == "station") {
    stations_meta.df <- results.df
    results.df <- NULL
  }

  # Group in a list
  results_and_stations_meta.l <- list(stations_meta.df = stations_meta.df, records.df = results.df)

  # Present a quick overview of the results in the console
  # cat("Overview of the queried results : \n")
  # print.data.frame(head(results_and_stations_meta.l$records.df))

  # Return the results and the station_meta dataframes stored in as a list
  return(results_and_stations_meta.l)
}


#----
#' Prepare the data obtained by get_from_agromet_api.fun(). It types all the character variables to their proper types.
#' @importFrom magrittr %>%
#' @importFrom chron times
#' @importFrom dplyr select
#' @importFrom dplyr bind_cols
#' @importFrom dplyr mutate_at
#' @importFrom dplyr vars
#' @importFrom dplyr one_of
#' @importFrom dplyr funs
#' @param  meta_and_records.l a list containing agromet records and metadata returned by get_from_agromet_api.fun().
#' @param table_name a character specifying the name of the agromet table from which the data where called using get_from_agromet_api.fun()
#' @return a typed dataframe
#' @export
prepare_agromet_API_data.fun  <- function(meta_and_records.l, table_name="cleandata"){

  # declaration of the function to convert sunrise and sunset columns to chron objects
  convertSun <- function(sunHour.chr){

    # transform to datetime format
    sunHour.posix <- strptime(x = sunHour.chr, format = "%H:%M:%S")

    # only keep the hour part using library chron
    sunHour.chron <- chron::times(format(sunHour.posix, "%H:%M:%S"))

    # retourn the sunHour.chron object
    return(sunHour.chron)
  }

  # Create the vector of all the existing sensors in the Agromet db
  sensors <- c("tsa", "tha", "hra", "tsf", "tss", "ens", "dvt", "vvt", "plu", "hct", "ts2", "th2", "hr2", "plu_sum")

  # Create the stations positions df
  stations_meta.df <- meta_and_records.l[[1]]

  # Create the records df
  records.df <- meta_and_records.l[[2]]
  if (table_name == "get_tmy") {
    records.df <- records.df %>% dplyr::select(-metadata)
  }

  # In stations_meta.df, tmy_period information are stored as df stored inside df. We need to extract these from this inner level and add as new columns
  tmy_period.df <- stations_meta.df$metadata$tmy_period

  if (table_name != "get_rawdata_dssf"){
    stations_meta.df <- stations_meta.df %>% dplyr::select(-metadata)
    stations_meta.df <- dplyr::bind_cols(stations_meta.df, tmy_period.df)
    # Transform from & to column to posix format for easier time handling
    data.df <- stations_meta.df %>%
      dplyr::mutate_at("from", as.POSIXct, format = "%Y-%m-%dT%H:%M:%S", tz = "GMT-2") %>%
      dplyr::mutate_at("to", as.POSIXct, format = "%Y-%m-%dT%H:%M:%S", tz = "GMT-2")
  }else{
    data.df <- NULL
  }

  if (!is.null(records.df)){
    # Join stations_meta and records by "id"
    if (!is.null(data.df)){
      records.df <- dplyr::left_join(data.df, records.df, by=c("sid"))
    }

    # Transform sid and id columns from character to numeric
    records.df <- records.df %>% dplyr::mutate_at(dplyr::vars(dplyr::one_of(c("sid", "id"))), dplyr::funs(as.numeric))

    # Transform mtime column to posix format for easier time handling
    records.df <- records.df %>% dplyr::mutate_at(dplyr::vars(dplyr::one_of(c("mtime", "mhour"))), as.POSIXct, format = "%Y-%m-%dT%H:%M:%SZ")

    # Transform meta altitude, longitude, latitude columns from character to numeric
    records.df <- records.df %>% dplyr::mutate_at(dplyr::vars(dplyr::one_of(c("altitude", "longitude", "latitude", "lon", "lat"))), dplyr::funs(as.numeric))

    if(table_name != "get_rawdata_dssf"){
      # Transform sensors columns from character to numeric values
      records.df <- records.df %>% dplyr::mutate_at(dplyr::vars(dplyr::one_of(sensors)), dplyr::funs(as.numeric))

      # Transform sunrise/sunset columns to times format for easier time handling
      if(!is.null(records.df$sunrise)){
        records.df <- records.df %>% dplyr::mutate_at(c("sunrise","sunset"), convertSun)
      }
    }
  }

  # Return the properly typed and structured records dataframe.
  return(records.df)
}
