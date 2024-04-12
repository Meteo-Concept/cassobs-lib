/**
 * @file dbconnection_observations.h
 * @brief Definition of the DbConnectionObservations class
 * @author Laurent Georget
 * @date 2018-09-20
 */
/*
 * Copyright (C) 2018  SAS Météo Concept <contact@meteo-concept.fr>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef DBCONNECTION_OBSERVATIONS_H
#define DBCONNECTION_OBSERVATIONS_H

#include <cstdint>
#include <ctime>
#include <functional>
#include <tuple>
#include <memory>
#include <tuple>
#include <vector>
#include <string>
#include <map>

#include <cassandra.h>
#include <date.h>

#include "message.h"
#include "dbconnection_common.h"
#include "observation.h"
#include "map_observation.h"
#include "cassandra_stmt_ptr.h"
#include "virtual_station.h"

namespace meteodata {
	/**
	 * @brief A handle to the database to insert meteorological measures
	 *
	 * An instance of this class is to be used by each meteo station
	 * connector to query details about the station and insert measures in
	 * the database periodically.
	 */
	class DbConnectionObservations : public DbConnectionCommon
	{
		public:
			/**
			 * @brief Construct a connection to the database
			 *
			 * @param user the username to use
			 * @param password the password corresponding to the username
			 */
			DbConnectionObservations(const std::string& address = "127.0.0.1", const std::string& user = "", const std::string& password = "");
			/**
			 * @brief Close the connection and destroy the database handle
			 */
			virtual ~DbConnectionObservations() = default;
			/**
			 * @brief Get the identifier of a station given its coordinates
			 *
			 * @param latitude The latitude of the station
			 * @param longitude The longitude of the station
			 * @param altitude The elevation of the station
			 * @param[out] station Where to store UUID corresponding to the station
			 * @param[out] name The common name given to the station
			 * @param[out] pollPeriod The period of time between two measurements from the station
			 * @param[out] lastArchiveDownloadTime The timestamp of the last archive entry downloaded from the station (in station's time)
			 * @param[out] storeInsideMeasurements Whether we have the authorization to store inside measurements from the station
			 *
			 * @return The boolean value true if everything went well, false if an error occurred
			 */
			bool getStationByCoords(int latitude, int longitude, int altitude, CassUuid& station, std::string& name, int& pollPeriod, time_t& lastArchiveDownloadTime, bool* storeInsideMeasurements = nullptr);

			/**
			 * @brief Get the coordinates of a station
			 *
			 * @param[in] station The station's UUID
			 * @param[out] latitude The latitude of the station
			 * @param[out] longitude The longitude of the station
			 * @param[out] altitude The elevation of the station
			 * @param[out] name The common name given to the station
			 * @param[out] pollPeriod The period of time between two measurements from the station
			 *
			 * @return The boolean value true if everything went well, false if an error occurred
			 */
			bool getStationCoordinates(CassUuid station, float& latitude, float& longitude, int& altitude, std::string& name, int& pollPeriod);

			/**
			 * @brief Get all the ICAOs (international meteorological stations identifier) with their UUID correspondance
			 *
			 * @param[out] synopStations A vector with a pair (UUID,ICAO) for each MétéoFrance SYNOP station
			 *
			 * @return The boolean value true if everything went well, false if an error occurred
			 */
			bool getAllIcaos(std::vector<std::tuple<CassUuid, std::string>>& synopStations);
			/**
			 * @brief Fetch the list of SYNOPs that are not available in realtime and must be
			 * downloaded every 24 hours or so
			 *
			 * @param[out] stations The so-called "deferred SYNOPs" as tuples (UUID, ICAO)
			 *
			 * @return True if everything went well, false if the query failed
			 */
			bool getDeferredSynops(std::vector<std::tuple<CassUuid, std::string>>& stations);

			/**
			 * @brief Insert a new data point in the database
			 *
			 * This method is only appropriate for VantagePro2 (R) stations
			 * connectors, we might have to genericize it later.
			 *
			 * @param station The identifier of the station
			 * @param message A message from a meteo station connector
			 * containing the measurements to insert in the database
			 *
			 * @return True is the measure data point could be succesfully
			 * inserted, false otherwise
			 */
			bool insertDataPoint(const CassUuid station, const Message& message);
			/**
			 * @brief Insert a new data point in the monitoring database
			 *
			 * This method is very similar to \a insertDataPoint() but uses the
			 * new database scheme, and inserts the data point in a special database
			 * used to monitor the data stations.
			 *
			 * @param station The identifier of the station
			 * @param message A message from a meteo station connector
			 * containing the measurements to insert in the database
			 *
			 * @return True is the measure data point could be succesfully
			 * inserted, false otherwise
			 */
			bool insertMonitoringDataPoint(const CassUuid station, const Message& message);
			/**
			 * @brief Insert a new data point in the V2 database
			 *
			 * This method is very similar to \a insertDataPoint() but uses the
			 * new database scheme. It ignores cumulative values such as rainfall24
			 * and insolation_time24.
			 *
			 * This method is now deprecated in favor of
			 * \a insertV2DataPoint(const CassUuid station, const Observation& obs).
			 *
			 * @deprecated since 2021-11-11, v. ??
			 *
			 * @param station The identifier of the station
			 * @param message A message from a meteo station connector
			 * containing the measurements to insert in the database
			 *
			 * @return True is the measure data point could be succesfully
			 * inserted, false otherwise
			 */
			bool insertV2DataPoint(const CassUuid station, const Message& message);

			/**
			 * @brief Insert a new data point in the V2 database
			 *
			 * This method does the same as the other overload of \a insertV2DataPoint
			 * but using an instance of \a Observation, provided by this library, which
			 * grants the library more control over the values, lets it filter them
			 * before insertion, etc.
			 *
			 * @param obs An observation from a meteo station connector
			 * containing the measurements to insert in the database
			 *
			 * @return True is the measure data point could be succesfully
			 * inserted, false otherwise
			 */
			bool insertV2DataPoint(const Observation& obs);

			/**
			 * @brief Insert a new special data point in the V2 database
			 *
			 * This method uses the new database scheme. Only the special
			 * variables rainfall24 and insolation_time24 corresponding to
			 * the cumulative values of the rainfall and the total
			 * insolation duration are considered in this method. These are
			 * useful for stations that report grand totals with a higher
			 * precision than individual measurements.
			 *
			 * @param station The identifier of the station
			 * @param rainfall24 The rainfall over the day (from 6h UTC the
			 * very day to 6h UTC the next day). This is a pair
			 * (bool,float), the second element is the value, it is taken
			 * into account if and only if the first element is true.
			 * @param insolationTime24 The insolation total duration in
			 * minutes over the day (from 0h UTC to 0h UTC the next day.)
			 * This is a pair (bool,float), the second element is the
			 * value, it is taken into account if and only if the first
			 * element is true.
			 *
			 * @return True is the measure data point could be succesfully
			 * inserted, false otherwise
			 */
			bool insertV2EntireDayValues(const CassUuid station, const time_t& time, std::pair<bool, float> rainfall24, std::pair<bool, int> insolationTime24);

			/**
			 * @brief Update the Tx value in the V2 database
			 *
			 * This method uses the new database scheme. Only the variable
			 * Tx corresponding respectively to the max temperature is
			 * considered in this method. This is necessary when the Tx
			 * occurs in the middle of a measurement period.
			 *
			 * This method is quite costly because it checks that the
			 * maximum is indeed greater than the value already recorded.
			 * According to the OMM norms, the Tx is measured between 6h
			 * UTC and 6h UTC, this method takes care of updating the
			 * correct day.
			 *
			 * @param station The identifier of the station
			 * @param tx The maximum temperature.
			 *
			 * @return True is the measure data point could be succesfully
			 * inserted, false otherwise
			 */
			bool insertV2Tx(const CassUuid station, const time_t& time, float tx);

			/**
			 * @brief Update the Tn value in the V2 database
			 *
			 * This method uses the new database scheme. Only the variable
			 * Tn corresponding respectively to the max temperature is
			 * considered in this method. This is necessary when the Tx
			 * occurs in the middle of a measurement period.
			 *
			 * This method is quite costly because it checks that the
			 * minimum is indeed lower than the value already recorded.
			 * According to the OMM norms, the Tn is measured between 18h
			 * UTC and 18h UTC, this method takes care of updating the
			 * correct day.
			 *
			 * @param station The identifier of the station
			 * @param tn The minimum temperature.
			 *
			 * @return True is the measure data point could be succesfully
			 * inserted, false otherwise
			 */
			bool insertV2Tn(const CassUuid station, const time_t& time, float tn);

			/**
			 * @brief Insert in the database the time of the last archive
			 * entry downloaded from a station
			 *
			 * In order to download archive, it is necessary to have the
			 * timestamp of an existing entry in the station'a archive so we
			 * store the timestamp of the last entry retrieved from the
			 * station each time we download archives.
			 *
			 * @param station The identifier of the station of interest
			 * @param time The new timestamp of the last archive entry
			 * downloaded from \a station
			 *
			 * @return True if the last archive timestamp could be updated,
			 * false otherwise.
			 */
			bool updateLastArchiveDownloadTime(const CassUuid station, const time_t& time);
			/**
			 * @brief Fetch the latest datapoint before some datetime
			 *
			 * @param station The station of interest
			 * @param boundary The timestamp the data to be fetched must be immediately
			 * anterior to
			 *
			 * @return True if everything went well, false if the query failed
			 */
			bool getLastDataBefore(const CassUuid& station, time_t boundary, Observation& values);

			/**
			 * @brief Get Weatherlink connection information for all the stations that send their
			 * data to weatherlink.com
			 *
			 * Each station is associated to a tuple (UUID, auth, token, timezone) where
			 * 'UUID' is the identifier of the station, 'auth' is the parameter string to use
			 * in queries made to weatherlink.com (it's a string of the form "user=XXXX&password=XXXX"),
			 * 'token' is the weatherlink.com v1 API token, 'timezone' is the timezone the
			 * station is configured in (for now : 0 => UTC, 1 => CEST).
			 *
			 * @param[out] stations A vector of tuple (UUID, auth, token, timezone)
			 *
			 * @return True if everything went well, false if an error occurred
			 */
			bool getAllWeatherlinkStations(std::vector<std::tuple<CassUuid, std::string, std::string, int>>& stations);

			/**
			 * @brief Get Weatherlink connection information for all the stations we retrieve the
			 * data from using APIv2 (WeatherLink Live and EnviroMonitors as of September 2019)
			 *
			 * Each station is associated to a tuple (UUID, archived, sensors, weatherlinkId, parsers) where
			 * 'UUID' is the identifier of the station, 'archived' is a boolean indicating whether
			 * the historic data is available or only the realtime data, 'mapping' is a mapping
			 * from sensor ids to UUIDs used for weatherlink stations that are mapped to several
			 * Meteodata stations and 'weatherlinkId' is the identifier of the station in
			 * WeatherLink APIv2 answers.
			 *
			 * @param[out] stations A vector of tuple (UUID, archived, sensors, weatherlinkId, parsers)
			 *
			 * @return True if everything went well, false if an error occurred
			 */
			bool getAllWeatherlinkAPIv2Stations(std::vector<std::tuple<CassUuid, bool, std::map<int,CassUuid>, std::string,
				std::map<int, std::map<std::string, std::string>>>>& stations);

			/**
			 * @brief Get MQTT subscription details for all the stations that send their
			 * data via MQTT
			 *
			 * Each station is associated to a tuple (UUID, host, port, user, password, password_length, topic, tz)
			 * where 'UUID' is the identifier of the station, 'host' the IP or domain name to connect to, 'port',
			 * the coresponding port number, 'user', the user name to use, 'password', the corresponding password
			 * for the user, 'password_length', the length of the password, 'topic', the name of the topic on which
			 * the station publishes archive messages, 'tz', the timezone used by the station.
			 *
			 * @param[out] stations A vector of tuples (UUID, host, port, user, password, password_length, topic)
			 *
			 * @return True if everything went well, false if an error occurred
			 */
			bool getMqttStations(std::vector<std::tuple<CassUuid, std::string, int, std::string, std::unique_ptr<char[]>, size_t, std::string, int>>& stations);
			/**
			 * @brief Get StatIC downloadable file location for stations that make their
			 * observations available in a StatIC file on a web server
			 *
			 * Each station is associated to a tuple (UUID, host, url, https, tz, sensors) where 'UUID' is the identifier of
			 * the station, 'host' the domain name of the web server, 'url' the URL of the StatIC file, 'https'
			 * whether HTTPS must be used instead of HTTP to access the file,  'tz' the timezone identifier
			 * (normally always "0" for "UTC", but it's better not to rely on that), and 'sensors' are additional keys
			 * to parse in the StatIC files besides the standard one (mapped to the corresponding variable).
			 *
			 * @param[out] stations A vector of tuples (UUID, host, url, https, tz)
			 *
			 * @return True if everything went well, false if an error occurred.
			 */
			bool getStatICTxtStations(std::vector<std::tuple<CassUuid, std::string, std::string, bool, int, std::map<std::string, std::string>>>& stations);
			/**
			 * @brief Get MBData downloadable file location for stations that make their
			 * observations available in a MBData file on a web server
			 *
			 * Each station is associated to a tuple (UUID, host, url, https, tz, type) where
			 * 'UUID' is the identifier of the station,
			 * 'host' the domain name of the web server,
			 * 'url' the URL of the file,
			 * 'https' whether https must be used,
			 * 'tz' the timezone code (the same as in the Weatherlink table),
			 * 'type' the type of the file (not all pieces of software are able to produce
			 * exactly the same format so there are some discrepancies which must be dealt with).
			 *
			 * @param[out] stations A vector of tuples (UUID, host, url, https, tz, type)
			 *
			 * @return True if everything went well, false if an error occurred.
			 */
			bool getMBDataTxtStations(std::vector<std::tuple<CassUuid, std::string, std::string, bool, int, std::string>>& stations);

			/**
			 * @brief Get FieldClimate connection information for all the stations we retrieve the
			 * data from using the FieldClimate API by Pessl
			 *
			 * Each station is associated to a tuple (UUID,fieldclimateId,
			 * tz, sensors) where 'UUID' is the identifier of the station,
			 * 'fieldclimate_id' is the identifier for the station in the
			 * FieldClimate API, 'tz' is the identifier of the timezone the
			 * station is configured with, and 'sensors' is a map between
			 * variables and sensors identifiers indicating which part of
			 * the data in the API returned results must be parsed.
			 *
			 * @param[out] stations A vector of tuple (UUID, fieldclimateId, tz, sensors)
			 *
			 * @return True if everything went well, false if an error occurred
			 */
			bool getAllFieldClimateApiStations(std::vector<std::tuple<CassUuid, std::string, int, std::map<std::string, std::string>>>& stations);

			/**
			 * @brief Get Objenious SPOT connection information for all the stations we retrieve the
			 * data from using the Objenious SPOT platform API by Bouygues
			 *
			 * Each station is associated to a tuple (UUID,objeniousId,
			 * sensors) where 'UUID' is the identifier of the station,
			 * 'objeniousId' is the identifier for the station in the
			 * FieldClimate API, and 'variables' is a map between
			 * variables and variables identifiers indicating which part of
			 * the data in the API returned results must be parsed.
			 *
			 * @param[out] stations A vector of tuple (UUID, objeniousId, variables)
			 *
			 * @return True if everything went well, false if an error occurred
			 */
			bool getAllObjeniousApiStations(std::vector<std::tuple<CassUuid, std::string, std::map<std::string, std::string>>>& stations);

			/**
			 * @brief Get LiveObjects connection information for all the stations we retrieve the
			 * data from using the LiveObjects platform by Orange
			 *
			 * Each station is associated to a tuple (UUID,streamId,topic)
			 * where 'UUID' is the identifier of the station,
			 * 'streamId' is the identifier for the station in the
			 * LiveObjects API, and topic is the name of the LiveObjects FIFO
			 * in which the data is available.
			 *
			 * @param[out] stations A vector of tuple (UUID, streamId, topic)
			 *
			 * @return True if everything went well, false if an error occurred
			 */
			bool getAllLiveobjectsStations(std::vector<std::tuple<CassUuid, std::string, std::string>>& stations);

			/**
			 * @brief Get information relative to stations branded by CIMEL
			 *
			 * Each station is associated to a tuple (UUID,cimelId,timezone)
			 * where 'UUID' is the identifier of the station,
			 * 'cimelId' is the identifier for the station in the
			 * CIMEL software (a combination of the city and a numerical identifier),
			 * and 'timezone' is the timezone identifier of the station, which should
			 * always be 0, for UTC.
			 *
			 * @param[out] stations A vector of tuple (UUID, cimelId, timezone)
			 *
			 * @return True if everything went well, false if an error occurred
			 */
			bool getAllCimelStations(std::vector<std::tuple<CassUuid, std::string, int>>& stations);

			/**
			 * @brief Get information relative to stations belonging
			 * to the Meteo France networks
			 *
			 * TODO
			 *
			 * @return True if everything went well, false if an error occurred
			 */
			bool getMeteoFranceStations(std::vector<std::tuple<CassUuid, std::string, std::string, int, float, float, int, int>>& stations);

			/**
			 * @brief Get information relative to virtual stations, i.e. stations whose data is actually
			 * sourced from a collection of other stations or sensors
			 *
			 * @param[out] stations A vector of VirtualStation-s
			 *
			 * @return True if everything went well, false if an error occurred
			 */
			bool getAllVirtualStations(std::vector<VirtualStation>& stations);

			/**
			 * @brief Get the total rainfall between two datetimes for a station
			 *
			 * @param[in] station The station UUID
			 * @param[in] begin The start of the time range
			 * @param[in] end The end of the time range
			 * @param[out] rainfall The rainfall at the station during the time range
			 *
			 * @return True if everything went well, false if an error occurred.
			 */
			bool getRainfall(const CassUuid& station, time_t begin, time_t end, float& rainfall);

			/**
			 * @brief Remove all data points for a given station and time range
			 *
			 * The time range cannot exceed the boundaries of a single day, chop
			 * the time range accordingly and call this method several times if needed.
			 *
			 * @param[in] station The station identifier
			 * @param[in] day The day the time range falls into
			 * @param[in] start The beginning of the time range
			 * @param[in] end The end of the time range
			 *
			 * @return True if everything went well, false otherwise
			 */
			bool deleteDataPoints(const CassUuid& station, const date::sys_days& day, const date::sys_seconds& start, const date::sys_seconds& end);

			/**
			 * @brief Retrieve the last integer value stored for a given
			 * station and key
			 *
			 * This method also provides the time at which the value was
			 * last updated.
			 *
			 * @param[in] station The station identifier
			 * @param[in] key The cache key that identifies the value to recover
			 * @param[out] lastUpdate The last time the value was updated for the station and key
			 * @param[out] value The value associated to the station and key
			 */
			bool getCachedInt(const CassUuid& station, const std::string& key, time_t& lastUpdate, int& value);

			/**
			 * @brief Retrieve the last floating-point value stored for a given
			 * station and key
			 *
			 * This method also provides the time at which the value was
			 * last updated.
			 *
			 * @param[in] station The station identifier
			 * @param[in] key The cache key that identifies the value to recover
			 * @param[out] lastUpdate The last time the value was updated for the station and key
			 * @param[out] value The value associated to the station and key
			 */
			bool getCachedFloat(const CassUuid& station, const std::string& key, time_t& lastUpdate, float& value);

			/**
			 * @brief Insert an integer value into the cache, replacing the previous
			 * one, if any and if it's older
			 *
			 * @param[in] station The station identifier
			 * @param[in] key The cache key that identifies the value to recover
			 * @param[in] update The update time of the new value
			 * @param[in] value The value associated to the station and key
			 */
			bool cacheInt(const CassUuid& station, const std::string& key, const time_t& update, int value);

			/**
			 * @brief Insert a floating-point value into the cache, replacing the previous
			 * one, if any and if it's older
			 *
			 * @param[in] station The station identifier
			 * @param[in] key The cache key that identifies the value to recover
			 * @param[in] update The update time of the new value
			 * @param[in] value The value associated to the station and key
			 */
			bool cacheFloat(const CassUuid& station, const std::string& key, const time_t& update, float value);

			/**
			 * @brief Get all values required to compute the cumulative values for an insertion query
			 *
			 * @param[out] obs A map observation structure to store the data
			 *
			 * @return True if everything went well, false if an error occurred.
			 */
			bool getMapValues(const CassUuid& station, time_t time, MapObservation& obs);

		private:
			/**
			 * @brief The prepared statement for the getStationByCoords()
			 * method
			 */
			CassandraStmtPtr _selectStationByCoords;
			/**
			 * @brief The prepared statement for the getStationCoordinates()
			 * method
			 */
			CassandraStmtPtr _selectStationCoordinates;
			/**
			 * @brief The prepared statement for the getAllIcaos()
			 * method
			 */
			CassandraStmtPtr _selectAllIcaos;
			/**
			 * @brief The prepared statement for the getDeferredSynops() method
			 */
			CassandraStmtPtr _selectDeferredSynops;
			/**
			 * @brief The prepared statement for the getLastInsertionTime()
			 * method
			 */
			CassandraStmtPtr _selectLastDataInsertionTime;
			/**
			 * @brief The prepared statement for the getLastDataBefore() method
			 */
			CassandraStmtPtr _selectLastDataBefore;
			/**
			 * @brief The prepared statement for the insertV2RawDataPoint() method
			 */
			CassandraStmtPtr _insertV2RawDataPoint;
			/**
			 * @brief The prepared statement for the insertV2FilteredDataPoint() method
			 */
			CassandraStmtPtr _insertV2FilteredDataPoint;
			/**
			 * @brief The prepared statement for the insertV2MapDataPoint() method
			 */
			CassandraStmtPtr _insertV2MapDataPoint;
			/**
			 * @brief The prepared statement for the insertV2EntireDayValues() method
			 */
			CassandraStmtPtr _insertEntireDayValues;
			/**
			 * @brief The prepared statement for the insertV2Tx() method
			 */
			CassandraStmtPtr _insertTx;
			/**
			 * @brief The prepared statement for the insertV2Tn() method
			 */
			CassandraStmtPtr _insertTn;
			/**
			 * @brief The prepared statement for the insertMonitoringDataPoint() method
			 */
			CassandraStmtPtr _insertDataPointInMonitoringDB;
			/**
			 * @brief The prepared statement for the
			 * updateLastArchiveDownload() method
			 */
			CassandraStmtPtr _updateLastArchiveDownloadTime;
			/**
			 * @brief The prepared statement for the
			 * getWeatherlinkStations() method
			 */
			CassandraStmtPtr _selectWeatherlinkStations;
			/**
			 * @brief The prepared statement for the
			 * getWeatherlinkAPIv2Stations() method
			 */
			CassandraStmtPtr _selectWeatherlinkAPIv2Stations;
			/**
			 * @brief The prepared statement for the
			 * getMqttStations() method
			 */
			CassandraStmtPtr _selectMqttStations;
			/**
			 * @brief The prepared statement for the
			 * getStatICTxtStations() method
			 */
			CassandraStmtPtr _selectStatICTxtStations;
			/**
			 * @brief The prepared statement for the
			 * getMBDataTxtStations() method
			 */
			CassandraStmtPtr _selectMBDataTxtStations;
			/**
			 * @brief The prepared statement for the
			 * getFieldClimateApiStations() method
			 */
			CassandraStmtPtr _selectFieldClimateApiStations;
			/**
			 * @brief The prepared statement for the
			 * getObjeniousApiStations() method
			 */
			CassandraStmtPtr _selectObjeniousApiStations;
			/**
			 * @brief The prepared statement for the
			 * getLiveobjectsStations() method
			 */
			CassandraStmtPtr _selectLiveobjectsStations;
			/**
			 * @brief The prepared statement for the
			 * getCimelStations() method
			 */
			CassandraStmtPtr _selectCimelStations;
			/**
			 * @brief The prepared statement for the
			 * getMeteoFranceStations() method
			 */
			CassandraStmtPtr _selectMeteoFranceStations;
			/**
			 * @brief The prepared statement for the getAllVirtualStations() method
			 */
			CassandraStmtPtr _selectVirtualStations;
			/**
			 * @brief The prepared statement for the getRainfall() method
			 */
			CassandraStmtPtr _getRainfall;
			/**
			 * @brief The prepared statement for the deleteDataPoints()
			 * method
			 */
			CassandraStmtPtr _deleteDataPoints;
			/**
			 * @brief The prepared statement for the getCachedInt()
			 * and getCachedFloat() methods
			 */
			CassandraStmtPtr _selectCached;
			/**
			 * @brief The prepared statement for the cacheInt()
			 * and cacheFloat() methods
			 */
			CassandraStmtPtr _insertIntoCache;
			/**
			 * @brief Prepare the Cassandra query/insert statements
			 */
			void prepareStatements();

			/**
			 * @brief Get the max temperature of a day, if recorded in the observations database
			 *
			 * @param[out] tx A vector (bool, float), the boolean tells whether the value is available
			 *
			 * @return True if everything went well, false if an error occurred.
			 */
			bool getTx(const CassUuid& station, time_t boundary, std::pair<bool, float>& value);
			/**
			 * @brief Get the min temperature of a day, if recorded in the observations database
			 *
			 * @param[out] tn A vector (bool, float), the boolean tells whether the value is available
			 *
			 * @return True if everything went well, false if an error occurred.
			 */
			bool getTn(const CassUuid& station, time_t boundary, std::pair<bool, float>& value);

			void populateV2CommonInsertionQuery(CassStatement* statement, const Observation& obs, int& c);
			void populateV2InsertionQuery(CassStatement* statement, const Observation& obs);
			void populateV2MapInsertionQuery(CassStatement* statement, const Observation& obs, const MapObservation& map, const std::chrono::seconds& insertionTime);

			/**
			 * @brief The prepared statement for the getTx() method
			 */
			CassandraStmtPtr _selectTx;
			/**
			 * @brief The prepared statement for the getTn() method
			 */
			CassandraStmtPtr _selectTn;
			/**
			 * @brief The prepared statement for the getMapValues() method
			 */
			CassandraStmtPtr _selectMapValues;

			/**
			 * @brief The interval of time at which observations are
			 * rounded on the observations map
			 */
			constexpr static chrono::minutes OBSERVATIONS_MAP_TIME_RESOLUTION{5};
	};
}

#endif
