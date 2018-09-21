/**
 * @file dbconnection.h
 * @brief Definition of the DbConnection class
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

#ifndef DBCONNECTION_H
#define DBCONNECTION_H

#include <cstdint>
#include <ctime>
#include <functional>
#include <tuple>
#include <memory>
#include <mutex>
#include <tuple>
#include <vector>
#include <string>

#include <cassandra.h>
#include <date.h>

#include "message.h"
#include "dbconnection_common.h"

namespace meteodata {
	/**
	 * @brief A handle to the database to insert meteorological measures
	 *
	 * An instance of this class is to be used by each meteo station
	 * connector to query details about the station and insert measures in
	 * the database periodically.
	 */
	class DbConnection : public DbConnectionCommon
	{
	public:
		/**
		 * @brief Construct a connection to the database
		 *
		 * @param user the username to use
		 * @param password the password corresponding to the username
		 */
		DbConnection(const std::string& address = "127.0.0.1", const std::string& user = "", const std::string& password = "");
		/**
		 * @brief Close the connection and destroy the database handle
		 */
		virtual ~DbConnection() = default;
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
		 * @param[out] lastDataInsertionTime The timestamp of the last data from the station inserted into the database
		 *
		 * @return The boolean value true if everything went well, false if an error occurred
		 */
		bool getStationByCoords(int latitude, int longitude, int altitude, CassUuid& station, std::string& name, int& pollPeriod, time_t& lastArchiveDownloadTime, time_t& lastDataInsertionTime);

		/**
		 * @brief Get all the ICAOs (international meteorological stations identifier) with their UUID correspondance
		 *
		 * @param[out] synopStations A vector with a pair (UUID,ICAO) for each MétéoFrance SYNOP station
		 *
		 * @return The boolean value true if everything went well, false if an error occurred
		 */
		bool getAllIcaos(std::vector<std::tuple<CassUuid, std::string>>& synopStations);

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
		 * @brief Insert a new data point in the V2 database
		 *
		 * This method is very similar to \a insertDataPoint() but uses the
		 * new database scheme.
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
		 * @brief Get the name of a station and its polling period
		 *
		 * @param uuid The station identifier
		 * @param[out] name Where to store the name of the station
		 * @param[out] pollPeriod Where to store the polling period of the
		 * station (the amount of time which should separate two
		 * measurements)
		 * @param[out] lastArchiveDownloadTime The timestamp of the
		 * last archive entry downloaded from the database
		 *
		 * @return True if, and only if, all went well
		 */
		bool getStationDetails(const CassUuid& uuid, std::string& name, int& pollPeriod, time_t& lastArchiveDownloadTime);
		/**
		 * @brief Identify the last time data was retrieved from a
		 * station
		 *
		 * @param station The station of interest
		 * @param[out] lastDataInsertionTime The timestamp of the last
		 * entry corresponding to the database
		 *
		 * @return True if everything went well and
		 * lastDataInsertionTime is the expected result, false if the
		 * query failed
		 */
		bool getLastDataInsertionTime(const CassUuid& station, time_t& lastDataInsertionTime);

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

	private:
		/**
		 * @brief The first prepared statement for the getStationByCoords()
		 * method
		 */
		std::unique_ptr<const CassPrepared, std::function<void(const CassPrepared*)>> _selectStationByCoords;
		/**
		 * @brief The second prepared statement for the getStationByCoords()
		 * method
		 */
		std::unique_ptr<const CassPrepared, std::function<void(const CassPrepared*)>> _selectStationDetails;
		/**
		 * @brief The prepared statement for the getAllIcaos()
		 * method
		 */
		std::unique_ptr<const CassPrepared, std::function<void(const CassPrepared*)>> _selectAllIcaos;
		/**
		 * @brief The prepared statement for the getLastInsertionTime()
		 * method
		 */
		std::unique_ptr<const CassPrepared, std::function<void(const CassPrepared*)>> _selectLastDataInsertionTime;
		/**
		 * @brief The prepared statement for the insertDataPoint() method
		 */
		std::unique_ptr<const CassPrepared, std::function<void(const CassPrepared*)>> _insertDataPoint;
		/**
		 * @brief The prepared statement for the insertV2DataPoint() method
		 */
		std::unique_ptr<const CassPrepared, std::function<void(const CassPrepared*)>> _insertDataPointInNewDB;
		/**
		 * @brief The prepared statement for the
		 * updateLastArchiveDownload() method
		 */
		std::unique_ptr<const CassPrepared, std::function<void(const CassPrepared*)>> _updateLastArchiveDownloadTime;
		/**
		 * @brief The prepared statement for the
		 * getWeatherlinkStations() method
		 */
		std::unique_ptr<const CassPrepared, std::function<void(const CassPrepared*)>> _selectWeatherlinkStations;
		/**
		 * @brief The prepared statement for the deleteDataPoints()
		 * method
		 */
		std::unique_ptr<const CassPrepared, std::function<void(const CassPrepared*)>> _deleteDataPoints;
		/**
		 * @brief A mutual exclusion semaphore to protect _insertDataPoint
		 */
		std::mutex _insertMutex;
		/**
		 * @brief A mutual exclusion semaphore to protect _selectStationByCoords
		 */
		std::mutex _selectMutex;
		/**
		 * @brief A mutual exclusion semaphore to protect _updateLastArchiveDownloadTime
		 */
		std::mutex _updateLastArchiveDownloadMutex;
		/**
		 * @brief A mutual exclusion semaphore to protect _deleteDataPoints
		 */
		std::mutex _deleteDataPointsMutex;
		/**
		 * @brief Prepare the Cassandra query/insert statements
		 */
		void prepareStatements();
	};
}

#endif
