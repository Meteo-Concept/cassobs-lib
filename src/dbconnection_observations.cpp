/**
 * @file dbconnection_observations.cpp
 * @brief Implementation of the DbConnectionObservations class
 * @author Laurent Georget
 * @date 2018-09-21
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

#include <iostream>
#include <mutex>
#include <exception>
#include <vector>
#include <utility>

#include <cassandra.h>
#include <syslog.h>
#include <unistd.h>
#include <date.h>

#include "dbconnection_observations.h"

namespace meteodata {
	DbConnectionObservations::DbConnectionObservations(const std::string& address, const std::string& user, const std::string& password) :
		DbConnectionCommon(address, user, password),
		_selectStationByCoords{nullptr, cass_prepared_free},
		_selectStationDetails{nullptr, cass_prepared_free},
		_selectAllIcaos{nullptr, cass_prepared_free},
		_selectLastDataInsertionTime{nullptr, cass_prepared_free},
		_insertDataPoint{nullptr, cass_prepared_free},
		_insertDataPointInNewDB{nullptr, cass_prepared_free},
		_updateLastArchiveDownloadTime{nullptr, cass_prepared_free},
		_selectWeatherlinkStations{nullptr, cass_prepared_free},
		_deleteDataPoints{nullptr, cass_prepared_free}
	{
		prepareStatements();
	}

	void DbConnectionObservations::prepareStatements()
	{
		CassFuture* prepareFuture = cass_session_prepare(_session.get(), "SELECT station FROM meteodata.coordinates WHERE elevation = ? AND latitude = ? AND longitude = ?");
		CassError rc = cass_future_error_code(prepareFuture);
		if (rc != CASS_OK) {
			std::string desc("Could not prepare statement selectStationByCoords: ");
			desc.append(cass_error_desc(rc));
			throw std::runtime_error(desc);
		}
		_selectStationByCoords.reset(cass_future_get_prepared(prepareFuture));
		cass_future_free(prepareFuture);

		prepareFuture = cass_session_prepare(_session.get(), "SELECT name,polling_period,last_archive_download FROM meteodata.stations WHERE id = ?");
		rc = cass_future_error_code(prepareFuture);
		if (rc != CASS_OK) {
			std::string desc("Could not prepare statement selectStationDetails: ");
			desc.append(cass_error_desc(rc));
			throw std::runtime_error(desc);
		}
		_selectStationDetails.reset(cass_future_get_prepared(prepareFuture));
		cass_future_free(prepareFuture);

		prepareFuture = cass_session_prepare(_session.get(), "SELECT id,icao FROM meteodata.stationsFR");
		rc = cass_future_error_code(prepareFuture);
		if (rc != CASS_OK) {
			std::string desc("Could not prepare statement selectAllIcaos: ");
			desc.append(cass_error_desc(rc));
			throw std::runtime_error(desc);
		}
		_selectAllIcaos.reset(cass_future_get_prepared(prepareFuture));
		cass_future_free(prepareFuture);

		prepareFuture = cass_session_prepare(_session.get(), "SELECT time FROM meteodata.meteo WHERE station = ? LIMIT 1");
		rc = cass_future_error_code(prepareFuture);
		if (rc != CASS_OK) {
			std::string desc("Could not prepare statement selectLastInsertionTime: ");
			desc.append(cass_error_desc(rc));
			throw std::runtime_error(desc);
		}
		_selectLastDataInsertionTime.reset(cass_future_get_prepared(prepareFuture));
		cass_future_free(prepareFuture);

		prepareFuture = cass_session_prepare(_session.get(),
			"INSERT INTO meteodata.meteo ("
			"station,"
			"time,"
			"bartrend,barometer,barometer_abs,barometer_raw,"
			"insidetemp,outsidetemp,"
			"insidehum,outsidehum,"
			"extratemp1,extratemp2, extratemp3,extratemp4,"
				"extratemp5, extratemp6,extratemp7,"
			"soiltemp1, soiltemp2, soiltemp3, soiltemp4,"
			"leaftemp1, leaftemp2, leaftemp3, leaftemp4,"
			"extrahum1, extrahum2, extrahum3, extrahum4,"
				"extrahum5, extrahum6, extrahum7,"
			"soilmoistures1, soilmoistures2, soilmoistures3,"
				"soilmoistures4,"
			"leafwetnesses1, leafwetnesses2, leafwetnesses3,"
				"leafwetnesses4,"
			"windspeed, winddir,"
			"avgwindspeed_10min, avgwindspeed_2min,"
			"windgust_10min, windgustdir,"
			"rainrate, rain_15min, rain_1h, rain_24h,"
			"dayrain, monthrain, yearrain,"
			"stormrain, stormstartdate,"
			"UV, solarrad,"
			"dewpoint, heatindex, windchill, thswindex,"
			"dayET, monthET, yearET,"
			"forecast, forecast_icons,"
			"sunrise, sunset,"
			"rain_archive, etp_archive)"
			"VALUES ("
			"?,"
			"?,"
			"?,?,?,?,"
			"?,?,"
			"?,?,"
			"?,?,?,?,"
				"?,?,?,"
			"?,?,?,?,"
			"?,?,?,?,"
			"?,?,?,?,"
				"?,?,?,"
			"?,?,?,"
				"?,"
			"?,?,?,"
				"?,"
			"?,?,"
			"?,?,"
			"?,?,"
			"?,?,?,?,"
			"?,?,?,"
			"?,?,"
			"?,?,"
			"?,?,?,?,"
			"?,?,?,"
			"?,?,"
			"?,?,"
			"?,?)");

		rc = cass_future_error_code(prepareFuture);
		if (rc != CASS_OK) {
			std::string desc("Could not prepare statement insertdataPoint: ");
			desc.append(cass_error_desc(rc));
			throw std::runtime_error(desc);
		}
		_insertDataPoint.reset(cass_future_get_prepared(prepareFuture));
		cass_future_free(prepareFuture);

		prepareFuture = cass_session_prepare(_session.get(),
			"INSERT INTO meteodata_v2.meteo ("
			"station,"
			"day, time,"
			"barometer,"
			"dewpoint,"
			"extrahum1, extrahum2,"
			"extratemp1,extratemp2, extratemp3,"
			"heatindex,"
			"insidehum,insidetemp,"
			"leaftemp1, leaftemp2,"
			"leafwetnesses1, leafwetnesses2,"
			"outsidehum,outsidetemp,"
			"rainrate, rainfall,"
			"et,"
			"soilmoistures1, soilmoistures2, soilmoistures3,"
				"soilmoistures4,"
			"soiltemp1, soiltemp2, soiltemp3, soiltemp4,"
			"solarrad,"
			"thswindex,"
			"uv,"
			"windchill,"
			"winddir, windgust, windspeed,"
			"insolation_time)"
			"VALUES ("
			"?,"
			"?, ?,"
			"?,"
			"?,"
			"?, ?,"
			"?,?, ?,"
			"?,"
			"?,?,"
			"?, ?,"
			"?, ?,"
			"?,?,"
			"?, ?,"
			"?,"
			"?, ?, ?,"
				"?,"
			"?, ?, ?, ?,"
			"?,"
			"?,"
			"?,"
			"?,"
			"?, ?, ?,"
			"?)");

		rc = cass_future_error_code(prepareFuture);
		if (rc != CASS_OK) {
			std::string desc("Could not prepare statement insertdataPointInNewDB: ");
			desc.append(cass_error_desc(rc));
			throw std::runtime_error(desc);
		}
		_insertDataPointInNewDB.reset(cass_future_get_prepared(prepareFuture));
		cass_future_free(prepareFuture);

		prepareFuture = cass_session_prepare(_session.get(), "UPDATE meteodata.stations SET last_archive_download = ? WHERE id = ?");
		rc = cass_future_error_code(prepareFuture);
		if (rc != CASS_OK) {
			std::string desc("Could not prepare statement updateLastArchiveDownloadTime: ");
			desc.append(cass_error_desc(rc));
			throw std::runtime_error(desc);
		}
		_updateLastArchiveDownloadTime.reset(cass_future_get_prepared(prepareFuture));
		cass_future_free(prepareFuture);

		prepareFuture = cass_session_prepare(_session.get(), "SELECT station, auth, api_token, tz FROM meteodata.weatherlink");
		rc = cass_future_error_code(prepareFuture);
		if (rc != CASS_OK) {
			std::string desc("Could not prepare statement selectWeatherlinkStations: ");
			desc.append(cass_error_desc(rc));
			throw std::runtime_error(desc);
		}
		_selectWeatherlinkStations.reset(cass_future_get_prepared(prepareFuture));
		cass_future_free(prepareFuture);

		prepareFuture = cass_session_prepare(_session.get(), "DELETE FROM meteodata_v2.meteo WHERE station=? AND day=? AND time>? AND time<=?");
		rc = cass_future_error_code(prepareFuture);
		if (rc != CASS_OK) {
			std::string desc("Could not prepare statement deleteDataPoints: ");
			desc.append(cass_error_desc(rc));
			throw std::runtime_error(desc);
		}
		_deleteDataPoints.reset(cass_future_get_prepared(prepareFuture));
		cass_future_free(prepareFuture);
	}

	// /!\ must be called under _selectMutex lock
	bool DbConnectionObservations::getStationDetails(const CassUuid& uuid, std::string& name, int& pollPeriod, time_t& lastArchiveDownloadTime)
	{
		CassFuture* query;
		CassStatement* statement = cass_prepared_bind(_selectStationDetails.get());
		std::cerr << "Statement prepared" << std::endl;
		cass_statement_bind_uuid(statement, 0, uuid);
		query = cass_session_execute(_session.get(), statement);
		std::cerr << "Executed statement getStationDetails" << std::endl;
		cass_statement_free(statement);

		const CassResult* result = cass_future_get_result(query);
		bool ret = false;
		if (result) {
			const CassRow* row = cass_result_first_row(result);
			if (row) {
				const char *stationName;
				size_t size;
				cass_value_get_string(cass_row_get_column(row,0), &stationName, &size);
				cass_value_get_int32(cass_row_get_column(row,1), &pollPeriod);
				cass_int64_t timeMillisec;
				cass_value_get_int64(cass_row_get_column(row,2), &timeMillisec);
				lastArchiveDownloadTime = timeMillisec/1000;
				name.clear();
				name.insert(0, stationName, size);
				ret = true;
			}
		}
		cass_result_free(result);
		cass_future_free(query);

		return ret;
	}

	// /!\ must be called under _selectMutex lock
	bool DbConnectionObservations::getLastDataInsertionTime(const CassUuid& uuid, time_t& lastDataInsertionTime)
	{
		CassFuture* query;
		CassStatement* statement = cass_prepared_bind(_selectLastDataInsertionTime.get());
		std::cerr << "Statement prepared" << std::endl;
		cass_statement_bind_uuid(statement, 0, uuid);
		query = cass_session_execute(_session.get(), statement);
		std::cerr << "Executed statement getLastDataInsertionTime" << std::endl;
		cass_statement_free(statement);

		const CassResult* result = cass_future_get_result(query);
		bool ret = false;
		if (result) {
			const CassRow* row = cass_result_first_row(result);
			ret = true;
			if (row) {
				cass_int64_t insertionTimeMillisec;
				cass_value_get_int64(cass_row_get_column(row,0), &insertionTimeMillisec);
				std::cerr << "Last insertion was at " << insertionTimeMillisec << std::endl;
				lastDataInsertionTime = insertionTimeMillisec / 1000;
			} else {
				lastDataInsertionTime = 0;
			}
		}
		cass_result_free(result);
		cass_future_free(query);

		return ret;
	}

	bool DbConnectionObservations::getStationByCoords(int elevation, int latitude, int longitude, CassUuid& station, std::string& name, int& pollPeriod, time_t& lastArchiveDownloadTime, time_t& lastDataInsertionTime)
	{
		CassFuture* query;
		{ /* mutex scope */
			std::lock_guard<std::mutex> queryMutex{_selectMutex};
			CassStatement* statement = cass_prepared_bind(_selectStationByCoords.get());
			std::cerr << "Statement prepared" << std::endl;
			cass_statement_bind_int32(statement, 0, elevation);
			cass_statement_bind_int32(statement, 1, latitude);
			cass_statement_bind_int32(statement, 2, longitude);
			query = cass_session_execute(_session.get(), statement);
			std::cerr << "Executed statement getStationByCoords" << std::endl;
			cass_statement_free(statement);
		}

		const CassResult* result = cass_future_get_result(query);
		bool ret = false;
		if (result) {
			const CassRow* row = cass_result_first_row(result);
			if (row) {
				cass_value_get_uuid(cass_row_get_column(row,0), &station);
				ret = getStationDetails(station, name, pollPeriod, lastArchiveDownloadTime);
				if (ret)
					getLastDataInsertionTime(station, lastDataInsertionTime);
			}
		}
		cass_result_free(result);
		cass_future_free(query);

		return ret;
	}

	bool DbConnectionObservations::insertDataPoint(const CassUuid station, const Message& msg)
	{
		CassFuture* query;
		{ /* mutex scope */
			std::lock_guard<std::mutex> queryMutex{_insertMutex};
			std::cerr << "About to insert data point in database" << std::endl;
			CassStatement* statement = cass_prepared_bind(_insertDataPoint.get());
			msg.populateDataPoint(station, statement);
			query = cass_session_execute(_session.get(), statement);
			cass_statement_free(statement);
		}

		const CassResult* result = cass_future_get_result(query);
		bool ret = true;
		if (!result) {
			const char* error_message;
			size_t error_message_length;
			cass_future_error_message(query, &error_message, &error_message_length);
			std::cerr << "Error from Cassandra: " << error_message << std::endl;
			ret = false;
		}
		cass_result_free(result);
		cass_future_free(query);

		return ret;
	}

	bool DbConnectionObservations::insertV2DataPoint(const CassUuid station, const Message& msg)
	{
		bool ret = true;

		CassFuture* query;
		{ /* mutex scope */
			std::lock_guard<std::mutex> queryMutex{_insertMutex};
			std::cerr << "About to insert data point in database" << std::endl;
			CassStatement* statement = cass_prepared_bind(_insertDataPointInNewDB.get());
			msg.populateV2DataPoint(station, statement);
			query = cass_session_execute(_session.get(), statement);
			cass_statement_free(statement);
		}

		const CassResult* result = cass_future_get_result(query);
		if (!result) {
			const char* error_message;
			size_t error_message_length;
			cass_future_error_message(query, &error_message, &error_message_length);
			std::cerr << "Error from Cassandra: " << error_message << std::endl;
			ret = false;
		}
		cass_result_free(result);
		cass_future_free(query);

		return ret;
	}

	bool DbConnectionObservations::updateLastArchiveDownloadTime(const CassUuid station, const time_t& time)
	{
		CassFuture* query;
		{ /* mutex scope */
			std::lock_guard<std::mutex> queryMutex{_updateLastArchiveDownloadMutex};
			std::cerr << "About to update an archive download time in database" << std::endl;
			CassStatement* statement = cass_prepared_bind(_updateLastArchiveDownloadTime.get());
			cass_statement_bind_int64(statement, 0, time * 1000);
			cass_statement_bind_uuid(statement, 1, station);
			query = cass_session_execute(_session.get(), statement);
			cass_statement_free(statement);
		}

		const CassResult* result = cass_future_get_result(query);
		bool ret = true;
		if (!result) {
			const char* error_message;
			size_t error_message_length;
			cass_future_error_message(query, &error_message, &error_message_length);
			std::cerr << "Error from Cassandra: " << error_message << std::endl;
			ret = false;
		}
		cass_result_free(result);
		cass_future_free(query);

		return ret;
	}

	bool DbConnectionObservations::getAllWeatherlinkStations(std::vector<std::tuple<CassUuid, std::string, std::string, int>>& stations)
	{
		std::unique_ptr<CassStatement, void(&)(CassStatement*)> statement{
			cass_prepared_bind(_selectWeatherlinkStations.get()),
			cass_statement_free
		};
		std::unique_ptr<CassFuture, void(&)(CassFuture*)> query{
			cass_session_execute(_session.get(), statement.get()),
			cass_future_free
		};

		const CassResult* result = cass_future_get_result(query.get());
		bool ret = false;
		if (result) {
			std::unique_ptr<CassIterator, void(&)(CassIterator*)> iterator{
				cass_iterator_from_result(result),
				cass_iterator_free
			};
			while (cass_iterator_next(iterator.get())) {
				const CassRow* row = cass_iterator_get_row(iterator.get());
				CassUuid station;
				cass_value_get_uuid(cass_row_get_column(row,0), &station);
				const char *authString;
				size_t sizeAuthString;
				cass_value_get_string(cass_row_get_column(row,1), &authString, &sizeAuthString);
				std::string apiToken;
				const CassValue* raw = cass_row_get_column(row, 2);
				if (!cass_value_is_null(raw)) {
					const char *token;
					size_t sizeToken;
					cass_value_get_string(raw, &token, &sizeToken);
					apiToken.assign(token, sizeToken);
				}
				int timezone;
				cass_value_get_int32(cass_row_get_column(row,3), &timezone);
				stations.emplace_back(station, std::string{authString, sizeAuthString}, apiToken, timezone);
			}
			ret = true;
		}

		return ret;
	}

	bool DbConnectionObservations::getAllIcaos(std::vector<std::tuple<CassUuid, std::string>>& stations)
	{
		std::unique_ptr<CassStatement, void(&)(CassStatement*)> statement{
			cass_prepared_bind(_selectAllIcaos.get()),
			cass_statement_free
		};
		std::unique_ptr<CassFuture, void(&)(CassFuture*)> query{
			cass_session_execute(_session.get(), statement.get()),
			cass_future_free
		};

		const CassResult* result = cass_future_get_result(query.get());
		bool ret = false;
		if (result) {
			std::unique_ptr<CassIterator, void(&)(CassIterator*)> iterator{
				cass_iterator_from_result(result),
				cass_iterator_free
			};
			while (cass_iterator_next(iterator.get())) {
				const CassRow* row = cass_iterator_get_row(iterator.get());
				CassUuid station;
				cass_value_get_uuid(cass_row_get_column(row,0), &station);
				const char *icaoStr;
				size_t icaoLength;
				cass_value_get_string(cass_row_get_column(row,1), &icaoStr, &icaoLength);
				if (icaoLength != 0)
					stations.emplace_back(station, std::string{icaoStr, icaoLength});
			}
			ret = true;
		}

		return ret;
	}

	bool DbConnectionObservations::deleteDataPoints(const CassUuid& station, const date::sys_days& day, const date::sys_seconds& start, const date::sys_seconds& end)
	{
		CassFuture* query;
		{ /* mutex scope */
			std::lock_guard<std::mutex> queryMutex{_deleteDataPointsMutex};
			std::cerr << "About to delete records from the database" << std::endl;
			CassStatement* statement = cass_prepared_bind(_deleteDataPoints.get());
			cass_statement_bind_uuid(statement, 0, station);
			cass_statement_bind_uint32(statement, 1, from_sysdays_to_CassandraDate(day));
			cass_statement_bind_int64(statement, 2, from_systime_to_CassandraDateTime(start));
			cass_statement_bind_int64(statement, 3, from_systime_to_CassandraDateTime(end));
			query = cass_session_execute(_session.get(), statement);
			cass_statement_free(statement);
		}

		const CassResult* result = cass_future_get_result(query);
		bool ret = true;
		if (!result) {
			const char* error_message;
			size_t error_message_length;
			cass_future_error_message(query, &error_message, &error_message_length);
			std::cerr << "Error from Cassandra: " << error_message << std::endl;
			ret = false;
		}
		cass_result_free(result);
		cass_future_free(query);

		return ret;
	}
}
