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
#include <exception>
#include <vector>
#include <utility>

#include <cassandra.h>
#include <syslog.h>
#include <unistd.h>
#include <date.h>

#include "dbconnection_observations.h"
#include "observation.h"

namespace meteodata {
	DbConnectionObservations::DbConnectionObservations(const std::string& address, const std::string& user, const std::string& password) :
		DbConnectionCommon(address, user, password),
		_selectStationByCoords{nullptr, &cass_prepared_free},
		_selectAllIcaos{nullptr, &cass_prepared_free},
		_selectLastDataInsertionTime{nullptr, &cass_prepared_free},
		_selectLastDataBefore{nullptr, &cass_prepared_free},
		_insertDataPoint{nullptr, &cass_prepared_free},
		_insertDataPointInNewDB{nullptr, &cass_prepared_free},
		_insertEntireDayValuesInNewDB{nullptr, &cass_prepared_free},
		_insertDataPointInMonitoringDB{nullptr, &cass_prepared_free},
		_updateLastArchiveDownloadTime{nullptr, &cass_prepared_free},
		_selectWeatherlinkStations{nullptr, &cass_prepared_free},
		_deleteDataPoints{nullptr, &cass_prepared_free}
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

		prepareFuture = cass_session_prepare(_session.get(), "SELECT "
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
			"insolation_time "
			" FROM meteodata_v2.meteo WHERE station = ? AND day = ? AND time <= ? ORDER BY time DESC LIMIT 1");
		rc = cass_future_error_code(prepareFuture);
		if (rc != CASS_OK) {
			std::string desc("Could not prepare statement selectLastDataBefore: ");
			desc.append(cass_error_desc(rc));
			throw std::runtime_error(desc);
		}
		_selectLastDataBefore.reset(cass_future_get_prepared(prepareFuture));
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
			"?,"			// "station,"
			"?,"			// "time,"
			"?,?,?,?,"		// "bartrend,barometer,barometer_abs,barometer_raw,"
			"?,?,"			// "insidetemp,outsidetemp,"
			"?,?,"			// "insidehum,outsidehum,"
			"?,?,?,?,"		// "extratemp1,extratemp2, extratemp3,extratemp4,"
				"?,?,?,"	// 	"extratemp5, extratemp6,extratemp7,"
			"?,?,?,?,"		// "soiltemp1, soiltemp2, soiltemp3, soiltemp4,"
			"?,?,?,?,"		// "leaftemp1, leaftemp2, leaftemp3, leaftemp4,"
			"?,?,?,?,"		// "extrahum1, extrahum2, extrahum3, extrahum4,"
				"?,?,?,"	// 	"extrahum5, extrahum6, extrahum7,"
			"?,?,?,"		// "soilmoistures1, soilmoistures2, soilmoistures3,"
				"?,"		// 	"soilmoistures4,"
			"?,?,?,"		// "leafwetnesses1, leafwetnesses2, leafwetnesses3,"
				"?,"		// 	"leafwetnesses4,"
			"?,?,"			// "windspeed, winddir,"
			"?,?,"			// "avgwindspeed_10min, avgwindspeed_2min,"
			"?,?,"			// "windgust_10min, windgustdir,"
			"?,?,?,?,"		// "rainrate, rain_15min, rain_1h, rain_24h,"
			"?,?,?,"		// "dayrain, monthrain, yearrain,"
			"?,?,"			// "stormrain, stormstartdate,"
			"?,?,"			// "UV, solarrad,"
			"?,?,?,?,"		// "dewpoint, heatindex, windchill, thswindex,"
			"?,?,?,"		// "dayET, monthET, yearET,"
			"?,?,"			// "forecast, forecast_icons,"
			"?,?,"			// "sunrise, sunset,"
			"?,?)");		// "rain_archive, etp_archive,"

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
			"insolation_time) "
			" VALUES ("
			"?,"		// "station,"
			"?, ?,"		// "day, time,"
			"?,"		// "barometer,"
			"?,"		// "dewpoint,"
			"?, ?,"		// "extrahum1, extrahum2,"
			"?,?, ?,"	// "extratemp1,extratemp2, extratemp3,"
			"?,"		// "heatindex,"
			"?,?,"		// "insidehum,insidetemp,"
			"?, ?,"		// "leaftemp1, leaftemp2,"
			"?, ?,"		// "leafwetnesses1, leafwetnesses2,"
			"?,?,"		// "outsidehum,outsidetemp,"
			"?, ?,"		// "rainrate, rainfall,"
			"?,"		// "et,"
			"?, ?, ?,"	// "soilmoistures1, soilmoistures2, soilmoistures3,"
				"?,"	// 	"soilmoistures4,"
			"?, ?, ?, ?,"	// "soiltemp1, soiltemp2, soiltemp3, soiltemp4,"
			"?,"		// "solarrad,"
			"?,"		// "thswindex,"
			"?,"		// "uv,"
			"?,"		// "windchill,"
			"?, ?, ?,"	// "winddir, windgust, windspeed,"
			"?)");		// "insolation_time)"

		rc = cass_future_error_code(prepareFuture);
		if (rc != CASS_OK) {
			std::string desc("Could not prepare statement insertdataPointInNewDB: ");
			desc.append(cass_error_desc(rc));
			throw std::runtime_error(desc);
		}
		_insertDataPointInNewDB.reset(cass_future_get_prepared(prepareFuture));
		cass_future_free(prepareFuture);

		prepareFuture = cass_session_prepare(_session.get(),
			"INSERT INTO meteodata_v2.meteo ("
			"station,"
			"day, time,"
			"rainfall24, insolation_time24) "
			" VALUES ("
			"?,"		// "station,"
			"?, ?,"		// "day, time,"
			"?, ?)");	// "rainfall24, insolation_time24)"

		rc = cass_future_error_code(prepareFuture);
		if (rc != CASS_OK) {
			std::string desc("Could not prepare statement insertEntireDayValuesInNewDB: ");
			desc.append(cass_error_desc(rc));
			throw std::runtime_error(desc);
		}
		_insertEntireDayValuesInNewDB.reset(cass_future_get_prepared(prepareFuture));
		cass_future_free(prepareFuture);

		prepareFuture = cass_session_prepare(_session.get(),
			"INSERT INTO meteodata_v2.monitoring_observations ("
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
			std::string desc("Could not prepare statement insertdataPointInMonitoringDB: ");
			desc.append(cass_error_desc(rc));
			throw std::runtime_error(desc);
		}
		_insertDataPointInMonitoringDB.reset(cass_future_get_prepared(prepareFuture));
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

	bool DbConnectionObservations::getLastDataInsertionTime(const CassUuid& uuid, time_t& lastDataInsertionTime)
	{
		std::cerr << "About to execute statement getLastDataInsertionTime" << std::endl;
		std::unique_ptr<CassStatement, void(&)(CassStatement*)> statement{
			cass_prepared_bind(_selectLastDataInsertionTime.get()),
			cass_statement_free
		};
		cass_statement_bind_uuid(statement.get(), 0, uuid);
		std::unique_ptr<CassFuture, void(&)(CassFuture*)> query{
			cass_session_execute(_session.get(), statement.get()),
			cass_future_free
		};
		std::unique_ptr<const CassResult, void(&)(const CassResult*)> result{
			cass_future_get_result(query.get()),
			cass_result_free
		};

		bool ret = false;
		if (result) {
			const CassRow* row = cass_result_first_row(result.get());
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

		return ret;
	}

	bool DbConnectionObservations::getLastDataBefore(const CassUuid& station, time_t boundary, Observation& obs)
	{
		std::cerr << "About to execute statement getLastDataBefore" << std::endl;
		std::unique_ptr<CassStatement, void(&)(CassStatement*)> statement{
			cass_prepared_bind(_selectLastDataBefore.get()),
			cass_statement_free
		};
		cass_statement_bind_uuid(statement.get(), 0, station);
		cass_statement_bind_uint32(statement.get(), 1, cass_date_from_epoch(boundary));
		cass_statement_bind_int64(statement.get(), 2, boundary * 1000);
		std::unique_ptr<CassFuture, void(&)(CassFuture*)> query{
			cass_session_execute(_session.get(), statement.get()),
			cass_future_free
		};
		std::unique_ptr<const CassResult, void(&)(const CassResult*)> result{
			cass_future_get_result(query.get()),
			cass_result_free
		};

		bool ret = false;
		if (result) {
			const CassRow* row = cass_result_first_row(result.get());
			if (row) {
				// First three columns are the primary key so we don't expect them to be null
				const CassValue* value = cass_row_get_column(row, 0);
				CassUuid u;
				cass_value_get_uuid(value, &u);
				//std::cerr << "We have a UUID" << std::endl;
				obs.setStation(u);
				// Discard the date, and deal only with the timestamp
				value = cass_row_get_column(row, 2);
				cass_int64_t t;
				cass_value_get_int64(value, &t);
				obs.setTimestamp(date::sys_seconds{chrono::seconds(t / 1000)});

				// Then, all the rest
				for (const auto& var : {
						"barometer", "dewpoint", "extratemp1", "extratemp2",
						"extratemp3", "heatindex", "insidetemp", "leaftemp1",
						"leaftemp2", "outsidetemp", "rainrate", "rainfall",
						"et", "soiltemp1", "soiltemp2", "soiltemp3", "soiltemp4",
						"thswindex", "windchill", "windgust", "windspeed"
					}) {
					value = cass_row_get_column_by_name(row, var);
					if (!cass_value_is_null(value)) {
						float f;
						//std::cerr << "We have variable " << var << std::endl;
						cass_value_get_float(value, &f);
						obs.set(var, f);
					}
				}
				for (const auto& var : {
						"insidehum", "leafwetnesses1", "leafwetnesses2",
						"outsidehum", "soilmoistures1", "soilmoistures2",
						"soilmoistures3", "soilmoistures4", "uv", "winddir",
						"solarrad", "insolation_time"
					}) {
					value = cass_row_get_column_by_name(row, var);
					if (!cass_value_is_null(value)) {
						cass_int32_t i;
						//std::cerr << "We have variable " << var << std::endl;
						cass_value_get_int32(value, &i);
						obs.set(var, i);
					}
				}
			}
		}

		return ret;
	}

	bool DbConnectionObservations::getStationByCoords(int elevation, int latitude, int longitude, CassUuid& station, std::string& name, int& pollPeriod, time_t& lastArchiveDownloadTime, time_t& lastDataInsertionTime)
	{
		std::cerr << "About to execute statement getStationByCoords" << std::endl;
		std::unique_ptr<CassStatement, void(&)(CassStatement*)> statement{
			cass_prepared_bind(_selectStationByCoords.get()),
			cass_statement_free
		};
		cass_statement_bind_int32(statement.get(), 0, elevation);
		cass_statement_bind_int32(statement.get(), 1, latitude);
		cass_statement_bind_int32(statement.get(), 2, longitude);
		std::unique_ptr<CassFuture, void(&)(CassFuture*)> query{
			cass_session_execute(_session.get(), statement.get()),
			cass_future_free
		};
		std::unique_ptr<const CassResult, void(&)(const CassResult*)> result{
			cass_future_get_result(query.get()),
			cass_result_free
		};

		bool ret = false;
		if (result) {
			const CassRow* row = cass_result_first_row(result.get());
			if (row) {
				cass_value_get_uuid(cass_row_get_column(row,0), &station);
				ret = getStationDetails(station, name, pollPeriod, lastArchiveDownloadTime);
				if (ret)
					getLastDataInsertionTime(station, lastDataInsertionTime);
			}
		}

		return ret;
	}

	bool DbConnectionObservations::insertDataPoint(const CassUuid station, const Message& msg)
	{
		std::cerr << "About to insert data point in database" << std::endl;
		std::unique_ptr<CassStatement, void(&)(CassStatement*)> statement{
			cass_prepared_bind(_insertDataPoint.get()),
			cass_statement_free
		};
		msg.populateDataPoint(station, statement.get());
		std::unique_ptr<CassFuture, void(&)(CassFuture*)> query{
			cass_session_execute(_session.get(), statement.get()),
			cass_future_free
		};
		std::unique_ptr<const CassResult, void(&)(const CassResult*)> result{
			cass_future_get_result(query.get()),
			cass_result_free
		};

		bool ret = true;
		if (!result) {
			const char* error_message;
			size_t error_message_length;
			cass_future_error_message(query.get(), &error_message, &error_message_length);
			std::cerr << "Error from Cassandra: " << error_message << std::endl;
			ret = false;
		}

		return ret;
	}

	bool DbConnectionObservations::insertV2DataPoint(const CassUuid station, const Message& msg)
	{
		std::cerr << "About to insert data point in database" << std::endl;
		std::unique_ptr<CassStatement, void(&)(CassStatement*)> statement{
			cass_prepared_bind(_insertDataPointInNewDB.get()),
			cass_statement_free
		};
		msg.populateV2DataPoint(station, statement.get());
		std::unique_ptr<CassFuture, void(&)(CassFuture*)> query{
			cass_session_execute(_session.get(), statement.get()),
			cass_future_free
		};
		std::unique_ptr<const CassResult, void(&)(const CassResult*)> result{
			cass_future_get_result(query.get()),
			cass_result_free
		};

		bool ret = true;
		if (!result) {
			const char* error_message;
			size_t error_message_length;
			cass_future_error_message(query.get(), &error_message, &error_message_length);
			std::cerr << "Error from Cassandra: " << error_message << std::endl;
			ret = false;
		}

		return ret;
	}

	bool DbConnectionObservations::insertV2EntireDayValues(const CassUuid station, const time_t& time, std::pair<bool, float> rainfall24, std::pair<bool, int> insolationTime24)
	{
		std::cerr << "About to insert entire day values in database" << std::endl;
		std::unique_ptr<CassStatement, void(&)(CassStatement*)> statement{
			cass_prepared_bind(_insertEntireDayValuesInNewDB.get()),
			cass_statement_free
		};
		cass_statement_bind_uuid(statement.get(), 0, station);
		cass_statement_bind_uint32(statement.get(), 1, cass_date_from_epoch(time));
		cass_statement_bind_int64(statement.get(), 2, time * 1000);
		if (rainfall24.first)
			cass_statement_bind_float(statement.get(), 3, rainfall24.second);
		if (insolationTime24.first)
			cass_statement_bind_int32(statement.get(), 4, insolationTime24.second);
		std::unique_ptr<CassFuture, void(&)(CassFuture*)> query{
			cass_session_execute(_session.get(), statement.get()),
			cass_future_free
		};
		std::unique_ptr<const CassResult, void(&)(const CassResult*)> result{
			cass_future_get_result(query.get()),
			cass_result_free
		};

		bool ret = true;
		if (!result) {
			const char* error_message;
			size_t error_message_length;
			cass_future_error_message(query.get(), &error_message, &error_message_length);
			std::cerr << "Error from Cassandra: " << error_message << std::endl;
			ret = false;
		}

		return ret;
	}

	bool DbConnectionObservations::insertMonitoringDataPoint(const CassUuid station, const Message& msg)
	{
		std::cerr << "About to insert reconstructed data point in database" << std::endl;
		std::unique_ptr<CassStatement, void(&)(CassStatement*)> statement{
			cass_prepared_bind(_insertDataPointInMonitoringDB.get()),
			cass_statement_free
		};
		msg.populateV2DataPoint(station, statement.get());
		std::unique_ptr<CassFuture, void(&)(CassFuture*)> query{
			cass_session_execute(_session.get(), statement.get()),
			cass_future_free
		};
		std::unique_ptr<const CassResult, void(&)(const CassResult*)> result{
			cass_future_get_result(query.get()),
			cass_result_free
		};

		bool ret = true;
		if (!result) {
			const char* error_message;
			size_t error_message_length;
			cass_future_error_message(query.get(), &error_message, &error_message_length);
			std::cerr << "Error from Cassandra: " << error_message << std::endl;
			ret = false;
		}

		return ret;
	}

	bool DbConnectionObservations::updateLastArchiveDownloadTime(const CassUuid station, const time_t& time)
	{
		std::cerr << "About to update an archive download time in database" << std::endl;
		std::unique_ptr<CassStatement, void(&)(CassStatement*)> statement{
			cass_prepared_bind(_updateLastArchiveDownloadTime.get()),
			cass_statement_free
		};
		cass_statement_bind_int64(statement.get(), 0, time * 1000);
		cass_statement_bind_uuid(statement.get(), 1, station);

		std::unique_ptr<CassFuture, void(&)(CassFuture*)> query{
			cass_session_execute(_session.get(), statement.get()),
			cass_future_free
		};
		std::unique_ptr<const CassResult, void(&)(const CassResult*)> result{
			cass_future_get_result(query.get()),
			cass_result_free
		};

		bool ret = true;
		if (!result) {
			const char* error_message;
			size_t error_message_length;
			cass_future_error_message(query.get(), &error_message, &error_message_length);
			std::cerr << "Error from Cassandra: " << error_message << std::endl;
			ret = false;
		}

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
		std::unique_ptr<const CassResult, void(&)(const CassResult*)> result{
			cass_future_get_result(query.get()),
			cass_result_free
		};
		bool ret = false;
		if (result) {
			std::unique_ptr<CassIterator, void(&)(CassIterator*)> iterator{
				cass_iterator_from_result(result.get()),
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
		std::unique_ptr<const CassResult, void(&)(const CassResult*)> result{
			cass_future_get_result(query.get()),
			cass_result_free
		};
		bool ret = false;
		if (result) {
			std::unique_ptr<CassIterator, void(&)(CassIterator*)> iterator{
				cass_iterator_from_result(result.get()),
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
		std::cerr << "About to delete records from the database" << std::endl;
		std::unique_ptr<CassStatement, void(&)(CassStatement*)> statement{
			cass_prepared_bind(_deleteDataPoints.get()),
			cass_statement_free
		};
		cass_statement_bind_uuid(statement.get(), 0, station);
		cass_statement_bind_uint32(statement.get(), 1, from_sysdays_to_CassandraDate(day));
		cass_statement_bind_int64(statement.get(), 2, from_systime_to_CassandraDateTime(start));
		cass_statement_bind_int64(statement.get(), 3, from_systime_to_CassandraDateTime(end));

		std::unique_ptr<CassFuture, void(&)(CassFuture*)> query{
			cass_session_execute(_session.get(), statement.get()),
			cass_future_free
		};
		std::unique_ptr<const CassResult, void(&)(const CassResult*)> result{
			cass_future_get_result(query.get()),
			cass_result_free
		};

		bool ret = true;
		if (!result) {
			const char* error_message;
			size_t error_message_length;
			cass_future_error_message(query.get(), &error_message, &error_message_length);
			std::cerr << "Error from Cassandra: " << error_message << std::endl;
			ret = false;
		}

		return ret;
	}
}
