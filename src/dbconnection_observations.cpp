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
#include <memory>
#include <cstring>
#include <map>

#include <cassandra.h>
#include <syslog.h>
#include <unistd.h>
#include <date.h>

#include "dbconnection_observations.h"
#include "observation.h"
#include "cassandra_stmt_ptr.h"

namespace meteodata {
	DbConnectionObservations::DbConnectionObservations(const std::string& address, const std::string& user, const std::string& password) :
		DbConnectionCommon(address, user, password)
	{
		DbConnectionObservations::prepareStatements();
	}

	void DbConnectionObservations::prepareStatements()
	{
		prepareOneStatement(_selectStationByCoords,
			"SELECT station FROM meteodata.coordinates "
			"WHERE elevation = ? AND latitude = ? AND longitude = ?"
		);

		prepareOneStatement(_selectStationCoordinates,
			"SELECT latitude,longitude,elevation,name,polling_period FROM meteodata.stations "
			"WHERE id = ?"
		);

		prepareOneStatement(_selectAllIcaos,
			"SELECT id,icao FROM meteodata.stationsFR"
		);

		prepareOneStatement(_selectDeferredSynops,
			"SELECT uuid,icao FROM meteodata.deferred_synops"
		);

		prepareOneStatement(_selectLastDataBefore,
			"SELECT "
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
			"insolation_time, "
			"soilmoistures10cm, soilmoistures20cm, "
			"soilmoistures30cm, soilmoistures40cm, "
			"soilmoistures50cm, soilmoistures60cm, "
			"soiltemp10cm, soiltemp20cm, "
			"soiltemp30cm, soiltemp40cm, "
			"soiltemp50cm, soiltemp60cm "
			" FROM meteodata_v2.meteo WHERE station = ? "
			" AND day = ? AND time <= ? ORDER BY time DESC LIMIT 1"
		);

		prepareOneStatement(_insertV2RawDataPoint,
			"INSERT INTO meteodata_v2.raw_meteo ("
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
			"insolation_time,"
			"min_outside_temperature, max_outside_temperature,"
			"leafwetnesses_timeratio1, "
			"soilmoistures10cm, soilmoistures20cm, "
			"soilmoistures30cm, soilmoistures40cm, "
			"soilmoistures50cm, soilmoistures60cm, "
			"soiltemp10cm, soiltemp20cm, "
			"soiltemp30cm, soiltemp40cm, "
			"soiltemp50cm, soiltemp60cm "
			") "
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
			"?,"		// "insolation_time"
			"?,?,"		// "min_outside_temperature, max_outside_temperature"
			"?,?,?,"	// "soilmoistures10cm, soilmoistures20cm, soilmoistures30cm"
			"?,?,?,"	// "soilmoistures40cm, soilmoistures50cm, soilmoistures60cm"
			"?,?,?,"	// "soiltemp10cm, soiltemp20cm, soiltemp30cm"
			"?,?,?,"	// "soiltemp40cm, soiltemp50cm, soiltemp60cm"
			"?"		// "leafwetnesses_timeratio1"
			")"
		);

		prepareOneStatement(_insertV2FilteredDataPoint,
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
			"insolation_time,"
			"min_outside_temperature, max_outside_temperature,"
			"leafwetnesses_timeratio1, "
			"soilmoistures10cm, soilmoistures20cm, "
			"soilmoistures30cm, soilmoistures40cm, "
			"soilmoistures50cm, soilmoistures60cm, "
			"soiltemp10cm, soiltemp20cm, "
			"soiltemp30cm, soiltemp40cm, "
			"soiltemp50cm, soiltemp60cm "
			") "
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
			"?,"		// "soilmoistures4,"
			"?, ?, ?, ?,"	// "soiltemp1, soiltemp2, soiltemp3, soiltemp4,"
			"?,"		// "solarrad,"
			"?,"		// "thswindex,"
			"?,"		// "uv,"
			"?,"		// "windchill,"
			"?, ?, ?,"	// "winddir, windgust, windspeed,"
			"?,"		// "insolation_time"
			"?,?,"		// "min_outside_temperature, max_outside_temperature"
			"?,"		// "leafwetnesses_timeratio1"
			"?,?,?,"	// "soilmoistures10cm, soilmoistures20cm, soilmoistures30cm"
			"?,?,?,"	// "soilmoistures40cm, soilmoistures50cm, soilmoistures60cm"
			"?,?,?,"	// "soiltemp10cm, soiltemp20cm, soiltemp30cm"
			"?,?,?"		// "soiltemp40cm, soiltemp50cm, soiltemp60cm"
			")"
		);

		prepareOneStatement(_insertEntireDayValues,
			"INSERT INTO meteodata_v2.meteo ("
			"station,"
			"day, time,"
			"rainfall24, insolation_time24) "
			" VALUES ("
			"?,"		// "station,"
			"?, ?,"		// "day, time,"
			"?, ?)"		// "rainfall24, insolation_time24)"
		);

		prepareOneStatement(_insertTx,
			"INSERT INTO meteodata_v2.meteo ("
			"station,"
			"day, time,"
			"tx) "
			" VALUES ("
			"?,"		// "station,"
			"?, ?,"		// "day, time,"
			"?)"		// "tx"
		);

		prepareOneStatement(_insertTn,
			"INSERT INTO meteodata_v2.meteo ("
			"station,"
			"day, time,"
			"tn) "
			" VALUES ("
			"?,"		// "station,"
			"?, ?,"		// "day, time,"
			"?)"		// "tn"
		);

		prepareOneStatement(_insertDataPointInMonitoringDB,
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
			"?)"
		);

		prepareOneStatement(_updateLastArchiveDownloadTime,
			"UPDATE meteodata.stations SET last_archive_download = ? WHERE id = ?"
		);

		prepareOneStatement(_selectWeatherlinkStations,
			"SELECT station, active, auth, api_token, tz FROM meteodata.weatherlink"
		);

		prepareOneStatement(_selectWeatherlinkAPIv2Stations,
			"SELECT station, active, archived, substations, weatherlink_id, parsers FROM meteodata.weatherlink_apiv2"
		);

		prepareOneStatement(_selectMqttStations,
			"SELECT station, active, host, port, user, password, topic, tz FROM meteodata.mqtt"
		);

		prepareOneStatement(_selectFieldClimateApiStations,
			"SELECT station, active, fieldclimate_id, sensors, tz FROM meteodata.fieldclimate"
		);

		prepareOneStatement(_selectObjeniousApiStations,
			"SELECT station, active, objenious_id, variables FROM meteodata.objenious"
		);

		prepareOneStatement(_selectLiveobjectsStations,
			"SELECT station, active, stream_id, topic_prefix FROM meteodata.liveobjects"
		);

		prepareOneStatement(_selectCimelStations,
			"SELECT station, active, cimelid, tz FROM meteodata.cimel"
		);

		prepareOneStatement(_selectStatICTxtStations,
			"SELECT station, active, host, url, https, tz, sensors FROM meteodata.statictxt"
		);

		prepareOneStatement(_selectMBDataTxtStations,
			"SELECT station, active, host, url, https, tz, type FROM meteodata.mbdatatxt"
		);

		prepareOneStatement(_getRainfall,
			"SELECT SUM(rainfall) FROM meteodata_v2.meteo "
			"WHERE station = ? AND day = ? AND time > ? AND time <= ?"
		);

		prepareOneStatement(_deleteDataPoints,
			"DELETE FROM meteodata_v2.meteo WHERE station=? AND day=? AND time>? AND time<=?"
		);

		prepareOneStatement(_selectTx,
			"SELECT tx FROM meteodata_v2.meteo WHERE station=? AND day=? LIMIT 1"
		);

		prepareOneStatement(_selectTn,
			"SELECT tn FROM meteodata_v2.meteo WHERE station=? AND day=? LIMIT 1"
		);

		prepareOneStatement(_selectCached,
			"SELECT time, value_int, value_float FROM meteodata_v2.cache WHERE station=? AND cache_key=?"
		);

		prepareOneStatement(_insertIntoCache,
			"INSERT INTO meteodata_v2.cache (station, cache_key, time, value_int, value_float) VALUES (?, ?, ?, ?, ?)"
		);
	}

	bool DbConnectionObservations::getLastDataBefore(const CassUuid& station, time_t boundary, Observation& obs)
	{
		std::unique_ptr<CassStatement, void(&)(CassStatement*)> statement{
			cass_prepared_bind(_selectLastDataBefore.get()),
			cass_statement_free
		};
		cass_statement_set_is_idempotent(statement.get(), cass_true);
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
						"thswindex", "windchill", "windgust", "windspeed",
						"soilmoistures10cm", "soilmoistures20cm", "soilmoistures30cm",
						"soilmoistures40cm", "soilmoistures50cm", "soilmoistures60cm",
						"soiltemp10cm", "soiltemp20cm", "soiltemp30cm",
						"soiltemp40cm", "soiltemp50cm", "soiltemp60cm"
					}) {
					value = cass_row_get_column_by_name(row, var);
					if (!cass_value_is_null(value)) {
						float f;
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
						cass_value_get_int32(value, &i);
						obs.set(var, i);
					}
				}
			}
		}

		return ret;
	}

	bool DbConnectionObservations::getStationByCoords(int elevation, int latitude, int longitude, CassUuid& station, std::string& name, int& pollPeriod, time_t& lastArchiveDownloadTime)
	{
		std::unique_ptr<CassStatement, void(&)(CassStatement*)> statement{
			cass_prepared_bind(_selectStationByCoords.get()),
			cass_statement_free
		};
		cass_statement_set_is_idempotent(statement.get(), cass_true);
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
			}
		}

		return ret;
	}

	bool DbConnectionObservations::getStationCoordinates(CassUuid station, float& latitude, float& longitude, int& elevation, std::string& name, int& pollPeriod)
	{
		std::unique_ptr<CassStatement, void(&)(CassStatement*)> statement{
			cass_prepared_bind(_selectStationCoordinates.get()),
			cass_statement_free
		};
		cass_statement_set_is_idempotent(statement.get(), cass_true);
		cass_statement_bind_uuid(statement.get(), 0, station);
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
				cass_value_get_float(cass_row_get_column(row,0), &latitude);
				cass_value_get_float(cass_row_get_column(row,1), &longitude);
				cass_value_get_int32(cass_row_get_column(row,2), &elevation);
				const char *nameStr;
				size_t sizeName;
				const CassValue* v = cass_row_get_column(row, 3);
				if (!cass_value_is_null(v)) {
					cass_value_get_string(cass_row_get_column(row, 3), &nameStr, &sizeName);
					name.assign(nameStr, sizeName);
				}
				cass_value_get_int32(cass_row_get_column(row,4), &pollPeriod);
			}
		}

		return ret;
	}

	bool DbConnectionObservations::insertV2DataPoint(const CassUuid station, const Message& msg)
	{
		std::unique_ptr<CassStatement, void(&)(CassStatement*)> statement{
			cass_prepared_bind(_insertV2FilteredDataPoint.get()),
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
			ret = false;
		}

		return ret;
	}

	void DbConnectionObservations::populateV2InsertionQuery(CassStatement* statement, const Observation& obs)
	{
		/*************************************************************/
		cass_statement_bind_uuid(statement, 0, obs.station);
		/*************************************************************/
		cass_statement_bind_uint32(statement, 1, cass_date_from_epoch(obs.time.time_since_epoch().count()));
		cass_statement_bind_int64(statement, 2, 1000*obs.time.time_since_epoch().count()); // in ms
		/*************************************************************/
		if (obs.barometer.first)
			cass_statement_bind_float(statement, 3, obs.barometer.second);
		/*************************************************************/
		if (obs.dewpoint.first)
			cass_statement_bind_float(statement, 4, obs.dewpoint.second);
		/*************************************************************/
		for (int i=0 ; i<2 ; i++) {
			if (obs.extrahum[i].first)
				cass_statement_bind_int32(statement, 5+i, obs.extrahum[i].second);
		}
		/*************************************************************/
		for (int i=0 ; i<3 ; i++) {
			if (obs.extratemp[i].first)
				cass_statement_bind_float(statement, 7+i, obs.extratemp[i].second);
		}
		/*************************************************************/
		if (obs.heatindex.first)
			cass_statement_bind_float(statement, 10, obs.heatindex.second);
		/*************************************************************/
		// Do not record inside hum
		/*************************************************************/
		// Do not record inside temp
		/*************************************************************/
		for (int i=0 ; i<2 ; i++) {
			if (obs.leaftemp[i].first)
				cass_statement_bind_float(statement, 13+i, obs.leaftemp[i].second);
			if (obs.leafwetnesses[i].first)
				cass_statement_bind_int32(statement, 15+i, obs.leafwetnesses[i].second);
		}
		/*************************************************************/
		if (obs.outsidehum.first)
			cass_statement_bind_int32(statement, 17, obs.outsidehum.second);
		/*************************************************************/
		if (obs.outsidetemp.first)
			cass_statement_bind_float(statement, 18, obs.outsidetemp.second);
		/*************************************************************/
		if (obs.rainrate.first)
			cass_statement_bind_float(statement, 19, obs.rainrate.second);
		/*************************************************************/
		if (obs.rainfall.first)
			cass_statement_bind_float(statement, 20, obs.rainfall.second);
		/*************************************************************/
		if (obs.et.first)
			cass_statement_bind_float(statement, 21, obs.et.second);
		/*************************************************************/
		for (int i=0 ; i<4 ; i++) {
			if (obs.soilmoistures[i].first)
				cass_statement_bind_int32(statement, 22+i, obs.soilmoistures[i].second);
		}
		/*************************************************************/
		for (int i=0 ; i<4 ; i++) {
			if (obs.soiltemp[i].first)
				cass_statement_bind_float(statement, 26+i, obs.soiltemp[i].second);
		}
		/*************************************************************/
		if (obs.solarrad.first)
			cass_statement_bind_int32(statement, 30, obs.solarrad.second);
		/*************************************************************/
		if (obs.thswindex.first)
			cass_statement_bind_float(statement, 31, obs.thswindex.second);
		/*************************************************************/
		if (obs.uv.first)
			cass_statement_bind_int32(statement, 32, obs.uv.second);
		/*************************************************************/
		if (obs.windchill.first)
			cass_statement_bind_float(statement, 33, obs.windchill.second);
		/*************************************************************/
		if (obs.winddir.first)
			cass_statement_bind_int32(statement, 34, obs.winddir.second);
		/*************************************************************/
		if (obs.windgust.first)
			cass_statement_bind_float(statement, 35, obs.windgust.second);
		/*************************************************************/
		if (obs.windspeed.first)
			cass_statement_bind_float(statement, 36, obs.windspeed.second);
		/*************************************************************/
		if (obs.insolation_time.first)
			cass_statement_bind_int32(statement, 37, obs.insolation_time.second);
		/*************************************************************/
		if (obs.min_outside_temperature.first)
			cass_statement_bind_float(statement, 38, obs.min_outside_temperature.second);
		/*************************************************************/
		if (obs.max_outside_temperature.first)
			cass_statement_bind_float(statement, 39, obs.max_outside_temperature.second);
		/*************************************************************/
		if (obs.leafwetness_timeratio1.first)
			cass_statement_bind_int32(statement, 40, obs.leafwetness_timeratio1.second);
		/*************************************************************/
		if (obs.soilmoistures10cm.first)
			cass_statement_bind_float(statement, 41, obs.soilmoistures10cm.second);
		if (obs.soilmoistures20cm.first)
			cass_statement_bind_float(statement, 42, obs.soilmoistures20cm.second);
		if (obs.soilmoistures30cm.first)
			cass_statement_bind_float(statement, 43, obs.soilmoistures30cm.second);
		if (obs.soilmoistures40cm.first)
			cass_statement_bind_float(statement, 44, obs.soilmoistures40cm.second);
		if (obs.soilmoistures50cm.first)
			cass_statement_bind_float(statement, 45, obs.soilmoistures50cm.second);
		if (obs.soilmoistures60cm.first)
			cass_statement_bind_float(statement, 46, obs.soilmoistures60cm.second);
		/*************************************************************/
		if (obs.soiltemp10cm.first)
			cass_statement_bind_float(statement, 47, obs.soiltemp10cm.second);
		if (obs.soiltemp20cm.first)
			cass_statement_bind_float(statement, 48, obs.soiltemp20cm.second);
		if (obs.soiltemp30cm.first)
			cass_statement_bind_float(statement, 49, obs.soiltemp30cm.second);
		if (obs.soiltemp40cm.first)
			cass_statement_bind_float(statement, 50, obs.soiltemp40cm.second);
		if (obs.soiltemp50cm.first)
			cass_statement_bind_float(statement, 51, obs.soiltemp50cm.second);
		if (obs.soiltemp60cm.first)
			cass_statement_bind_float(statement, 52, obs.soiltemp60cm.second);
		/*************************************************************/
	}

	bool DbConnectionObservations::insertV2DataPoint(const Observation& obs)
	{
		std::unique_ptr<CassStatement, void(&)(CassStatement*)> statement{
			cass_prepared_bind(_insertV2RawDataPoint.get()),
			cass_statement_free
		};

		populateV2InsertionQuery(statement.get(), obs);
		std::unique_ptr<CassFuture, void(&)(CassFuture*)> query{
			cass_session_execute(_session.get(), statement.get()),
			cass_future_free
		};
		std::unique_ptr<const CassResult, void(&)(const CassResult*)> result{
			cass_future_get_result(query.get()),
			cass_result_free
		};

		if (!result) {
			const char* error_message;
			size_t error_message_length;
			cass_future_error_message(query.get(), &error_message, &error_message_length);
			return false;
		}


		std::unique_ptr<CassStatement, void(&)(CassStatement*)> statement2{
			cass_prepared_bind(_insertV2FilteredDataPoint.get()),
			cass_statement_free
		};
		Observation copy{obs};
		copy.filterOutImpossibleValues();
		populateV2InsertionQuery(statement2.get(), copy);
		query.reset(cass_session_execute(_session.get(), statement2.get()));
		result.reset(cass_future_get_result(query.get()));

		if (!result) {
			const char* error_message;
			size_t error_message_length;
			cass_future_error_message(query.get(), &error_message, &error_message_length);
			return false;
		}
		return true;
	}

	bool DbConnectionObservations::insertV2EntireDayValues(const CassUuid station, const time_t& time, std::pair<bool, float> rainfall24, std::pair<bool, int> insolationTime24)
	{
		std::unique_ptr<CassStatement, void(&)(CassStatement*)> statement{
			cass_prepared_bind(_insertEntireDayValues.get()),
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
			ret = false;
		}

		return ret;
	}

	bool DbConnectionObservations::insertV2Tx(const CassUuid station, const time_t& time, float tx)
	{
		std::unique_ptr<CassStatement, void(&)(CassStatement*)> statement{
			cass_prepared_bind(_insertTx.get()),
			cass_statement_free
		};

		std::chrono::system_clock::time_point tp{std::chrono::seconds{time}};
		auto daypoint = date::floor<date::days>(tp);
		auto ymd = date::sys_days{date::year_month_day{daypoint}};
		auto tod = date::make_time(tp - daypoint);
		if (tod.hours().count() <= 6)
			ymd -= date::days(1);
		time_t correctedTime = std::chrono::system_clock::to_time_t(ymd);

		std::pair<bool, float> oldTx;
		if (!getTx(station, correctedTime, oldTx))
			return false;
		if (oldTx.first && tx <= oldTx.second)
			return true;

		cass_statement_bind_uuid(statement.get(), 0, station);
		cass_statement_bind_uint32(statement.get(), 1, cass_date_from_epoch(correctedTime));
		cass_statement_bind_int64(statement.get(), 2, correctedTime * 1000);
		cass_statement_bind_float(statement.get(), 3, tx);
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
			ret = false;
		}

		return ret;
	}

	bool DbConnectionObservations::insertV2Tn(const CassUuid station, const time_t& time, float tn)
	{
		std::unique_ptr<CassStatement, void(&)(CassStatement*)> statement{
			cass_prepared_bind(_insertTn.get()),
			cass_statement_free
		};

		std::chrono::system_clock::time_point tp{std::chrono::seconds{time}};
		auto daypoint = date::floor<date::days>(tp);
		auto ymd = date::sys_days{date::year_month_day{daypoint}};
		auto tod = date::make_time(tp - daypoint);
		if (tod.hours().count() > 18)
			ymd += date::days(1);
		time_t correctedTime = std::chrono::system_clock::to_time_t(ymd);

		std::pair<bool, float> oldTn;
		if (!getTn(station, correctedTime, oldTn))
			return false;
		if (oldTn.first && tn >= oldTn.second)
			return true;

		cass_statement_bind_uuid(statement.get(), 0, station);
		cass_statement_bind_uint32(statement.get(), 1, cass_date_from_epoch(correctedTime));
		cass_statement_bind_int64(statement.get(), 2, correctedTime * 1000);
		cass_statement_bind_float(statement.get(), 3, tn);
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
			ret = false;
		}

		return ret;
	}

	bool DbConnectionObservations::insertMonitoringDataPoint(const CassUuid station, const Message& msg)
	{
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
			ret = false;
		}

		return ret;
	}

	bool DbConnectionObservations::updateLastArchiveDownloadTime(const CassUuid station, const time_t& time)
	{
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
			ret = false;
		}

		return ret;
	}

	bool DbConnectionObservations::getAllWeatherlinkStations(std::vector<std::tuple<CassUuid, std::string, std::string, int>>& stations)
	{
		return performSelect(_selectWeatherlinkStations.get(),
			[&stations](const CassRow* row) {
				const CassValue* v = cass_row_get_column(row, 0);
				if (cass_value_is_null(v))
					return;
				CassUuid station;
				cass_value_get_uuid(v, &station);

				v = cass_row_get_column(row, 1);
				if (cass_value_is_null(v))
					return;
				cass_bool_t active;
				cass_value_get_bool(v, &active);

				v = cass_row_get_column(row, 2);
				if (cass_value_is_null(v))
					return;
				const char *authString;
				size_t sizeAuthString;
				cass_value_get_string(v, &authString, &sizeAuthString);

				std::string apiToken;
				const CassValue* raw = cass_row_get_column(row,3);
				if (!cass_value_is_null(raw)) {
					const char *token;
					size_t sizeToken;
					cass_value_get_string(raw, &token, &sizeToken);
					apiToken.assign(token, sizeToken);
				}

				int timezone;
				v = cass_row_get_column(row, 2);
				if (cass_value_is_null(v))
					timezone = 0;
				else
					cass_value_get_int32(cass_row_get_column(row,4), &timezone);
				if (active == cass_true)
					stations.emplace_back(station, std::string{authString, sizeAuthString}, apiToken, timezone);
			}
		);
	}

	bool DbConnectionObservations::getAllWeatherlinkAPIv2Stations(std::vector<std::tuple<CassUuid, bool, std::map<int, CassUuid>, std::string,
			std::map<int, std::map<std::string, std::string>> >>& stations)
	{
		return performSelect(_selectWeatherlinkAPIv2Stations.get(),
			[&stations](const CassRow* row) {
				const CassValue* v = cass_row_get_column(row, 0);
				if (cass_value_is_null(v))
					return;
				CassUuid station;
				cass_value_get_uuid(v, &station);

				v = cass_row_get_column(row, 1);
				if (cass_value_is_null(v))
					return;
				cass_bool_t active;
				cass_value_get_bool(v, &active);

				v = cass_row_get_column(row, 2);
				if (cass_value_is_null(v))
					return;
				cass_bool_t archived;
				cass_value_get_bool(v, &archived);

				std::map<int, CassUuid> mapping;
				const CassValue* mappingValue = cass_row_get_column(row, 3);
				if (!cass_value_is_null(mappingValue)) {
					std::unique_ptr<CassIterator, void(&)(CassIterator*)> mappingIterator{
						cass_iterator_from_map(mappingValue),
						cass_iterator_free
					};
					while (cass_iterator_next(mappingIterator.get())) {
						int sensorId;
						cass_value_get_int32(cass_iterator_get_map_key(mappingIterator.get()), &sensorId);
						CassUuid substation;
						cass_value_get_uuid(cass_iterator_get_map_value(mappingIterator.get()), &substation);
						mapping.emplace(sensorId, substation);
					}
				}

				v = cass_row_get_column(row, 4);
				if (cass_value_is_null(v))
					return;
				const char *weatherlinkId;
				size_t sizeWeatherlinkId;
				cass_value_get_string(v, &weatherlinkId, &sizeWeatherlinkId);


				std::map<int, std::map<std::string, std::string>> parsers;
				const CassValue* parsersValue = cass_row_get_column(row, 5);
				if (!cass_value_is_null(parsersValue)) {
					std::unique_ptr<CassIterator, void(&)(CassIterator*)> parsersIterator{
						cass_iterator_from_map(parsersValue),
						cass_iterator_free
					};
					while (cass_iterator_next(parsersIterator.get())) {
						int sensorId;
						cass_value_get_int32(cass_iterator_get_map_key(parsersIterator.get()), &sensorId);

						std::map<std::string, std::string> parser;
						std::unique_ptr<CassIterator, void(&)(CassIterator*)> variablesIterator{
								cass_iterator_from_map(cass_iterator_get_map_value(parsersIterator.get())),
								cass_iterator_free
						};
						while (cass_iterator_next(variablesIterator.get())) {
							const char *category;
							size_t sizeCategory;
							cass_value_get_string(cass_iterator_get_map_key(variablesIterator.get()), &category, &sizeCategory);

							const char *variable;
							size_t sizeVariable;
							cass_value_get_string(cass_iterator_get_map_value(variablesIterator.get()), &variable, &sizeVariable);

							parser.emplace(std::string{category, sizeCategory}, std::string{variable, sizeVariable});
						}
						parsers.emplace(sensorId, std::move(parser));
					}
				}

				if (active == cass_true)
					stations.emplace_back(station, archived == cass_true /* wierd way to cast to bool */, std::move(mapping),
							std::string{weatherlinkId, sizeWeatherlinkId}, std::move(parsers));
			}
		);
	}

	bool DbConnectionObservations::getMqttStations(std::vector<std::tuple<CassUuid, std::string, int, std::string, std::unique_ptr<char[]>, size_t, std::string, int>>& stations)
	{
		return performSelect(_selectMqttStations.get(),
			[&stations](const CassRow* row) {
				const CassValue* v = cass_row_get_column(row, 0);
				if (cass_value_is_null(v))
					return;
				CassUuid station;
				cass_value_get_uuid(v, &station);

				v = cass_row_get_column(row, 1);
				if (cass_value_is_null(v))
					return;
				cass_bool_t active;
				cass_value_get_bool(v, &active);

				v = cass_row_get_column(row, 2);
				if (cass_value_is_null(v))
					return;
				const char *host;
				size_t sizeHost;
				cass_value_get_string(v, &host, &sizeHost);

				v = cass_row_get_column(row, 3);
				if (cass_value_is_null(v))
					return;
				int port;
				cass_value_get_int32(v, &port);

				v = cass_row_get_column(row, 4);
				if (cass_value_is_null(v))
					return;
				const char *user;
				size_t sizeUser;
				cass_value_get_string(v, &user, &sizeUser);

				v = cass_row_get_column(row, 5);
				if (cass_value_is_null(v))
					return;
				const char *pw;
				size_t sizePw;
				cass_value_get_string(v, &pw, &sizePw);
				std::unique_ptr<char[]> pwCopy = std::make_unique<char[]>(sizePw+1);
				std::strncpy(pwCopy.get(), pw, sizePw);

				v = cass_row_get_column(row, 6);
				if (cass_value_is_null(v))
					return;
				const char *topic;
				size_t sizeTopic;
				cass_value_get_string(v, &topic, &sizeTopic);

				v = cass_row_get_column(row, 7);
				if (cass_value_is_null(v))
					return;
				int tz;
				cass_value_get_int32(v, &tz);

				if (active == cass_true)
					stations.emplace_back(station, std::string{host, sizeHost}, port, std::string{user, sizeUser}, std::move(pwCopy), sizePw, std::string{topic, sizeTopic}, tz);
			}
		);
	}

	bool DbConnectionObservations::getStatICTxtStations(std::vector<std::tuple<CassUuid, std::string, std::string, bool, int, std::map<std::string, std::string>>>& stations)
	{
		return performSelect(_selectStatICTxtStations.get(),
			[&stations](const CassRow* row) {
				const CassValue* v = cass_row_get_column(row, 0);
				if (cass_value_is_null(v))
					return;
				CassUuid station;
				cass_value_get_uuid(v, &station);

				v = cass_row_get_column(row, 1);
				if (cass_value_is_null(v))
					return;
				cass_bool_t active;
				cass_value_get_bool(v, &active);

				v = cass_row_get_column(row, 2);
				if (cass_value_is_null(v))
					return;
				const char *host;
				size_t sizeHost;
				cass_value_get_string(v, &host, &sizeHost);

				v = cass_row_get_column(row, 3);
				if (cass_value_is_null(v))
					return;
				const char *url;
				size_t sizeUrl;
				cass_value_get_string(v, &url, &sizeUrl);

				v = cass_row_get_column(row, 4);
				if (cass_value_is_null(v))
					return;
				cass_bool_t https;
				cass_value_get_bool(v, &https);

				v = cass_row_get_column(row, 5);
				if (cass_value_is_null(v))
					return;
				int tz;
				cass_value_get_int32(v, &tz);

				std::map<std::string, std::string> mapping;
				const CassValue* mappingValue = cass_row_get_column(row, 6);
				if (!cass_value_is_null(mappingValue)) {
					std::unique_ptr<CassIterator, void(&)(CassIterator*)> mappingIterator{
						cass_iterator_from_map(mappingValue),
						cass_iterator_free
					};
					while (cass_iterator_next(mappingIterator.get())) {
						const char* cKey;
						std::size_t sKey;
						cass_value_get_string(cass_iterator_get_map_key(mappingIterator.get()), &cKey, &sKey);
						std::string key{cKey, sKey};

						const char* cValue;
						std::size_t sValue;
						cass_value_get_string(cass_iterator_get_map_value(mappingIterator.get()), &cValue, &sValue);
						std::string value{cValue, sValue};

						mapping.emplace(key, value);
					}
				}

				if (active == cass_true)
					stations.emplace_back(station, std::string{host, sizeHost}, std::string{url, sizeUrl}, bool(https), tz, mapping);
			}
		);
	}

	bool DbConnectionObservations::getMBDataTxtStations(std::vector<std::tuple<CassUuid, std::string, std::string, bool, int, std::string>>& stations)
	{
		return performSelect(_selectMBDataTxtStations.get(),
			[&stations](const CassRow* row) {
				const CassValue* v = cass_row_get_column(row, 0);
				if (cass_value_is_null(v))
					return;
				CassUuid station;
				cass_value_get_uuid(v, &station);

				v = cass_row_get_column(row, 1);
				if (cass_value_is_null(v))
					return;
				cass_bool_t active;
				cass_value_get_bool(v, &active);

				v = cass_row_get_column(row, 2);
				if (cass_value_is_null(v))
					return;
				const char* host;
				size_t sizeHost;
				cass_value_get_string(v, &host, &sizeHost);

				v = cass_row_get_column(row, 3);
				if (cass_value_is_null(v))
					return;
				const char* url;
				size_t sizeUrl;
				cass_value_get_string(v, &url, &sizeUrl);

				v = cass_row_get_column(row, 4);
				if (cass_value_is_null(v))
					return;
				cass_bool_t https;
				cass_value_get_bool(v, &https);

				v = cass_row_get_column(row, 5);
				if (cass_value_is_null(v))
					return;
				int tz;
				cass_value_get_int32(v, &tz);

				v = cass_row_get_column(row, 6);
				if (cass_value_is_null(v))
					return;
				const char* type;
				size_t sizeType;
				cass_value_get_string(v, &type, &sizeType);
				if (active == cass_true)
					stations.emplace_back(station, std::string{host, sizeHost}, std::string{url, sizeUrl}, bool(https), tz, std::string{type, sizeType});
			}
		);
	}

	bool DbConnectionObservations::getAllIcaos(std::vector<std::tuple<CassUuid, std::string>>& stations)
	{
		return performSelect(_selectAllIcaos.get(),
			[&stations](const CassRow* row) {
				const CassValue* v = cass_row_get_column(row, 0);
				if (cass_value_is_null(v))
					return;
				CassUuid station;
				cass_value_get_uuid(v, &station);

				v = cass_row_get_column(row, 1);
				if (cass_value_is_null(v))
					return;
				const char *icaoStr;
				size_t icaoLength;
				cass_value_get_string(v, &icaoStr, &icaoLength);
				if (icaoLength != 0)
					stations.emplace_back(station, std::string{icaoStr, icaoLength});
			}
		);
	}

	bool DbConnectionObservations::getDeferredSynops(std::vector<std::tuple<CassUuid, std::string>>& stations)
	{
		return performSelect(_selectDeferredSynops.get(),
			[&stations](const CassRow* row) {
				const CassValue* v = cass_row_get_column(row, 0);
				if (cass_value_is_null(v))
					return;
				CassUuid station;
				cass_value_get_uuid(v, &station);

				v = cass_row_get_column(row, 1);
				if (cass_value_is_null(v))
					return;
				const char *icaoStr;
				size_t icaoLength;
				cass_value_get_string(v, &icaoStr, &icaoLength);
				stations.emplace_back(station, std::string{icaoStr, icaoLength});
			}
		);
	}

	bool DbConnectionObservations::getAllFieldClimateApiStations(std::vector<std::tuple<CassUuid, std::string, int, std::map<std::string, std::string>>>& stations)
	{
		return performSelect(_selectFieldClimateApiStations.get(),
			[&stations](const CassRow* row) {
				const CassValue* v = cass_row_get_column(row, 0);
				if (cass_value_is_null(v))
					return;
				CassUuid station;
				cass_value_get_uuid(v, &station);

				v = cass_row_get_column(row, 1);
				if (cass_value_is_null(v))
					return;
				cass_bool_t active;
				cass_value_get_bool(v, &active);

				v = cass_row_get_column(row, 2);
				if (cass_value_is_null(v))
					return;
				const char *fieldClimateId;
				size_t sizeFieldClimateId;
				cass_value_get_string(v, &fieldClimateId, &sizeFieldClimateId);

				std::map<std::string, std::string> sensors;
				const CassValue* mappingValue = cass_row_get_column(row, 3);
				if (!cass_value_is_null(mappingValue)) {
					std::unique_ptr<CassIterator, void(&)(CassIterator*)> mappingIterator{
						cass_iterator_from_map(mappingValue),
						cass_iterator_free
					};
					while (cass_iterator_next(mappingIterator.get())) {
						const char *variable;
						size_t sizeVariable;
						cass_value_get_string(cass_iterator_get_map_key(mappingIterator.get()), &variable, &sizeVariable);

						const char *sensor;
						size_t sizeSensor;
						cass_value_get_string(cass_iterator_get_map_value(mappingIterator.get()), &sensor, &sizeSensor);
						sensors.emplace(std::string{variable, sizeVariable}, std::string{sensor, sizeSensor});
					}
				}


				int tz;
				v = cass_row_get_column(row, 4);
				if (cass_value_is_null(v))
					tz = 0;
				else
					cass_value_get_int32(v, &tz);
				if (active == cass_true)
					stations.emplace_back(station, std::string{fieldClimateId, sizeFieldClimateId}, tz, sensors);
			}
		);
	}

	bool DbConnectionObservations::getAllObjeniousApiStations(std::vector<std::tuple<CassUuid, std::string, std::map<std::string, std::string>>>& stations)
	{
		return performSelect(_selectObjeniousApiStations.get(),
			[&stations](const CassRow* row) {
				const CassValue* v = cass_row_get_column(row, 0);
				if (cass_value_is_null(v))
					return;
				CassUuid station;
				cass_value_get_uuid(v, &station);

				v = cass_row_get_column(row, 1);
				if (cass_value_is_null(v))
					return;
				cass_bool_t active;
				cass_value_get_bool(v, &active);

				v = cass_row_get_column(row, 2);
				if (cass_value_is_null(v))
					return;
				const char *objeniousId;
				size_t sizeObjeniousId;
				cass_value_get_string(v, &objeniousId, &sizeObjeniousId);

				std::map<std::string, std::string> variables;
				const CassValue* mappingValue = cass_row_get_column(row, 3);
				if (!cass_value_is_null(mappingValue)) {
					std::unique_ptr<CassIterator, void(&)(CassIterator*)> mappingIterator{
						cass_iterator_from_map(mappingValue),
						cass_iterator_free
					};
					while (cass_iterator_next(mappingIterator.get())) {
						const char *variable;
						size_t sizeVariable;
						cass_value_get_string(cass_iterator_get_map_key(mappingIterator.get()), &variable, &sizeVariable);

						const char *objeniousVar;
						size_t sizeObjeniousVar;
						cass_value_get_string(cass_iterator_get_map_value(mappingIterator.get()), &objeniousVar, &sizeObjeniousVar);
						variables.emplace(std::string{variable, sizeVariable}, std::string{objeniousVar, sizeObjeniousVar});
					}
				}

				if (active == cass_true)
					stations.emplace_back(station, std::string{objeniousId, sizeObjeniousId}, variables);
			}
		);
	}

	bool DbConnectionObservations::getAllLiveobjectsStations(std::vector<std::tuple<CassUuid, std::string, std::string>>& stations)
	{
		return performSelect(_selectLiveobjectsStations.get(),
			[&stations](const CassRow* row) {
				 const CassValue* v = cass_row_get_column(row, 0);
				 if (cass_value_is_null(v))
					 return;
				 CassUuid station;
				 cass_value_get_uuid(v, &station);

				 v = cass_row_get_column(row, 1);
				 if (cass_value_is_null(v))
					 return;
				 cass_bool_t active;
				 cass_value_get_bool(v, &active);

				 v = cass_row_get_column(row, 2);
				 if (cass_value_is_null(v))
					 return;
				 const char *streamId;
				 size_t sizeStreamId;
				 cass_value_get_string(v, &streamId, &sizeStreamId);

				 v = cass_row_get_column(row, 3);
				 if (cass_value_is_null(v))
					 return;
				 const char *topicId;
				 size_t sizeTopicId;
				 cass_value_get_string(v, &topicId, &sizeTopicId);

				 if (active == cass_true)
					 stations.emplace_back(station, std::string{streamId, sizeStreamId}, std::string{topicId, sizeTopicId});
			}
		);
	}


	bool DbConnectionObservations::getAllCimelStations(std::vector<std::tuple<CassUuid, std::string, int>>& stations)
	{
		return performSelect(_selectCimelStations.get(),
				[&stations](const CassRow* row) {
					const CassValue* v = cass_row_get_column(row, 0);
					if (cass_value_is_null(v))
						return;
					CassUuid station;
					cass_value_get_uuid(v, &station);

					v = cass_row_get_column(row, 1);
					if (cass_value_is_null(v))
						return;
					cass_bool_t active;
					cass_value_get_bool(v, &active);

					v = cass_row_get_column(row, 2);
					if (cass_value_is_null(v))
						return;
					const char *cimelId;
					size_t sizeCimelId;
					cass_value_get_string(v, &cimelId, &sizeCimelId);

					int timezone;
					v = cass_row_get_column(row, 3);
					if (cass_value_is_null(v))
						timezone = 0;
					else
						cass_value_get_int32(cass_row_get_column(row,3), &timezone);

					if (active == cass_true)
						stations.emplace_back(station, std::string{cimelId, sizeCimelId}, timezone);
				}
		);
	}

	bool DbConnectionObservations::getRainfall(const CassUuid& station, time_t begin, time_t end, float& rainfall)
	{
		std::unique_ptr<CassStatement, void(&)(CassStatement*)> statement{
			cass_prepared_bind(_getRainfall.get()),
			cass_statement_free
		};
		cass_statement_set_is_idempotent(statement.get(), cass_true);

		date::sys_seconds day(date::floor<date::days>(chrono::system_clock::from_time_t(begin)));
		date::sys_seconds final{chrono::seconds(end)};
		bool ret = true;
		rainfall = 0;
		while (day < final && ret) {
			cass_statement_bind_uuid(statement.get(), 0, station);
			cass_statement_bind_uint32(statement.get(), 1, cass_date_from_epoch(chrono::system_clock::to_time_t(day)));
			cass_statement_bind_int64(statement.get(), 2, begin * 1000);
			cass_statement_bind_int64(statement.get(), 3, end * 1000);
			std::unique_ptr<CassFuture, void(&)(CassFuture*)> query{
				cass_session_execute(_session.get(), statement.get()),
				cass_future_free
			};
			std::unique_ptr<const CassResult, void(&)(const CassResult*)> result{
				cass_future_get_result(query.get()),
				cass_result_free
			};

			if (result) {
				const CassRow* row = cass_result_first_row(result.get());
				if (row) {
					const CassValue* value = cass_row_get_column(row, 0);
					if (!cass_value_is_null(value)) {
						float f;
						cass_value_get_float(value, &f);
						rainfall += f;
					}
				}
			} else {
				ret = false;
			}

			day += date::days(1);
		}

		return ret;
	}

	bool DbConnectionObservations::deleteDataPoints(const CassUuid& station, const date::sys_days& day, const date::sys_seconds& start, const date::sys_seconds& end)
	{
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
			ret = false;
		}

		return ret;
	}

	bool DbConnectionObservations::getTx(const CassUuid& station, time_t boundary, std::pair<bool, float>& value)
	{
		std::unique_ptr<CassStatement, void(&)(CassStatement*)> statement{
			cass_prepared_bind(_selectTx.get()),
			cass_statement_free
		};

		cass_statement_set_is_idempotent(statement.get(), cass_true);
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
		value.first = false;
		if (result) {
			const CassRow* row = cass_result_first_row(result.get());
			if (row) {
				ret = true;
				const CassValue* v = cass_row_get_column(row, 0);
				if (!cass_value_is_null(v)) {
					float f;
					cass_value_get_float(v, &f);
					value.first = true;
					value.second = f;
				}
			}
		}

		return ret;
	}

	bool DbConnectionObservations::getTn(const CassUuid& station, time_t boundary, std::pair<bool, float>& value)
	{
		std::unique_ptr<CassStatement, void(&)(CassStatement*)> statement{
			cass_prepared_bind(_selectTn.get()),
			cass_statement_free
		};

		cass_statement_set_is_idempotent(statement.get(), cass_true);
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
		value.first = false;
		if (result) {
			const CassRow* row = cass_result_first_row(result.get());
			if (row) {
				ret = true;
				const CassValue* v = cass_row_get_column(row, 0);
				if (!cass_value_is_null(v)) {
					float f;
					cass_value_get_float(v, &f);
					value.first = true;
					value.second = f;
				}
			}
		}

		return ret;
	}

	bool DbConnectionObservations::getCachedInt(const CassUuid& station, const std::string& key, time_t& lastUpdate, int& value)
	{
		std::unique_ptr<CassStatement, void(&)(CassStatement*)> statement{
			cass_prepared_bind(_selectCached.get()),
			cass_statement_free
		};

		cass_statement_set_is_idempotent(statement.get(), cass_true);
		cass_statement_bind_uuid(statement.get(), 0, station);
		cass_statement_bind_string_n(statement.get(), 1, key.data(), key.length());
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
				ret = true;
				const CassValue* v = cass_row_get_column(row, 0);
				if (!cass_value_is_null(v)) {
					cass_int64_t cassTimestamp;
					cass_value_get_int64(v, &cassTimestamp);
					lastUpdate = cassTimestamp / 1000;
				} else {
					ret = false;
				}

				v = cass_row_get_column(row, 1);
				if (ret && !cass_value_is_null(v)) {
					int i;
					cass_value_get_int32(v, &i);
					value = i;
				} else {
					ret = false;
				}
				// column 2 is not parsed
			}
		}

		return ret;
	}

	bool DbConnectionObservations::getCachedFloat(const CassUuid& station, const std::string& key, time_t& lastUpdate, float& value)
	{
		std::unique_ptr<CassStatement, void(&)(CassStatement*)> statement{
			cass_prepared_bind(_selectCached.get()),
			cass_statement_free
		};

		cass_statement_set_is_idempotent(statement.get(), cass_true);
		cass_statement_bind_uuid(statement.get(), 0, station);
		cass_statement_bind_string_n(statement.get(), 1, key.data(), key.length());
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
				ret = true;
				const CassValue* v = cass_row_get_column(row, 0);
				if (!cass_value_is_null(v)) {
					cass_int64_t cassTimestamp;
					cass_value_get_int64(v, &cassTimestamp);
					lastUpdate = cassTimestamp / 1000;
				} else {
					ret = false;
				}

				// column 1 is not parsed
				v = cass_row_get_column(row, 2);
				if (ret && !cass_value_is_null(v)) {
					float f;
					cass_value_get_float(v, &f);
					value = f;
				} else {
					ret = false;
				}
			}
		}

		return ret;
	}

	bool DbConnectionObservations::cacheInt(const CassUuid& station, const std::string& key, const time_t& update, int value)
	{
		std::unique_ptr<CassStatement, void(&)(CassStatement*)> statement{
			cass_prepared_bind(_insertIntoCache.get()),
			cass_statement_free
		};


		time_t previousTime;
		int previousValue;
		bool alreadyPresent = getCachedInt(station, key, previousTime, previousValue);
		if (alreadyPresent && previousTime > update) {
			// there's already a value and it's more recent, don't
			// insert the new one
			return false;
		}


		cass_statement_bind_uuid(statement.get(), 0, station);
		cass_statement_bind_string_n(statement.get(), 1, key.data(), key.length());
		cass_statement_bind_int64(statement.get(), 2, update * 1000);
		cass_statement_bind_int32(statement.get(), 3, value);
		// field 4 intentionally not filled in
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
			ret = false;
		}

		return ret;
	}

	bool DbConnectionObservations::cacheFloat(const CassUuid& station, const std::string& key, const time_t& update, float value)
	{
		std::unique_ptr<CassStatement, void(&)(CassStatement*)> statement{
			cass_prepared_bind(_insertIntoCache.get()),
			cass_statement_free
		};


		time_t previousTime;
		float previousValue;
		bool alreadyPresent = getCachedFloat(station, key, previousTime, previousValue);
		if (alreadyPresent && previousTime > update) {
			// there's already a value and it's more recent, don't
			// insert the new one
			return false;
		}


		cass_statement_bind_uuid(statement.get(), 0, station);
		cass_statement_bind_string_n(statement.get(), 1, key.data(), key.length());
		cass_statement_bind_int64(statement.get(), 2, update * 1000);
		// field 3 intentionally not filled in
		cass_statement_bind_float(statement.get(), 4, value);
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
			ret = false;
		}

		return ret;
	}
}
