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

#include <chrono>
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
#include "map_observation.h"
#include "cassandra_stmt_ptr.h"
#include "virtual_station.h"

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
			"SELECT id,icao,active FROM meteodata.stationsfr"
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
			"winddir, windgust, min_windspeed, windspeed,"
			"insolation_time, "
			"soilmoistures10cm, soilmoistures20cm, "
			"soilmoistures30cm, soilmoistures40cm, "
			"soilmoistures50cm, soilmoistures60cm, "
			"soiltemp10cm, soiltemp20cm, "
			"soiltemp30cm, soiltemp40cm, "
			"soiltemp50cm, soiltemp60cm, "
			"leaf_wetness_percent1, "
			"voltage_battery, voltage_solar_panel, voltage_backup "
			" FROM meteodata_v2.meteo WHERE station = ? "
			" AND day = ? AND time <= ? ORDER BY time DESC LIMIT 1"
		);

		prepareOneStatement(_selectMapValues,
			"SELECT "
			"time,"
			"outsidetemp, max_outside_temperature, min_outside_temperature, "
			"rainfall,"
			"et,"
			"windgust,"
			"insolation_time "
			" FROM meteodata_v2.meteo WHERE station = ? "
			" AND day = ? ORDER BY time DESC"
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
			"winddir, windgust, min_windspeed, windspeed,"
			"insolation_time,"
			"min_outside_temperature, max_outside_temperature,"
			"leafwetnesses_timeratio1, "
			"soilmoistures10cm, soilmoistures20cm, "
			"soilmoistures30cm, soilmoistures40cm, "
			"soilmoistures50cm, soilmoistures60cm, "
			"soiltemp10cm, soiltemp20cm, "
			"soiltemp30cm, soiltemp40cm, "
			"soiltemp50cm, soiltemp60cm, "
			"leaf_wetness_percent1, "
			"voltage_battery, voltage_solar_panel, voltage_backup "
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
			"?, ?, ?, ?,"	// "winddir, windgust, min_windspeed, windspeed,"
			"?,"		// "insolation_time"
			"?,?,"		// "min_outside_temperature, max_outside_temperature"
			"?,"		// "leafwetnesses_timeratio1"
			"?,?,?,"	// "soilmoistures10cm, soilmoistures20cm, soilmoistures30cm"
			"?,?,?,"	// "soilmoistures40cm, soilmoistures50cm, soilmoistures60cm"
			"?,?,?,"	// "soiltemp10cm, soiltemp20cm, soiltemp30cm"
			"?,?,?,"	// "soiltemp40cm, soiltemp50cm, soiltemp60cm"
			"?, "		// "leaf_wetness_percent1"
			"?,?,? "	// "voltage_battery, voltage_solar_panel, voltage_backup"
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
			"winddir, windgust, min_windspeed, windspeed,"
			"insolation_time,"
			"min_outside_temperature, max_outside_temperature,"
			"leafwetnesses_timeratio1, "
			"soilmoistures10cm, soilmoistures20cm, "
			"soilmoistures30cm, soilmoistures40cm, "
			"soilmoistures50cm, soilmoistures60cm, "
			"soiltemp10cm, soiltemp20cm, "
			"soiltemp30cm, soiltemp40cm, "
			"soiltemp50cm, soiltemp60cm,"
			"leaf_wetness_percent1, "
			"voltage_battery, voltage_solar_panel, voltage_backup "
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
			"?, ?, ?, ?,"	// "winddir, windgust, min_windspeed, windspeed,"
			"?,"		// "insolation_time"
			"?,?,"		// "min_outside_temperature, max_outside_temperature"
			"?,"		// "leafwetnesses_timeratio1"
			"?,?,?,"	// "soilmoistures10cm, soilmoistures20cm, soilmoistures30cm"
			"?,?,?,"	// "soilmoistures40cm, soilmoistures50cm, soilmoistures60cm"
			"?,?,?,"	// "soiltemp10cm, soiltemp20cm, soiltemp30cm"
			"?,?,?,"	// "soiltemp40cm, soiltemp50cm, soiltemp60cm"
			"?,"		// "leaf_wetness_percent1"
			"?,?,? "	// "voltage_battery, voltage_solar_panel, voltage_backup"
			")"
		);

		prepareOneStatement(_insertV2MapDataPoint,
			"INSERT INTO meteodata_v2.observations_map ("
			"time,"
			"station,"
			"actual_time,"
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
			"winddir, windgust, min_windspeed, windspeed,"
			"insolation_time,"
			"min_outside_temperature, max_outside_temperature,"
			"leafwetnesses_timeratio1, "
			"soilmoistures10cm, soilmoistures20cm, "
			"soilmoistures30cm, soilmoistures40cm, "
			"soilmoistures50cm, soilmoistures60cm, "
			"soiltemp10cm, soiltemp20cm, "
			"soiltemp30cm, soiltemp40cm, "
			"soiltemp50cm, soiltemp60cm,"
			"leaf_wetness_percent1, "
			"voltage_battery, voltage_solar_panel, voltage_backup, "
			"rainfall1h, rainfall3h, rainfall6h, "
			"rainfall12h, rainfall24h, rainfall48h, "
			"max_outside_temperature1h, max_outside_temperature6h, "
			"max_outside_temperature12h, max_outside_temperature24h, "
			"min_outside_temperature1h, min_outside_temperature6h, "
			"min_outside_temperature12h, min_outside_temperature24h, "
			"et1h, et12h, et24h, et48h, "
			"windgust1h, windgust12h, windgust24h "
			") "
			" VALUES ("
			"?, ?, ?,"	// "time, station, actual_time,"
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
			"?, ?, ?, ?,"	// "winddir, windgust, min_windspeed, windspeed,"
			"?,"		// "insolation_time"
			"?,?,"		// "min_outside_temperature, max_outside_temperature"
			"?,"		// "leafwetnesses_timeratio1"
			"?,?,?,"	// "soilmoistures10cm, soilmoistures20cm, soilmoistures30cm"
			"?,?,?,"	// "soilmoistures40cm, soilmoistures50cm, soilmoistures60cm"
			"?,?,?,"	// "soiltemp10cm, soiltemp20cm, soiltemp30cm"
			"?,?,?,"	// "soiltemp40cm, soiltemp50cm, soiltemp60cm"
			"?,"		// "leaf_wetness_percent1, "
			"?,?,?, "	// "voltage_battery, voltage_solar_panel, voltage_backup, "
			"?,?,?, "	// "rainfall1h, rainfall3h, rainfall6h, "
			"?,?,?, "	// "rainfall12h, rainfall24h, rainfall48h, "
			"?,?,"		// "max_outside_temperature1h, max_outside_temperature6h, "
			"?,?,"		// "max_outside_temperature12h, max_outside_temperature24h, "
			"?,?,"		// "min_outside_temperature1h, min_outside_temperature6h, "
			"?,?,"		// "min_outside_temperature12h, min_outside_temperature24h, "
			"?,?,?,?, "	// "et1h, et12h, et24h, et48h, "
			"?,?,? "	// "windgust1h, windgust12h, windgust24h "
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
			"winddir, windgust, min_windspeed, windspeed,"
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
			"?, ?, ?, ?,"
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

		prepareOneStatement(_selectMeteoFranceStations,
			"SELECT id, active, icao, idstation, date_creation, latitude, longitude, elevation, type FROM meteodata.stationsfr"
		);

		prepareOneStatement(_selectVirtualStations,
			"SELECT station, active, period, sources FROM meteodata.virtual_stations"
		);

		prepareOneStatement(_selectNbiotStations,
			"SELECT station, active, imei, imsi, hmac_key, sensor_type FROM meteodata.nbiot"
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

		prepareOneStatement(_selectLastSchedulerDownloadTime,
			"SELECT last_download FROM meteodata.scheduling_status WHERE scheduler=?"
		);

		prepareOneStatement(_insertLastSchedulerDownloadTime,
			"INSERT INTO meteodata.scheduling_status (scheduler,last_download) VALUES (?,?)"
		);

		prepareOneStatement(_selectLastConfiguration,
			"SELECT station, active, id, config, added_on FROM meteodata.pending_configurations WHERE station=? ORDER BY id DESC LIMIT 1"
		);

		prepareOneStatement(_selectOneConfiguration,
			"SELECT station, active, id, config, added_on FROM meteodata.pending_configurations WHERE station=? AND id=?"
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
						"thswindex", "windchill", "windgust", "min_windspeed", "windspeed",
						"soilmoistures10cm", "soilmoistures20cm", "soilmoistures30cm",
						"soilmoistures40cm", "soilmoistures50cm", "soilmoistures60cm",
						"soiltemp10cm", "soiltemp20cm", "soiltemp30cm",
						"soiltemp40cm", "soiltemp50cm", "soiltemp60cm",
						"leaf_wetness_percent1"
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

				ret = true;
			}
		}

		return ret;
	}

	bool DbConnectionObservations::getStationByCoords(int elevation, int latitude, int longitude, CassUuid& station,
		std::string& name, int& pollPeriod, time_t& lastArchiveDownloadTime, bool* storeInsideMeasurements)
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
				ret = getStationDetails(station, name, pollPeriod, lastArchiveDownloadTime, storeInsideMeasurements);
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

	void DbConnectionObservations::populateV2CommonInsertionQuery(CassStatement* statement, const Observation& obs, int& c)
	{
		/*************************************************************/
		if (obs.barometer.first)
			cass_statement_bind_float(statement, c, obs.barometer.second);
		c++;
		/*************************************************************/
		if (obs.dewpoint.first)
			cass_statement_bind_float(statement, c, obs.dewpoint.second);
		c++;
		/*************************************************************/
		for (int i=0 ; i<2 ; i++) {
			if (obs.extrahum[i].first)
				cass_statement_bind_int32(statement, c, obs.extrahum[i].second);
			c++;
		}
		/*************************************************************/
		for (int i=0 ; i<3 ; i++) {
			if (obs.extratemp[i].first)
				cass_statement_bind_float(statement, c, obs.extratemp[i].second);
			c++;
		}
		/*************************************************************/
		if (obs.heatindex.first)
			cass_statement_bind_float(statement, c, obs.heatindex.second);
		c++;
		/*************************************************************/
		if (obs.insidehum.first)
			cass_statement_bind_int32(statement, c, obs.insidehum.second);
		c++;
		/*************************************************************/
		if (obs.insidetemp.first)
			cass_statement_bind_float(statement, c, obs.insidetemp.second);
		c++;
		/*************************************************************/
		for (int i=0 ; i<2 ; i++) {
			if (obs.leaftemp[i].first)
				cass_statement_bind_float(statement, c, obs.leaftemp[i].second);
			c++;
		}
		/*************************************************************/
		for (int i=0 ; i<2 ; i++) {
			if (obs.leafwetnesses[i].first)
				cass_statement_bind_int32(statement, c, obs.leafwetnesses[i].second);
			c++;
		}
		/*************************************************************/
		if (obs.outsidehum.first)
			cass_statement_bind_int32(statement, c, obs.outsidehum.second);
		c++;
		/*************************************************************/
		if (obs.outsidetemp.first)
			cass_statement_bind_float(statement, c, obs.outsidetemp.second);
		c++;
		/*************************************************************/
		if (obs.rainrate.first)
			cass_statement_bind_float(statement, c, obs.rainrate.second);
		c++;
		/*************************************************************/
		if (obs.rainfall.first)
			cass_statement_bind_float(statement, c, obs.rainfall.second);
		c++;
		/*************************************************************/
		if (obs.et.first)
			cass_statement_bind_float(statement, c, obs.et.second);
		c++;
		/*************************************************************/
		for (int i=0 ; i<4 ; i++) {
			if (obs.soilmoistures[i].first)
				cass_statement_bind_int32(statement, c, obs.soilmoistures[i].second);
			c++;
		}
		/*************************************************************/
		for (int i=0 ; i<4 ; i++) {
			if (obs.soiltemp[i].first)
				cass_statement_bind_float(statement, c, obs.soiltemp[i].second);
			c++;
		}
		/*************************************************************/
		if (obs.solarrad.first)
			cass_statement_bind_int32(statement, c, obs.solarrad.second);
		c++;
		/*************************************************************/
		if (obs.thswindex.first)
			cass_statement_bind_float(statement, c, obs.thswindex.second);
		c++;
		/*************************************************************/
		if (obs.uv.first)
			cass_statement_bind_int32(statement, c, obs.uv.second);
		c++;
		/*************************************************************/
		if (obs.windchill.first)
			cass_statement_bind_float(statement, c, obs.windchill.second);
		c++;
		/*************************************************************/
		if (obs.winddir.first)
			cass_statement_bind_int32(statement, c, obs.winddir.second);
		c++;
		/*************************************************************/
		if (obs.windgust.first)
			cass_statement_bind_float(statement, c, obs.windgust.second);
		c++;
		/*************************************************************/
		if (obs.min_windspeed.first)
			cass_statement_bind_float(statement, c, obs.min_windspeed.second);
		c++;
		/*************************************************************/
		if (obs.windspeed.first)
			cass_statement_bind_float(statement, c, obs.windspeed.second);
		c++;
		/*************************************************************/
		if (obs.insolation_time.first)
			cass_statement_bind_int32(statement, c, obs.insolation_time.second);
		c++;
		/*************************************************************/
		if (obs.min_outside_temperature.first)
			cass_statement_bind_float(statement, c, obs.min_outside_temperature.second);
		c++;
		/*************************************************************/
		if (obs.max_outside_temperature.first)
			cass_statement_bind_float(statement, c, obs.max_outside_temperature.second);
		c++;
		/*************************************************************/
		if (obs.leafwetness_timeratio1.first)
			cass_statement_bind_int32(statement, c, obs.leafwetness_timeratio1.second);
		c++;
		/*************************************************************/
		if (obs.soilmoistures10cm.first)
			cass_statement_bind_float(statement, c, obs.soilmoistures10cm.second);
		c++;
		if (obs.soilmoistures20cm.first)
			cass_statement_bind_float(statement, c, obs.soilmoistures20cm.second);
		c++;
		if (obs.soilmoistures30cm.first)
			cass_statement_bind_float(statement, c, obs.soilmoistures30cm.second);
		c++;
		if (obs.soilmoistures40cm.first)
			cass_statement_bind_float(statement, c, obs.soilmoistures40cm.second);
		c++;
		if (obs.soilmoistures50cm.first)
			cass_statement_bind_float(statement, c, obs.soilmoistures50cm.second);
		c++;
		if (obs.soilmoistures60cm.first)
			cass_statement_bind_float(statement, c, obs.soilmoistures60cm.second);
		c++;
		/*************************************************************/
		if (obs.soiltemp10cm.first)
			cass_statement_bind_float(statement, c, obs.soiltemp10cm.second);
		c++;
		if (obs.soiltemp20cm.first)
			cass_statement_bind_float(statement, c, obs.soiltemp20cm.second);
		c++;
		if (obs.soiltemp30cm.first)
			cass_statement_bind_float(statement, c, obs.soiltemp30cm.second);
		c++;
		if (obs.soiltemp40cm.first)
			cass_statement_bind_float(statement, c, obs.soiltemp40cm.second);
		c++;
		if (obs.soiltemp50cm.first)
			cass_statement_bind_float(statement, c, obs.soiltemp50cm.second);
		c++;
		if (obs.soiltemp60cm.first)
			cass_statement_bind_float(statement, c, obs.soiltemp60cm.second);
		c++;
		/*************************************************************/
		if (obs.leafwetness_percent1.first)
			cass_statement_bind_float(statement, c, obs.leafwetness_percent1.second);
		c++;
		/*************************************************************/
	}

	void DbConnectionObservations::populateV2InsertionQuery(CassStatement* statement, const Observation& obs)
	{
		int c = 0;

		/*************************************************************/
		cass_statement_bind_uuid(statement, c++, obs.station);
		/*************************************************************/
		cass_statement_bind_uint32(statement, c++, cass_date_from_epoch(obs.time.time_since_epoch().count()));
		cass_statement_bind_int64(statement, c++, 1000*obs.time.time_since_epoch().count()); // in ms
		/*************************************************************/

		populateV2CommonInsertionQuery(statement, obs, c);
	}

	void DbConnectionObservations::populateV2MapInsertionQuery(CassStatement* statement, const Observation& obs, const MapObservation& map, const std::chrono::seconds& insertionTime)
	{
		int c = 0;

		chrono::seconds actualTime = obs.time.time_since_epoch();

		/*************************************************************/
		cass_statement_bind_int64(statement, c++, 1000*insertionTime.count()); // in ms
		cass_statement_bind_uuid(statement, c++, obs.station);
		cass_statement_bind_int64(statement, c++, 1000*actualTime.count()); // in ms
		/*************************************************************/

		populateV2CommonInsertionQuery(statement, obs, c);

		/*************************************************************/
		if (map.rainfall1h.first)
			cass_statement_bind_float(statement, c, map.rainfall1h.second);
		c++;
		if (map.rainfall3h.first)
			cass_statement_bind_float(statement, c, map.rainfall3h.second);
		c++;
		if (map.rainfall6h.first)
			cass_statement_bind_float(statement, c, map.rainfall6h.second);
		c++;
		if (map.rainfall12h.first)
			cass_statement_bind_float(statement, c, map.rainfall12h.second);
		c++;
		if (map.rainfall24h.first)
			cass_statement_bind_float(statement, c, map.rainfall24h.second);
		c++;
		if (map.rainfall48h.first)
			cass_statement_bind_float(statement, c, map.rainfall48h.second);
		c++;
		/*************************************************************/
		if (map.max_outside_temperature1h.first)
			cass_statement_bind_float(statement, c, map.max_outside_temperature1h.second);
		c++;
		if (map.max_outside_temperature6h.first)
			cass_statement_bind_float(statement, c, map.max_outside_temperature6h.second);
		c++;
		if (map.max_outside_temperature12h.first)
			cass_statement_bind_float(statement, c, map.max_outside_temperature12h.second);
		c++;
		if (map.max_outside_temperature24h.first)
			cass_statement_bind_float(statement, c, map.max_outside_temperature24h.second);
		c++;
		if (map.min_outside_temperature1h.first)
			cass_statement_bind_float(statement, c, map.min_outside_temperature1h.second);
		c++;
		if (map.min_outside_temperature6h.first)
			cass_statement_bind_float(statement, c, map.min_outside_temperature6h.second);
		c++;
		if (map.min_outside_temperature12h.first)
			cass_statement_bind_float(statement, c, map.min_outside_temperature12h.second);
		c++;
		if (map.min_outside_temperature24h.first)
			cass_statement_bind_float(statement, c, map.min_outside_temperature24h.second);
		c++;
		/*************************************************************/
		if (map.et1h.first)
			cass_statement_bind_float(statement, c, map.et1h.second);
		c++;
		if (map.et12h.first)
			cass_statement_bind_float(statement, c, map.et12h.second);
		c++;
		if (map.et24h.first)
			cass_statement_bind_float(statement, c, map.et24h.second);
		c++;
		if (map.et48h.first)
			cass_statement_bind_float(statement, c, map.et48h.second);
		c++;
		/*************************************************************/
		if (map.windgust1h.first)
			cass_statement_bind_float(statement, c, map.windgust1h.second);
		c++;
		if (map.windgust12h.first)
			cass_statement_bind_float(statement, c, map.windgust12h.second);
		c++;
		if (map.windgust24h.first)
			cass_statement_bind_float(statement, c, map.windgust24h.second);
		c++;
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

		std::unique_ptr<CassStatement, void(&)(CassStatement*)> statement3{
			cass_prepared_bind(_insertV2MapDataPoint.get()),
			cass_statement_free
		};
		MapObservation map;
		getMapValues(obs.station, chrono::system_clock::to_time_t(obs.time), map);
		chrono::seconds truncatedTime = obs.time.time_since_epoch() - obs.time.time_since_epoch() % OBSERVATIONS_MAP_TIME_RESOLUTION;
		populateV2MapInsertionQuery(statement3.get(), copy, map, truncatedTime);
		query.reset(cass_session_execute(_session.get(), statement3.get()));
		result.reset(cass_future_get_result(query.get()));

		if (!result) {
			const char* error_message;
			size_t error_message_length;
			cass_future_error_message(query.get(), &error_message, &error_message_length);
			return false;
		}

		// Insert the same observation at the following increment, as a
		// temporary measurement
		statement3.reset(cass_prepared_bind(_insertV2MapDataPoint.get()));
		truncatedTime += OBSERVATIONS_MAP_TIME_RESOLUTION;
		populateV2MapInsertionQuery(statement3.get(), copy, map, truncatedTime);
		query.reset(cass_session_execute(_session.get(), statement3.get()));
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

				v = cass_row_get_column(row, 2);
				if (cass_value_is_null(v))
					return;
				cass_bool_t active;
				cass_value_get_bool(v, &active);

				if (icaoLength != 0 && active == cass_true)
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

	bool DbConnectionObservations::getMeteoFranceStations(std::vector<std::tuple<CassUuid, std::string, std::string, int, float, float, int, int>>& stations)
	{
		return performSelect(_selectMeteoFranceStations.get(),
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
					std::string icao;
					if (!cass_value_is_null(v)) {
						const char *icaoC;
						size_t sizeIcao;
						cass_value_get_string(v, &icaoC, &sizeIcao);
						icao = std::string{icaoC, sizeIcao};
					}

					v = cass_row_get_column(row, 3);
					if (cass_value_is_null(v))
						return;
					const char *mfId;
					size_t sizeMfId;
					cass_value_get_string(v, &mfId, &sizeMfId);

					int date;
					v = cass_row_get_column(row, 4);
					if (cass_value_is_null(v))
						date = 0;
					else
						cass_value_get_int32(cass_row_get_column(row,4), &date);

					float latitude;
					v = cass_row_get_column(row, 5);
					if (cass_value_is_null(v))
						latitude = 0;
					else
						cass_value_get_float(cass_row_get_column(row,5), &latitude);

					float longitude;
					v = cass_row_get_column(row, 6);
					if (cass_value_is_null(v))
						longitude = 0;
					else
						cass_value_get_float(cass_row_get_column(row,6), &longitude);

					int elevation;
					v = cass_row_get_column(row, 7);
					if (cass_value_is_null(v))
						elevation = 0;
					else
						cass_value_get_int32(cass_row_get_column(row,7), &elevation);

					int type;
					v = cass_row_get_column(row, 8);
					if (cass_value_is_null(v))
						type = 0;
					else
						cass_value_get_int32(cass_row_get_column(row,8), &type);

					if (active == cass_true)
						stations.emplace_back(station, icao, std::string{mfId, sizeMfId}, date, latitude, longitude, elevation, type);
				}
		);
	}

	bool DbConnectionObservations::getAllNbiotStations(std::vector<NbiotStation>& stations)
	{
		return performSelect(_selectNbiotStations.get(),
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

				if (active != cass_true)
					return;

				v = cass_row_get_column(row, 2);
				const char *imei;
				size_t sizeImei;
				cass_value_get_string(v, &imei, &sizeImei);

				v = cass_row_get_column(row, 3);
				const char *imsi;
				size_t sizeImsi;
				cass_value_get_string(v, &imsi, &sizeImsi);

				v = cass_row_get_column(row, 4);
				const char *hmacKey;
				size_t sizeHmacKey;
				cass_value_get_string(v, &hmacKey, &sizeHmacKey);

				v = cass_row_get_column(row, 5);
				const char *sensorType;
				size_t sizeSensorType;
				cass_value_get_string(v, &sensorType, &sizeSensorType);

				stations.push_back(
					NbiotStation{
						station,
						std::string{imei, sizeImei},
						std::string{imsi, sizeImsi},
						std::string{hmacKey, sizeHmacKey},
						std::string{sensorType, sizeSensorType}
					}
				);
			}
		);
	}

	bool DbConnectionObservations::getAllVirtualStations(std::vector<VirtualStation>& stations)
	{
		return performSelect(_selectVirtualStations.get(),
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

				if (active != cass_true)
					return;

				v = cass_row_get_column(row, 2);
				if (cass_value_is_null(v))
					return;
				int period;
				cass_value_get_int32(v, &period);

				std::vector<std::pair<CassUuid, std::vector<std::string>>> sources;
				const CassValue* mappingValue = cass_row_get_column(row, 3);
				if (!cass_value_is_null(mappingValue)) {
					std::unique_ptr<CassIterator, void(&)(CassIterator*)> mappingIterator{
						cass_iterator_from_map(mappingValue),
						cass_iterator_free
					};
					while (cass_iterator_next(mappingIterator.get())) {
						CassUuid st;
						cass_value_get_uuid(cass_iterator_get_map_key(mappingIterator.get()), &st);

						auto sourceValue = cass_iterator_get_map_value(mappingIterator.get());
						if (!cass_value_is_null(sourceValue)) {
							std::vector<std::string> variables;
							std::unique_ptr<CassIterator, void(&)(CassIterator*)> variableIterator{
								cass_iterator_from_collection(sourceValue),
								cass_iterator_free
							};
							while (cass_iterator_next(variableIterator.get())) {
								const char *var;
								size_t sizeVar;
								cass_value_get_string(cass_iterator_get_value(variableIterator.get()), &var, &sizeVar);
								variables.emplace_back(var, sizeVar);
							}
							sources.emplace_back(st, std::move(variables));
						}
					}
				}

				stations.push_back(VirtualStation{station, period, std::move(sources)});
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

	bool DbConnectionObservations::getMapValues(const CassUuid& uuid, time_t time, MapObservation& obs)
	{
		// Truncate all times to seconds, it's enough resolution
		auto ref = chrono::system_clock::from_time_t(time);
		auto t1h = ref - chrono::hours{1};
		auto t3h = ref - chrono::hours{3};
		auto t6h = ref - chrono::hours{6};
		auto t12h = ref - chrono::hours{12};
		auto t24h = ref - chrono::hours{24};
		auto t48h = ref - chrono::hours{48};

		auto handleResponse = [&](const CassRow* row) {
			const CassValue* v = cass_row_get_column(row, 0);
			if (cass_value_is_null(v))
				return;
			cass_int64_t timeMillisec;
			cass_value_get_int64(v, &timeMillisec);
			auto t = chrono::system_clock::from_time_t(timeMillisec / 1000);

			if (t < t48h)
				return;

			v = cass_row_get_column(row, 1);
			std::pair<bool, float> temp = { false, 0.f };
			if (!cass_value_is_null(v)) {
				cass_value_get_float(v, &temp.second);
				temp.first = true;
			}

			v = cass_row_get_column(row, 2);
			std::pair<bool, float> maxtemp = { false, 0.f };
			if (!cass_value_is_null(v)) {
				cass_value_get_float(v, &maxtemp.second);
				maxtemp.first = true;
			}

			v = cass_row_get_column(row, 3);
			std::pair<bool, float> mintemp = { false, 0.f };
			if (!cass_value_is_null(v)) {
				cass_value_get_float(v, &mintemp.second);
				mintemp.first = true;
			}

			if (!maxtemp.first && temp.first)
				maxtemp = std::move(temp);
			if (maxtemp.first) {
				if (t > t1h) {
					if (!obs.max_outside_temperature1h.first || maxtemp.second > obs.max_outside_temperature1h.second) {
						obs.max_outside_temperature1h = { true, maxtemp.second };
					}
				}

				if (t > t6h) {
					if (!obs.max_outside_temperature6h.first || maxtemp.second > obs.max_outside_temperature6h.second) {
						obs.max_outside_temperature6h = { true, maxtemp.second };
					}
				}

				if (t > t12h) {
					if (!obs.max_outside_temperature12h.first || maxtemp.second > obs.max_outside_temperature12h.second) {
						obs.max_outside_temperature12h = { true, maxtemp.second };
					}
				}

				if (t > t24h) {
					if (!obs.max_outside_temperature24h.first || maxtemp.second > obs.max_outside_temperature24h.second) {
						obs.max_outside_temperature24h = { true, maxtemp.second };
					}
				}
			}

			if (!mintemp.first && temp.first)
				mintemp = std::move(temp);
			if (mintemp.first) {
				if (t > t1h) {
					if (!obs.min_outside_temperature1h.first || mintemp.second < obs.min_outside_temperature1h.second) {
						obs.min_outside_temperature1h = { true, mintemp.second };
					}
				}

				if (t > t6h) {
					if (!obs.min_outside_temperature6h.first || mintemp.second < obs.min_outside_temperature6h.second) {
						obs.min_outside_temperature6h = { true, mintemp.second };
					}
				}

				if (t > t12h) {
					if (!obs.min_outside_temperature12h.first || mintemp.second < obs.min_outside_temperature12h.second) {
						obs.min_outside_temperature12h = { true, mintemp.second };
					}
				}

				if (t > t24h) {
					if (!obs.min_outside_temperature24h.first || mintemp.second < obs.min_outside_temperature24h.second) {
						obs.min_outside_temperature24h = { true, mintemp.second };
					}
				}
			}

			v = cass_row_get_column(row, 4);
			if (!cass_value_is_null(v)) {
				float rainfall = 0.f;
				cass_value_get_float(v, &rainfall);

				obs.rainfall1h.first = true;
				obs.rainfall3h.first = true;
				obs.rainfall6h.first = true;
				obs.rainfall12h.first = true;
				obs.rainfall24h.first = true;
				obs.rainfall48h.first = true;

				if (t > t1h) {
					obs.rainfall1h.second += rainfall;
				}
				if (t > t3h) {
					obs.rainfall3h.second += rainfall;
				}
				if (t > t6h) {
					obs.rainfall6h.second += rainfall;
				}
				if (t > t12h) {
					obs.rainfall12h.second += rainfall;
				}
				if (t > t24h) {
					obs.rainfall24h.second += rainfall;
				}
				if (t > t48h) {
					obs.rainfall48h.second += rainfall;
				}
			}

			v = cass_row_get_column(row, 5);
			if (!cass_value_is_null(v)) {
				float et = 0.f;
				cass_value_get_float(v, &et);

				obs.et1h.first = true;
				obs.et12h.first = true;
				obs.et24h.first = true;
				obs.et48h.first = true;

				if (t > t1h) {
					obs.et1h.second += et;
				}
				if (t > t12h) {
					obs.et12h.second += et;
				}
				if (t > t24h) {
					obs.et24h.second += et;
				}
				if (t > t48h) {
					obs.et48h.second += et;
				}
			}

			v = cass_row_get_column(row, 6);
			if (!cass_value_is_null(v)) {
				float windgust = 0.f;
				cass_value_get_float(v, &windgust);

				if (t > t1h) {
					if (!obs.windgust1h.first || obs.windgust1h.second < windgust)
						obs.windgust1h = { true, windgust };
				}
				if (t > t12h) {
					if (!obs.windgust12h.first || obs.windgust12h.second < windgust)
						obs.windgust12h = { true, windgust };
				}
				if (t > t24h) {
					if (!obs.windgust24h.first || obs.windgust24h.second < windgust)
						obs.windgust24h = { true, windgust };
				}
			}
		};

		bool r = performSelect(_selectMapValues.get(),
			handleResponse,
			[&](CassStatement* stmt) {
				cass_statement_bind_uuid(stmt, 0, uuid);
				cass_statement_bind_uint32(stmt, 1, cass_date_from_epoch(time));
			}
		);

		if (r) {
			r = performSelect(_selectMapValues.get(),
				handleResponse,
				[&](CassStatement* stmt) {
					cass_statement_bind_uuid(stmt, 0, uuid);
					cass_statement_bind_uint32(stmt, 1, cass_date_from_epoch(time - 24 * 3600));
				}
			);
		}

		if (r) {
			r = performSelect(_selectMapValues.get(),
				handleResponse,
				[&](CassStatement* stmt) {
					cass_statement_bind_uuid(stmt, 0, uuid);
					cass_statement_bind_uint32(stmt, 1, cass_date_from_epoch(time - 48 * 3600));
				}
			);
		}

		return r;
	}

	bool DbConnectionObservations::getLastSchedulerDownloadTime(const std::string& station, time_t& lastArchiveDownloadTime)
	{
		return performSelect(_selectLastSchedulerDownloadTime.get(),
			[&](const CassRow* row) {
				const CassValue* v = cass_row_get_column(row, 0);
				if (cass_value_is_null(v))
					return;
				cass_int64_t timeMillisec;
				cass_value_get_int64(v, &timeMillisec);
				lastArchiveDownloadTime = timeMillisec / 1000;
			},
			[&](CassStatement* stmt) {
				cass_statement_bind_string_n(stmt, 0, station.c_str(), station.length());
			}
		);
	}

	bool DbConnectionObservations::insertLastSchedulerDownloadTime(const std::string& scheduler, const time_t& time)
	{
		std::unique_ptr<CassStatement, void(&)(CassStatement*)> statement{
			cass_prepared_bind(_insertLastSchedulerDownloadTime.get()),
			cass_statement_free
		};
		cass_statement_bind_string_n(statement.get(), 0, scheduler.c_str(), scheduler.length());
		cass_statement_bind_int64(statement.get(), 1, time * 1000);

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

	bool DbConnectionObservations::getLastConfiguration(const CassUuid& station, ModemStationConfiguration& config)
	{
		return performSelect(_selectLastConfiguration.get(),
			[&config](const CassRow* row) {
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

				if (active != cass_true)
					return;

				v = cass_row_get_column(row, 2);
				int id;
				cass_value_get_int32(v, &id);

				v = cass_row_get_column(row, 3);
				const char *conf;
				size_t sizeConf;
				cass_value_get_string(v, &conf, &sizeConf);

				v = cass_row_get_column(row, 4);
				int64_t timeMillisec;
				cass_value_get_int64(v, &timeMillisec);

				config.station = station;
				config.id = id;
				config.config = std::string{conf, sizeConf};
				config.addedOn = timeMillisec / 1000;
			},
			[&](CassStatement* stmt) {
				cass_statement_bind_uuid(stmt, 0, station);
			}
		);
	}

	bool DbConnectionObservations::getOneConfiguration(const CassUuid& station, int id, ModemStationConfiguration& config)
	{
		return performSelect(_selectLastConfiguration.get(),
			[&config](const CassRow* row) {
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

				if (active != cass_true)
					return;

				v = cass_row_get_column(row, 2);
				int id;
				cass_value_get_int32(v, &id);

				v = cass_row_get_column(row, 3);
				const char *conf;
				size_t sizeConf;
				cass_value_get_string(v, &conf, &sizeConf);

				v = cass_row_get_column(row, 4);
				int64_t timeMillisec;
				cass_value_get_int64(v, &timeMillisec);

				config.station = station;
				config.id = id;
				config.config = std::string{conf, sizeConf};
				config.addedOn = timeMillisec / 1000;
			},
			[&](CassStatement* stmt) {
				cass_statement_bind_uuid(stmt, 0, station);
				cass_statement_bind_int32(stmt, 1, id);
			}
		);
	}
}
