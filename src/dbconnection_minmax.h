/**
 * @file dbconnection_minmax.h
 * @brief Definition of the DbConnectionMinmax class
 * @author Laurent Georget
 * @date 2017-10-30
 */
/*
 * Copyright (C) 2017  SAS Météo Concept <contact@meteo-concept.fr>
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

#ifndef DBCONNECTION_MINMAX_H
#define DBCONNECTION_MINMAX_H

#include <ctime>
#include <functional>
#include <tuple>
#include <memory>
#include <mutex>
#include <vector>
#include <unordered_map>
#include <utility>
#include <mutex>
#include <sstream>

#include <cassandra.h>
#include <date/date.h>
#include <pqxx/pqxx>

#include "dbconnection_common.h"
#include "cassandra_stmt_ptr.h"

namespace meteodata {

template<typename T, typename Op>
inline void compute(std::pair<bool, T>& result, const std::pair<bool, T>& op1, const std::pair<bool, T>& op2, Op op)
{
	if (op1.first && op2.first) {
		result.second = op(op1.second, op2.second);
		result.first = true;
	} else if (op1.first) {
		result.second = op1.second;
		result.first = true;
	} else if (op2.first) {
		result.second = op2.second;
		result.first = true;
	} else {
		result.first = false;
	}
}

template<typename T>
inline void computeMin(std::pair<bool, T>& result, const std::pair<bool, T>& op1, const std::pair<bool, T>& op2)
{
	compute(result, op1, op2, [](const T& t1, const T& t2){ return t1 < t2 ? t1 : t2; });
}

template<typename T>
inline void computeMax(std::pair<bool, T>& result, const std::pair<bool, T>& op1, const std::pair<bool, T>& op2)
{
	compute(result, op1, op2, [](const T& t1, const T& t2){ return t1 >= t2 ? t1 : t2; });
}

template<typename T>
inline void computeMean(std::pair<bool, T>& result, const std::pair<bool, T>& op1, const std::pair<bool, T>& op2)
{
	compute(result, op1, op2, [](const T& t1, const T& t2){ return (t1 + t2) / 2; });
}

template<typename T>
inline std::pair<bool, T> computeMin(const std::pair<bool, T>& op1, const std::pair<bool, T>& op2)
{
	std::pair<bool, T> result;
	computeMin(result, op1, op2);
	return result;
}

template<typename T>
inline std::pair<bool, T> computeMax(const std::pair<bool, T>& op1, const std::pair<bool, T>& op2)
{
	std::pair<bool, T> result;
	computeMax(result, op1, op2);
	return result;
}

template<typename T>
inline std::pair<bool,T> computeMean(const std::pair<bool, T>& op1, const std::pair<bool, T>& op2)
{
	std::pair<bool, T> result;
	computeMean(result, op1, op2);
	return result;
}


/**
 * @brief A handle to the database to insert meteorological measures
 *
 * An instance of this class is to be used by each meteo station
 * connector to query details about the station and insert measures in
 * the database periodically.
 */
class DbConnectionMinmax : public DbConnectionCommon
{
public:
	/**
	 * @brief Construct a connection to the database
	 *
	 * @param user the username to use
	 * @param password the password corresponding to the username
	 */
	DbConnectionMinmax(
		const std::string& address = "127.0.0.1", const std::string& user = "", const std::string& password = "",
		const std::string& pgAddress = "127.0.0.1", const std::string& pgUser = "", const std::string& pgPassword = ""
	);
	/**
	 * @brief Close the connection and destroy the database handle
	 */
	virtual ~DbConnectionMinmax() = default;

	struct Values
	{
		// Values from 6h to 6h
		std::pair<bool, float> insideTemp_max;
		std::pair<bool, float> leafTemp_max[2];
		std::pair<bool, float> outsideTemp_max;
		std::pair<bool, float> soilTemp_max[4];
		std::pair<bool, float> extraTemp_max[3];
		std::pair<bool, float> rainfall;

		// Values from 18h to 18h
		std::pair<bool, float> insideTemp_min;
		std::pair<bool, float> leafTemp_min[2];
		std::pair<bool, float> outsideTemp_min;
		std::pair<bool, float> soilTemp_min[4];
		std::pair<bool, float> extraTemp_min[3];

		// Values from 0h to 0h
		std::pair<bool, float> barometer_min;
		std::pair<bool, float> barometer_max;
		std::pair<bool, float> barometer_avg;
		std::pair<bool, int> leafWetnesses_min[2];
		std::pair<bool, int> leafWetnesses_max[2];
		std::pair<bool, int> leafWetnesses_avg[2];
		std::pair<bool, int> soilMoistures_min[4];
		std::pair<bool, int> soilMoistures_max[4];
		std::pair<bool, int> soilMoistures_avg[4];
		std::pair<bool, int> insideHum_min;
		std::pair<bool, int> insideHum_max;
		std::pair<bool, int> insideHum_avg;
		std::pair<bool, int> outsideHum_min;
		std::pair<bool, int> outsideHum_max;
		std::pair<bool, int> outsideHum_avg;
		std::pair<bool, int> extraHum_min[2];
		std::pair<bool, int> extraHum_max[2];
		std::pair<bool, int> extraHum_avg[2];
		std::pair<bool, int> solarRad_max;
		std::pair<bool, int> solarRad_avg;
		std::pair<bool, int> uv_max;
		std::pair<bool, int> uv_avg;
		std::pair<bool, std::vector<int>> winddir;
		std::pair<bool, float> windgust_max;
		std::pair<bool, float> windgust_avg;
		std::pair<bool, float> windspeed_max;
		std::pair<bool, float> windspeed_avg;
		std::pair<bool, float> rainrate_max;
		std::pair<bool, float> dewpoint_min;
		std::pair<bool, float> dewpoint_max;
		std::pair<bool, float> dewpoint_avg;
		std::pair<bool, float> et;
		std::pair<bool, int> insolation_time;

		// Computed values
		std::pair<bool, float> dayRain;
		std::pair<bool, float> monthRain;
		std::pair<bool, float> yearRain;
		std::pair<bool, float> dayEt;
		std::pair<bool, float> monthEt;
		std::pair<bool, float> yearEt;
		std::pair<bool, float> insideTemp_avg;
		std::pair<bool, float> leafTemp_avg[4];
		std::pair<bool, float> outsideTemp_avg;
		std::pair<bool, float> soilTemp_avg[4];
		std::pair<bool, float> extraTemp_avg[3];
	};

	bool insertDataPoint(const CassUuid& station, const date::sys_days& date, const Values& values);

	bool getValues6hTo6h(const CassUuid& station, const date::sys_days& date, Values& values);
	bool getValues18hTo18h(const CassUuid& station, const date::sys_days& date, Values& values);
	bool getValues0hTo0h(const CassUuid& station, const date::sys_days& date, Values& values);
	bool getYearlyValues(const CassUuid& station, const date::sys_days& date, std::pair<bool, float>& rain, std::pair<bool, float>& et);

	bool insertDataPointInTimescaleDB(const CassUuid& station, const date::sys_days& date, const Values& values);
	template<typename I>
	bool insertDataPointsInTimescaleDB(const CassUuid& station, I begin, I end)
	{
		std::lock_guard<std::mutex> mutexed{_pqTransactionMutex};
		try {
			pqxx::work tx{_pqConnection};
			for (I it = begin ; it != end ; ++it) {
				doInsertDataPointInTimescaleDB(station, it->first, it->second, tx);
			}
			tx.commit();
		} catch (const pqxx::pqxx_exception& e) {
			return false;
		}
		return true;
	}

private:
	static constexpr char SELECT_VALUES_AFTER_6H_STMT[] =
		"SELECT "
			"MAX(insidetemp)     AS insideTemp_max,"
			"MAX(leaftemp1)      AS leafTemp1_max,"
			"MAX(leaftemp2)      AS leafTemp2_max,"
			"MAX(outsidetemp)    AS outsideTemp_max,"
			"MAX(max_outside_temperature)    AS real_outsideTemp_max,"
			"MAX(soiltemp1)      AS soilTemp1_max,"
			"MAX(soiltemp2)      AS soilTemp2_max,"
			"MAX(soiltemp3)      AS soilTemp3_max,"
			"MAX(soiltemp4)      AS soilTemp4_max,"
			"MAX(extratemp1)     AS extraTemp1_max,"
			"MAX(extratemp2)     AS extraTemp2_max,"
			"MAX(extratemp3)     AS extraTemp3_max,"
			"meteodata_v2.sum(rainfall)       AS rainfall,"
			"MAX(rainrate)       AS rainrate_max"
			" FROM meteodata_v2.meteo WHERE station = ? AND day = ? AND time > ?";

	static constexpr char SELECT_VALUES_BEFORE_6H_STMT[] =
		"SELECT "
			"MAX(insidetemp)     AS insideTemp_max,"
			"MAX(leaftemp1)      AS leafTemp1_max,"
			"MAX(leaftemp2)      AS leafTemp2_max,"
			"MAX(outsidetemp)    AS outsideTemp_max,"
			"MAX(max_outside_temperature)    AS real_outsideTemp_max,"
			"MAX(soiltemp1)      AS soilTemp1_max,"
			"MAX(soiltemp2)      AS soilTemp2_max,"
			"MAX(soiltemp3)      AS soilTemp3_max,"
			"MAX(soiltemp4)      AS soilTemp4_max,"
			"MAX(extratemp1)     AS extraTemp1_max,"
			"MAX(extratemp2)     AS extraTemp2_max,"
			"MAX(extratemp3)     AS extraTemp3_max,"
			"meteodata_v2.sum(rainfall)       AS rainfall,"
			"MAX(rainrate)       AS rainrate_max"
			" FROM meteodata_v2.meteo WHERE station = ? AND day = ? AND time <= ?";

	static constexpr char SELECT_VALUES_FROM_6H_TO_6H_POSTGRESQL[] = "select_values_from_6h_to_6h";
	static constexpr char SELECT_VALUES_FROM_6H_TO_6H_POSTGRESQL_STMT[] =
		"SELECT "
			"MAX(insidetemp)     AS insideTemp_max,"
			"MAX(leaftemp1)      AS leafTemp1_max,"
			"MAX(leaftemp2)      AS leafTemp2_max,"
			"MAX(outsidetemp)    AS outsideTemp_max,"
			"MAX(max_outside_temperature)    AS real_outsideTemp_max,"
			"MAX(soiltemp1)      AS soilTemp1_max,"
			"MAX(soiltemp2)      AS soilTemp2_max,"
			"MAX(soiltemp3)      AS soilTemp3_max,"
			"MAX(soiltemp4)      AS soilTemp4_max,"
			"MAX(extratemp1)     AS extraTemp1_max,"
			"MAX(extratemp2)     AS extraTemp2_max,"
			"MAX(extratemp3)     AS extraTemp3_max,"
			"SUM(rainfall)       AS rainfall,"
			"MAX(rainrate)       AS rainrate_max"
			" FROM meteodata.observations WHERE station = $1 AND datetime >= ($2::timestamptz + INTERVAL 'PT6H') AND datetime < ($2::timestamptz + INTERVAL 'PT30H')";


	static constexpr char SELECT_VALUES_ALL_DAY_STMT[] =
		"SELECT "
			"MIN(barometer)               AS barometer_min,"
			"MAX(barometer)               AS barometer_max,"
			"meteodata_v2.avg(barometer)  AS barometer_avg,"
			"MIN(leafwetnesses1)          AS leafWetnesses1_min,"
			"MAX(leafwetnesses1)          AS leafWetnesses1_max,"
			"meteodata_v2.avg(leafwetnesses1)          AS leafWetnesses1_avg,"
			"MIN(leafwetnesses2)          AS leafWetnesses2_min,"
			"MAX(leafwetnesses2)          AS leafWetnesses2_max,"
			"meteodata_v2.avg(leafwetnesses2)          AS leafWetnesses2_avg,"
			"MIN(soilmoistures1)          AS soilMoistures1_min,"
			"MAX(soilmoistures1)          AS soilMoistures1_max,"
			"meteodata_v2.avg(soilmoistures1)          AS soilMoistures1_avg,"
			"MIN(soilmoistures2)          AS soilMoistures2_min,"
			"MAX(soilmoistures2)          AS soilMoistures2_max,"
			"meteodata_v2.avg(soilmoistures2)          AS soilMoistures2_avg,"
			"MIN(soilmoistures3)          AS soilMoistures3_min,"
			"MAX(soilmoistures3)          AS soilMoistures3_max,"
			"meteodata_v2.avg(soilmoistures3)          AS soilMoistures3_avg,"
			"MIN(soilmoistures4)          AS soilMoistures4_min,"
			"MAX(soilmoistures4)          AS soilMoistures4_max,"
			"meteodata_v2.avg(soilmoistures4)          AS soilMoistures4_avg,"
			"MIN(insidehum)               AS insideHum_min,"
			"MAX(insidehum)               AS insideHum_max,"
			"meteodata_v2.avg(insidehum)  AS insideHum_avg,"
			"MIN(outsidehum)              AS outsideHum_min,"
			"MAX(outsidehum)              AS outsideHum_max,"
			"meteodata_v2.avg(outsidehum) AS outsideHum_avg,"
			"MIN(extrahum1)               AS extraHum1_min,"
			"MAX(extrahum1)               AS extraHum1_max,"
			"meteodata_v2.avg(extrahum1)  AS extraHum1_avg,"
			"MIN(extrahum2)               AS extraHum2_min,"
			"MAX(extrahum2)               AS extraHum2_max,"
			"meteodata_v2.avg(extrahum2)  AS extraHum2_avg,"
			"MAX(solarrad)                AS solarRad_max,"
			"meteodata_v2.avg(solarrad)   AS solarRad_avg,"
			"MAX(uv)                      AS uv_max,"
			"meteodata_v2.avg(uv)         AS uv_avg,"
			"MAX(windgust)                AS windgust_max,"
			"meteodata_v2.avg(windgust)   AS windgust_avg,"
			"MAX(windspeed)               AS windspeed_max,"
			"meteodata_v2.avg(windspeed)  AS windspeed_avg,"
			"MIN(dewpoint)                AS dewpoint_min,"
			"MAX(dewpoint)                AS dewpoint_max,"
			"meteodata_v2.avg(dewpoint)   AS dewpoint_avg,"
			"meteodata_v2.sum(et)                      AS et,"
			"meteodata_v2.sum(insolation_time)         AS insolation_time,"
			"rainfall24                   AS rainfall24,"
			"insolation_time24            AS insolation_time24,"
			"tx                           AS tx,"
			"tn                           AS tn "
		//	" FROM meteodata.meteo WHERE station = ? AND time >= ? AND time < ?";
			" FROM meteodata_v2.meteo WHERE station = ? AND day = ?";

	static constexpr char SELECT_VALUES_ALL_DAY_POSTGRESQL[] = "select_values_all_day";
	static constexpr char SELECT_VALUES_ALL_DAY_POSTGRESQL_STMT[] =
		"SELECT "
			"MIN(barometer)          AS barometer_min,"
			"MAX(barometer)          AS barometer_max,"
			"AVG(barometer)          AS barometer_avg,"
			"MIN(leafwetnesses1)     AS leafWetnesses1_min,"
			"MAX(leafwetnesses1)     AS leafWetnesses1_max,"
			"AVG(leafwetnesses1)     AS leafWetnesses1_avg,"
			"MIN(leafwetnesses2)     AS leafWetnesses2_min,"
			"MAX(leafwetnesses2)     AS leafWetnesses2_max,"
			"AVG(leafwetnesses2)     AS leafWetnesses2_avg,"
			"MIN(soilmoistures1)     AS soilMoistures1_min,"
			"MAX(soilmoistures1)     AS soilMoistures1_max,"
			"AVG(soilmoistures1)     AS soilMoistures1_avg,"
			"MIN(soilmoistures2)     AS soilMoistures2_min,"
			"MAX(soilmoistures2)     AS soilMoistures2_max,"
			"AVG(soilmoistures2)     AS soilMoistures2_avg,"
			"MIN(soilmoistures3)     AS soilMoistures3_min,"
			"MAX(soilmoistures3)     AS soilMoistures3_max,"
			"AVG(soilmoistures3)     AS soilMoistures3_avg,"
			"MIN(soilmoistures4)     AS soilMoistures4_min,"
			"MAX(soilmoistures4)     AS soilMoistures4_max,"
			"AVG(soilmoistures4)     AS soilMoistures4_avg,"
			"MIN(insidehum)          AS insideHum_min,"
			"MAX(insidehum)          AS insideHum_max,"
			"AVG(insidehum)          AS insideHum_avg,"
			"MIN(outsidehum)         AS outsideHum_min,"
			"MAX(outsidehum)         AS outsideHum_max,"
			"AVG(outsidehum)         AS outsideHum_avg,"
			"MIN(extrahum1)          AS extraHum1_min,"
			"MAX(extrahum1)          AS extraHum1_max,"
			"AVG(extrahum1)          AS extraHum1_avg,"
			"MIN(extrahum2)          AS extraHum2_min,"
			"MAX(extrahum2)          AS extraHum2_max,"
			"AVG(extrahum2)          AS extraHum2_avg,"
			"MAX(solarrad)           AS solarRad_max,"
			"AVG(solarrad)           AS solarRad_avg,"
			"MAX(uv)                 AS uv_max,"
			"AVG(uv)                 AS uv_avg,"
			"MAX(windgust)           AS windgust_max,"
			"AVG(windgust)           AS windgust_avg,"
			"MAX(windspeed)          AS windspeed_max,"
			"AVG(windspeed)          AS windspeed_avg,"
			"MIN(dewpoint)           AS dewpoint_min,"
			"MAX(dewpoint)           AS dewpoint_max,"
			"AVG(dewpoint)           AS dewpoint_avg,"
			"SUM(et)                 AS et,"
			"SUM(insolation_time)    AS insolation_time,"
			"MAX(rainfall24)         AS rainfall24,"
			"MAX(insolation_time24)  AS insolation_time24,"
			"MAX(tx)                 AS tx,"
			"MIN(tn)                 AS tn "
			" FROM meteodata.observations WHERE station = $1 AND datetime >= $2::timestamptz AND datetime < ($2::timestamptz + INTERVAL 'PT24H')";

	static constexpr char SELECT_VALUES_AFTER_18H_STMT[] =
		"SELECT "
			"MIN(insidetemp)     AS insideTemp_min,"
			"MIN(leaftemp1)      AS leafTemp1_min,"
			"MIN(leaftemp2)      AS leafTemp2_min,"
			"MIN(outsidetemp)    AS outsideTemp_min,"
			"MIN(min_outside_temperature)    AS real_outsideTemp_min,"
			"MIN(soiltemp1)      AS soilTemp1_min,"
			"MIN(soiltemp2)      AS soilTemp2_min,"
			"MIN(soiltemp3)      AS soilTemp3_min,"
			"MIN(soiltemp4)      AS soilTemp4_min,"
			"MIN(extratemp1)     AS extraTemp1_min,"
			"MIN(extratemp2)     AS extraTemp2_min,"
			"MIN(extratemp3)     AS extraTemp3_min "
		//	" FROM meteodata.meteo WHERE station = ? AND time >= ? AND time < ?";
			" FROM meteodata_v2.meteo WHERE station = ? AND day = ? AND time >= ?";

	static constexpr char SELECT_VALUES_BEFORE_18H_STMT[] =
		"SELECT "
			"MIN(insidetemp)     AS insideTemp_min,"
			"MIN(leaftemp1)      AS leafTemp1_min,"
			"MIN(leaftemp2)      AS leafTemp2_min,"
			"MIN(outsidetemp)    AS outsideTemp_min,"
			"MIN(min_outside_temperature)    AS real_outsideTemp_min,"
			"MIN(soiltemp1)      AS soilTemp1_min,"
			"MIN(soiltemp2)      AS soilTemp2_min,"
			"MIN(soiltemp3)      AS soilTemp3_min,"
			"MIN(soiltemp4)      AS soilTemp4_min,"
			"MIN(extratemp1)     AS extraTemp1_min,"
			"MIN(extratemp2)     AS extraTemp2_min,"
			"MIN(extratemp3)     AS extraTemp3_min "
		//	" FROM meteodata.meteo WHERE station = ? AND time >= ? AND time < ?";
			" FROM meteodata_v2.meteo WHERE station = ? AND day = ? AND time < ?";

	static constexpr char SELECT_VALUES_FROM_18H_TO_18H_POSTGRESQL[] = "select_values_from_18h_to_18h";
	static constexpr char SELECT_VALUES_FROM_18H_TO_18H_POSTGRESQL_STMT[] =
		"SELECT "
			"MIN(insidetemp)     AS insideTemp_min,"
			"MIN(leaftemp1)      AS leafTemp1_min,"
			"MIN(leaftemp2)      AS leafTemp2_min,"
			"MIN(outsidetemp)    AS outsideTemp_min,"
			"MIN(min_outside_temperature)    AS real_outsideTemp_min,"
			"MIN(soiltemp1)      AS soilTemp1_min,"
			"MIN(soiltemp2)      AS soilTemp2_min,"
			"MIN(soiltemp3)      AS soilTemp3_min,"
			"MIN(soiltemp4)      AS soilTemp4_min,"
			"MIN(extratemp1)     AS extraTemp1_min,"
			"MIN(extratemp2)     AS extraTemp2_min,"
			"MIN(extratemp3)     AS extraTemp3_min "
			" FROM meteodata.observations WHERE station = $1 AND datetime >= ($2::timestamptz - INTERVAL 'PT6H') AND datetime < ($2::timestamptz + INTERVAL 'PT18H')";

	/**
	 * @brief The first prepared statement for the getValues()
	 * method
	 */
	CassandraStmtPtr _selectValuesAfter6h;
	CassandraStmtPtr _selectValuesAfter18h;
	CassandraStmtPtr _selectValuesAllDay;
	CassandraStmtPtr _selectValuesBefore6h;
	CassandraStmtPtr _selectValuesBefore18h;

	static constexpr char SELECT_YEARLY_VALUES_STMT[] =
		"SELECT yearrain,yearet FROM meteodata_v2.minmax WHERE station = ? AND monthyear = ? AND day = ?";
	static constexpr char SELECT_YEARLY_VALUES_POSTGRESQL[] = "select_yearly_values";
	static constexpr char SELECT_YEARLY_VALUES_POSTGRESQL_STMT[] =
		"SELECT yearrain,yearet FROM meteodata.minmax WHERE station = $1 AND day = $2";

	CassandraStmtPtr _selectYearlyValues;

	static constexpr char INSERT_DATAPOINT_STMT[] =
		"INSERT INTO meteodata_v2.minmax ("
		"station,"
		"monthyear, day,"
		"barometer_min, barometer_max, barometer_avg,"
		"dayet, monthet, yearet,"
		"dayrain, monthrain, yearrain,"
		"dewpoint_max, dewpoint_avg,"
		"insidehum_min, insidehum_max, insidehum_avg,"
		"insidetemp_min, insidetemp_max, insidetemp_avg,"
		"leaftemp1_min, leaftemp1_max, leaftemp1_avg,"
		"leaftemp2_min, leaftemp2_max, leaftemp2_avg,"
		"leafwetnesses1_min, leafwetnesses1_max, leafwetnesses1_avg,"
		"leafwetnesses2_min, leafwetnesses2_max, leafwetnesses2_avg,"
		"outsidehum_min, outsidehum_max, outsidehum_avg,"
		"outsidetemp_min, outsidetemp_max, outsidetemp_avg,"
		"rainrate_max,"
		"soilmoistures1_min, soilmoistures1_max, soilmoistures1_avg,"
		"soilmoistures2_min, soilmoistures2_max, soilmoistures2_avg,"
		"soilmoistures3_min, soilmoistures3_max, soilmoistures3_avg,"
		"soilmoistures4_min, soilmoistures4_max, soilmoistures4_avg,"
		"soiltemp1_min, soiltemp1_max, soiltemp1_avg,"
		"soiltemp2_min, soiltemp2_max, soiltemp2_avg,"
		"soiltemp3_min, soiltemp3_max, soiltemp3_avg,"
		"soiltemp4_min, soiltemp4_max, soiltemp4_avg,"
		"extratemp1_min, extratemp1_max, extratemp1_avg,"
		"extratemp2_min, extratemp2_max, extratemp2_avg,"
		"extratemp3_min, extratemp3_max, extratemp3_avg,"
		"extrahum1_min, extrahum1_max, extrahum1_avg,"
		"extrahum2_min, extrahum2_max, extrahum2_avg,"
		"solarrad_max, solarrad_avg,"
		"uv_max, uv_avg,"
		"winddir,"
		"windgust_max, windgust_avg,"
		"windspeed_max, windspeed_avg,"
		"insolation_time)"
		" VALUES ("
		"?,"
		"?, ?,"
		"?, ?, ?,"
		"?, ?, ?,"
		"?, ?, ?,"
		"?, ?,"
		"?, ?, ?,"
		"?, ?, ?,"
		"?, ?, ?,"
		"?, ?, ?,"
		"?, ?, ?,"
		"?, ?, ?,"
		"?, ?, ?,"
		"?, ?, ?,"
		"?,"
		"?, ?, ?,"
		"?, ?, ?,"
		"?, ?, ?,"
		"?, ?, ?,"
		"?, ?, ?,"
		"?, ?, ?,"
		"?, ?, ?,"
		"?, ?, ?,"
		"?, ?, ?,"
		"?, ?, ?,"
		"?, ?, ?,"
		"?, ?, ?,"
		"?, ?, ?,"
		"?, ?,"
		"?, ?,"
		"?,"
		"?, ?,"
		"?, ?,"
		"?)";

	static constexpr char UPSERT_DATAPOINT_POSTGRESQL[] = "upsert_minmax_datapoint";
	static constexpr char UPSERT_DATAPOINT_POSTGRESQL_STMT[] =
		"INSERT INTO meteodata.minmax ("
		"station,"
		"day,"
		"barometer_min, barometer_max, barometer_avg,"
		"dayet, monthet, yearet,"
		"dayrain, monthrain, yearrain,"
		"dewpoint_max, dewpoint_avg,"
		"insidehum_min, insidehum_max, insidehum_avg,"
		"insidetemp_min, insidetemp_max, insidetemp_avg,"
		"leaftemp1_min, leaftemp1_max, leaftemp1_avg,"
		"leaftemp2_min, leaftemp2_max, leaftemp2_avg,"
		"leafwetnesses1_min, leafwetnesses1_max, leafwetnesses1_avg,"
		"leafwetnesses2_min, leafwetnesses2_max, leafwetnesses2_avg,"
		"outsidehum_min, outsidehum_max, outsidehum_avg,"
		"outsidetemp_min, outsidetemp_max, outsidetemp_avg,"
		"rainrate_max,"
		"soilmoistures1_min, soilmoistures1_max, soilmoistures1_avg,"
		"soilmoistures2_min, soilmoistures2_max, soilmoistures2_avg,"
		"soilmoistures3_min, soilmoistures3_max, soilmoistures3_avg,"
		"soilmoistures4_min, soilmoistures4_max, soilmoistures4_avg,"
		"soiltemp1_min, soiltemp1_max, soiltemp1_avg,"
		"soiltemp2_min, soiltemp2_max, soiltemp2_avg,"
		"soiltemp3_min, soiltemp3_max, soiltemp3_avg,"
		"soiltemp4_min, soiltemp4_max, soiltemp4_avg,"
		"extratemp1_min, extratemp1_max, extratemp1_avg,"
		"extratemp2_min, extratemp2_max, extratemp2_avg,"
		"extratemp3_min, extratemp3_max, extratemp3_avg,"
		"extrahum1_min, extrahum1_max, extrahum1_avg,"
		"extrahum2_min, extrahum2_max, extrahum2_avg,"
		"solarrad_max, solarrad_avg,"
		"uv_max, uv_avg,"
		"winddir,"
		"windgust_max, windgust_avg,"
		"windspeed_max, windspeed_avg,"
		"insolation_time"
		") VALUES ("
		"$1,"
		"$2,"
		"$3, $4, $5,"
		"$6, $7, $8,"
		"$9,  $10, $11,"
		"$12, $13,"
		"$14, $15, $16,"
		"$17, $18, $19,"
		"$20, $21, $22,"
		"$23, $24, $25,"
		"$26, $27, $28,"
		"$29, $30, $31,"
		"$32, $33, $34,"
		"$35, $36, $37,"
		"$38,"
		"$39, $40, $41,"
		"$42, $43, $44,"
		"$45, $46, $47,"
		"$48, $49, $50,"
		"$51, $52, $53,"
		"$54, $55, $56,"
		"$57, $58, $59,"
		"$60, $61, $62,"
		"$63, $64, $65,"
		"$66, $67, $68,"
		"$69, $70, $71,"
		"$72, $73, $74,"
		"$75, $76, $77,"
		"$78, $79,"
		"$80, $81,"
		"$82,"
		"$83, $84,"
		"$85, $86,"
		"$87) "
	        " ON CONFLICT (station, day) DO UPDATE "
		"SET "
		"barometer_min=COALESCE($3, barometer_min),"
		"barometer_max=COALESCE($4, barometer_max),"
		"barometer_avg=COALESCE($5, barometer_avg),"
		"dayet=COALESCE($6, dayet),"
		"monthet=COALESCE($7, monthet),"
		"yearet=COALESCE($8, yearet),"
		"dayrain=COALESCE($9, dayrain),"
		"monthrain=COALESCE($10, monthrain),"
		"yearrain=COALESCE($11, yearrain),"
		"dewpoint_max=COALESCE($12, dewpoint_max),"
		"dewpoint_avg=COALESCE($13, dewpoint_avg),"
		"insidehum_min=COALESCE($14, insidehum_min),"
		"insidehum_max=COALESCE($15, insidehum_max),"
		"insidehum_avg=COALESCE($16, insidehum_avg),"
		"insidetemp_min=COALESCE($17, insidetemp_min),"
		"insidetemp_max=COALESCE($18, insidetemp_max),"
		"insidetemp_avg=COALESCE($19, insidetemp_avg),"
		"leaftemp1_min=COALESCE($20, leaftemp1_min),"
		"leaftemp1_max=COALESCE($21, leaftemp1_max),"
		"leaftemp1_avg=COALESCE($22, leaftemp1_avg),"
		"leaftemp2_min=COALESCE($23, leaftemp2_min),"
		"leaftemp2_max=COALESCE($24, leaftemp2_max),"
		"leaftemp2_avg=COALESCE($25, leaftemp2_avg),"
		"leafwetnesses1_min=COALESCE($26, leafwetnesses1_min),"
		"leafwetnesses1_max=COALESCE($27, leafwetnesses1_max),"
		"leafwetnesses1_avg=COALESCE($28, leafwetnesses1_avg),"
		"leafwetnesses2_min=COALESCE($29, leafwetnesses2_min),"
		"leafwetnesses2_max=COALESCE($30, leafwetnesses2_max),"
		"leafwetnesses2_avg=COALESCE($31, leafwetnesses2_avg),"
		"outsidehum_min=COALESCE($32, outsidehum_min),"
		"outsidehum_max=COALESCE($33, outsidehum_max),"
		"outsidehum_avg=COALESCE($34, outsidehum_avg),"
		"outsidetemp_min=COALESCE($35, outsidetemp_min),"
		"outsidetemp_max=COALESCE($36, outsidetemp_max),"
		"outsidetemp_avg=COALESCE($37, outsidetemp_avg),"
		"rainrate_max=COALESCE($38, rainrate_max),"
		"soilmoistures1_min=COALESCE($39, soilmoistures1_min),"
		"soilmoistures1_max=COALESCE($40, soilmoistures1_max),"
		"soilmoistures1_avg=COALESCE($41, soilmoistures1_avg),"
		"soilmoistures2_min=COALESCE($42, soilmoistures2_min),"
		"soilmoistures2_max=COALESCE($43, soilmoistures2_max),"
		"soilmoistures2_avg=COALESCE($44, soilmoistures2_avg),"
		"soilmoistures3_min=COALESCE($45, soilmoistures3_min),"
		"soilmoistures3_max=COALESCE($46, soilmoistures3_max),"
		"soilmoistures3_avg=COALESCE($47, soilmoistures3_avg),"
		"soilmoistures4_min=COALESCE($48, soilmoistures4_min),"
		"soilmoistures4_max=COALESCE($49, soilmoistures4_max),"
		"soilmoistures4_avg=COALESCE($50, soilmoistures4_avg),"
		"soiltemp1_min=COALESCE($51, soiltemp1_min),"
		"soiltemp1_max=COALESCE($52, soiltemp1_max),"
		"soiltemp1_avg=COALESCE($53, soiltemp1_avg),"
		"soiltemp2_min=COALESCE($54, soiltemp2_min),"
		"soiltemp2_max=COALESCE($55, soiltemp2_max),"
		"soiltemp2_avg=COALESCE($56, soiltemp2_avg),"
		"soiltemp3_min=COALESCE($57, soiltemp3_min),"
		"soiltemp3_max=COALESCE($58, soiltemp3_max),"
		"soiltemp3_avg=COALESCE($59, soiltemp3_avg),"
		"soiltemp4_min=COALESCE($60, soiltemp4_min),"
		"soiltemp4_max=COALESCE($61, soiltemp4_max),"
		"soiltemp4_avg=COALESCE($62, soiltemp4_avg),"
		"extratemp1_min=COALESCE($63, extratemp1_min),"
		"extratemp1_max=COALESCE($64, extratemp1_max),"
		"extratemp1_avg=COALESCE($65, extratemp1_avg),"
		"extratemp2_min=COALESCE($66, extratemp2_min),"
		"extratemp2_max=COALESCE($67, extratemp2_max),"
		"extratemp2_avg=COALESCE($68, extratemp2_avg),"
		"extratemp3_min=COALESCE($69, extratemp3_min),"
		"extratemp3_max=COALESCE($70, extratemp3_max),"
		"extratemp3_avg=COALESCE($71, extratemp3_avg),"
		"extrahum1_min=COALESCE($72, extrahum1_min),"
		"extrahum1_max=COALESCE($73, extrahum1_max),"
		"extrahum1_avg=COALESCE($74, extrahum1_avg),"
		"extrahum2_min=COALESCE($75, extrahum2_min),"
		"extrahum2_max=COALESCE($76, extrahum2_max),"
		"extrahum2_avg=COALESCE($77, extrahum2_avg),"
		"solarrad_max=COALESCE($78, solarrad_max),"
		"solarrad_avg=COALESCE($79, solarrad_avg),"
		"uv_max=COALESCE($80, uv_max),"
		"uv_avg=COALESCE($81, uv_avg),"
		"winddir=COALESCE($82, winddir),"
		"windgust_max=COALESCE($83, windgust_max),"
		"windgust_avg=COALESCE($84, windgust_avg),"
		"windspeed_max=COALESCE($85, windspeed_max),"
		"windspeed_avg=COALESCE($86, windspeed_avg),"
		"insolation_time=COALESCE($87, insolation_time)"
	;

	/**
	 * @brief The prepared statement for the insetDataPoint() method
	 */
	CassandraStmtPtr _insertDataPoint;
	/**
	 * @brief Prepare the Cassandra query/insert statements
	 */
	void prepareStatements();

	pqxx::connection _pqConnection;

	std::mutex _pqTransactionMutex;

	void doInsertDataPointInTimescaleDB(const CassUuid& station, const date::sys_days& date, const Values& values, pqxx::transaction_base& tx);
};
}

#endif
