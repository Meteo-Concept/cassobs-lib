/**
 * @file dbconnection_records.h
 * @brief Definition of the DbConnectionRecords class
 * @author Laurent Georget
 * @date 2019-12-18
 */
/*
 * Copyright (C) 2019  SAS Météo Concept <contact@meteo-concept.fr>
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

#ifndef DBCONNECTION_RECORDS_H
#define DBCONNECTION_RECORDS_H

#include <ctime>
#include <functional>
#include <tuple>
#include <memory>
#include <mutex>
#include <vector>
#include <unordered_map>
#include <utility>

#include <cassandra.h>
#include <date.h>

#include "dbconnection_common.h"
#include "cassandra_stmt_ptr.h"
#include "monthly_records.h"

namespace meteodata {

/**
 * @brief A handle to the database to insert meteorological records
 *
 * An instance of this class is to be used by the periodic script
 * that maintains the list of meteorological records by station.
 */
class DbConnectionRecords : public DbConnectionCommon
{
public:
	/**
	 * @brief Construct a connection to the database
	 *
	 * @param address the host of the database
	 * @param user the username to use
	 * @param password the password corresponding to the username
	 */
	DbConnectionRecords(const std::string& address = "127.0.0.1", const std::string& user = "", const std::string& password = "");
	/**
	 * @brief Close the connection and destroy the database handle
	 */
	virtual ~DbConnectionRecords() = default;

	bool insertDataPoint(const CassUuid& station, MonthlyRecords& records);

	bool getCurrentRecords(const CassUuid& station, date::month month, MonthlyRecords& values);
	bool getValuesForAllDaysInMonth(const CassUuid& station, int year, int month, MonthlyRecords& values);

private:
	static constexpr char SELECT_CURRENT_RECORDS_STMT[] =
		"SELECT "
			"outsidetemp_max_max     AS outsideTemp_max_max,"
			"outsidetemp_min_min     AS outsideTemp_min_min,"
			"outsidetemp_max_min     AS outsideTemp_max_min,"
			"outsidetemp_min_max     AS outsideTemp_min_max,"
			"outsidetemp_ampl        AS outsideTemp_ampl_max,"
			"outsidetemp_max_over_30 AS outsideTemp_max_over_30,"
			"outsidetemp_max_over_25 AS outsideTemp_max_over_25,"
			"outsidetemp_max_under_0 AS outsideTemp_max_under_0,"
			"outsidetemp_min_under_0 AS outsideTemp_min_under_0,"
			"outsidetemp_min_under_minus_5  AS outsideTemp_min_under_minus_5,"
			"outsidetemp_min_under_minus_10 AS outsideTemp_min_under_minus_10,"
			"outsidetemp_avg_max     AS outsideTemp_avg_max,"
			"outsidetemp_avg_min     AS outsideTemp_avg_min,"
			"outsidetemp_max_avg_max AS outsideTemp_max_avg_max,"
			"outsidetemp_max_avg_min AS outsideTemp_max_avg_min,"
			"outsidetemp_min_avg_max AS outsideTemp_min_avg_max,"
			"outsidetemp_min_avg_min AS outsideTemp_min_avg_min,"
			"dayrain_max             AS dayRain_max,"
			"monthrain_max           AS monthRain_max,"
			"monthrain_min           AS monthRain_min,"
			"dayrain_over_1          AS dayRain_over_1,"
			"dayrain_over_5          AS dayRain_over_5,"
			"dayrain_over_10         AS dayRain_over_10,"
			"monthinsolation_max     AS monthInsolation_max,"
			"monthinsolation_min     AS monthInsolation_min,"
			"dayinsolation_over_1    AS dayInsolation_over_1,"
			"dayinsolation_over_5    AS dayInsolation_over_5,"
			"dayinsolation_at_0      AS dayInsolation_at_0,"
			"gust_max                AS gust_max,"
			"windspeed_avg_max       AS windspeed_avg_max,"
			"windspeed_avg_min       AS windspeed_avg_min"
		//	" FROM meteodata.meteo WHERE station = ? AND time >= ? AND time < ?";
			" FROM meteodata_v2.records WHERE station = ? AND period = ?";

	static constexpr char SELECT_VALUES_FOR_ALL_DAYS_IN_MONTH_STMT[] =
		"SELECT "
			"day                 AS day,"
			"outsidetemp_max     AS outsideTemp_max,"
			"outsidetemp_min     AS outsideTemp_min,"
			"outsidetemp_avg     AS outsideTemp_avg,"
			"dayrain             AS dayrain,"
			"windspeed_avg       AS windSpeed_avg,"
			"windgust_max        AS windGust_max,"
			"insolation_time     AS insolationTime "
			" FROM meteodata_v2.minmax WHERE station = ? AND monthyear = ?";

	CassandraStmtPtr _selectCurrentRecords;
	CassandraStmtPtr _selectValuesForAllDaysInMonth;

	static constexpr char INSERT_DATAPOINT_STMT[] =
		"INSERT INTO meteodata_v2.records ("
			"station,"
			"period,"
			"outsidetemp_max_max, outsidetemp_min_min,"
			"outsidetemp_max_min, outsidetemp_min_max,"
			"outsidetemp_ampl,"
			"outsidetemp_max_over_30, outsidetemp_max_over_25,"
			"outsidetemp_max_under_0, outsidetemp_min_under_0,"
			"outsidetemp_min_under_minus_5, outsidetemp_min_under_minus_10,"
			"outsidetemp_avg_max, outsidetemp_avg_min,"
			"outsidetemp_max_avg_max, outsidetemp_max_avg_min,"
			"outsidetemp_min_avg_max, outsidetemp_min_avg_min,"
			"dayrain_max,"
			"monthrain_max, monthrain_min,"
			"dayrain_over_1, dayrain_over_5, dayrain_over_10,"
			"monthinsolation_max, monthinsolation_min,"
			"dayinsolation_over_1, dayinsolation_over_5, dayinsolation_at_0,"
			"gust_max,"
			"windspeed_avg_max, windspeed_avg_min"
		") VALUES ("
			"?,"
			"?,"
			"?, ?,"
			"?, ?,"
			"?,"
			"?, ?,"
			"?, ?,"
			"?, ?,"
			"?, ?,"
			"?, ?,"
			"?, ?,"
			"?,"
			"?, ?,"
			"?, ?, ?,"
			"?, ?,"
			"?, ?, ?,"
			"?,"
			"?, ?"
		")";
	/**
	 * @brief The prepared statement for the insetDataPoint() method
	 */
	CassandraStmtPtr _insertDataPoint;
	/**
	 * @brief Prepare the Cassandra query/insert statements
	 */
	void prepareStatements();
};
}

#endif
