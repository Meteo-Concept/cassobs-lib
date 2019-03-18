/**
 * @file dbconnection_month_minmax.h
 * @brief Definition of the DbConnectionMonthMinmax class
 * @author Laurent Georget
 * @date 2018-07-10
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

#ifndef DBCONNECTION_MONTH_MINMAX_H
#define DBCONNECTION_MONTH_MINMAX_H

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

namespace meteodata {

/**
 * @brief A handle to the database to insert meteorological measures
 *
 * An instance of this class is to be used by each meteo station
 * connector to query details about the station and insert measures in
 * the database periodically.
 */
class DbConnectionMonthMinmax : public DbConnectionCommon
{
	public:
		/**
		 * @brief Construct a connection to the database
		 *
		 * @param user the username to use
		 * @param password the password corresponding to the username
		 */
		DbConnectionMonthMinmax(const std::string& address = "127.0.0.1", const std::string& user = "", const std::string& password = "");
		/**
		 * @brief Close the connection and destroy the database handle
		 */
		virtual ~DbConnectionMonthMinmax() = default;

		struct Values
		{
			std::pair<bool, float> outsideTemp_avg;
			std::pair<bool, float> outsideTemp_max_max;
			std::pair<bool, float> outsideTemp_max_min;
			std::pair<bool, float> outsideTemp_min_max;
			std::pair<bool, float> outsideTemp_min_min;

			std::pair<bool, float> rainfall;
			std::pair<bool, float> rainrate_max;

			std::pair<bool, float> barometer_min;
			std::pair<bool, float> barometer_max;
			std::pair<bool, float> barometer_avg;

			std::pair<bool, int> outsideHum_min;
			std::pair<bool, int> outsideHum_max;
			std::pair<bool, int> solarRad_max;
			std::pair<bool, int> solarRad_avg;
			std::pair<bool, int> insolationTime;
			std::pair<bool, int> uv_max;
			std::pair<bool, float> windgust_max;
			std::pair<bool, std::vector<int>> winddir;
			std::pair<bool, float> etp;
		};

		inline void storeCassandraInt(const CassRow* row, int column, std::pair<bool, int>& value)
		{
			const CassValue* raw = cass_row_get_column(row, column);
			if (cass_value_is_null(raw)) {
				//	std::cerr << "Detected an int null value at column " << column << std::endl;
				value.first = false;
			} else {
				value.first = true;
				cass_value_get_int32(raw, &value.second);
			}
		}

		inline void storeCassandraFloat(const CassRow* row, int column, std::pair<bool, float>& value)
		{
			const CassValue* raw = cass_row_get_column(row, column);
			if (cass_value_is_null(raw)) {
				//	std::cerr << "Detected a float null value as column " << column << std::endl;
				value.first = false;
			} else {
				value.first = true;
				cass_value_get_float(raw, &value.second);
			}
		}

		inline void bindCassandraInt(CassStatement* stmt, int column, const std::pair<bool, int>& value)
		{
			if (value.first)
				cass_statement_bind_int32(stmt, column, value.second);
		}

		inline void bindCassandraFloat(CassStatement* stmt, int column, const std::pair<bool, float>& value)
		{
			if (value.first)
				cass_statement_bind_float(stmt, column, value.second);
		}

		inline void bindCassandraList(CassStatement* stmt, int column, const std::pair<bool, std::vector<int>>& values)
		{
			if (values.first) {
				CassCollection *collection = cass_collection_new(CASS_COLLECTION_TYPE_LIST, values.second.size());
				for (int v : values.second)
					cass_collection_append_int32(collection, v);
				cass_statement_bind_collection(stmt, column, collection);
				cass_collection_free(collection);
			}
		}

		bool insertDataPoint(const CassUuid& station, int year, int month, const Values& values);

		bool getDailyValues(const CassUuid& station, int year, int month, Values& values);

	private:
		static constexpr char SELECT_DAILY_VALUES_STMT[] =
			"SELECT "
			"meteodata_v2.avg(outsidetemp_avg) AS outsidetemp, "
			"MAX(outsidetemp_max)		AS outsidetemp_max_max, "
			"MIN(outsidetemp_max)		AS outsidetemp_max_min, "
			"MAX(outsidetemp_min)		AS outsidetemp_min_max, "
			"MIN(outsidetemp_min)		AS outsidetemp_min_min, "
			"MAX(windgust_max)		AS windgust_max, "
			"SUM(dayrain)			AS rainfall, "
			"MAX(rainrate_max)		AS rainrate_max, "
			"SUM(dayet)			AS etp, "
			"MIN(barometer_min)		AS barometer_min, "
			"meteodata_v2.avg(barometer_avg) AS barometer_avg, "
			"MAX(barometer_max)		AS barometer_max, "
			"MIN(outsidehum_min)		AS outsidehum_min, "
			"MAX(outsidehum_max)		AS outsidehum_max, "
			"MAX(solarrad_max)		AS solarrad_max, "
			"meteodata_v2.avg(solarrad_avg)	AS solarrad_avg, "
			"MAX(uv_max)			AS uv_max, "
			"SUM(insolation_time)		AS insolation_time "
			" FROM meteodata_v2.minmax WHERE station = ? AND monthyear = ?";
			//" FROM meteodata.minmax WHERE station = ? AND date >= ? AND date < ?";
		/**
		 * @brief The first prepared statement for the getValues()
		 * method
		 */
		CassandraStmtPtr _selectDailyValues;

		static constexpr char INSERT_DATAPOINT_STMT[] =
			"INSERT INTO meteodata_v2.month_minmax ("
			"station,"
			"year,"
			"month,"
			"barometer_avg,"
			"barometer_max,"
			"barometer_min,"
			"etp,"
			"outsidehum_max,"
			"outsidehum_min,"
			"outsidetemp_avg,"
			"outsidetemp_max_max,"
			"outsidetemp_max_min,"
			"outsidetemp_min_max,"
			"outsidetemp_min_min,"
			"rainfall,"
			"rainrate_max,"
			"solarrad_avg,"
			"solarrad_max,"
			"uv_max,"
			"winddir,"
			"windgust_speed_max,"
			"insolation_time)"
			" VALUES ("
			"?,"
			"?,"
			"?,"
			"?,"
			"?,"
			"?,"
			"?,"
			"?,"
			"?,"
			"?,"
			"?,"
			"?,"
			"?,"
			"?,"
			"?,"
			"?,"
			"?,"
			"?,"
			"?,"
			"?,"
			"?,"
			"?)";
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
