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

#include <pqxx/pqxx>
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
		DbConnectionMonthMinmax(
			const std::string& address = "127.0.0.1", const std::string& user = "", const std::string& password = "",
			const std::string& pgAddress = "127.0.0.1", const std::string& pgUser = "", const std::string& pgPassword = ""
		);
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
			std::pair<bool, float> rainfall_max;
			std::pair<bool, float> rainrate_max;

			std::pair<bool, float> barometer_min;
			std::pair<bool, float> barometer_max;
			std::pair<bool, float> barometer_avg;

			std::pair<bool, int> outsideHum_min;
			std::pair<bool, int> outsideHum_max;

			std::pair<bool, int> solarRad_max;
			std::pair<bool, int> solarRad_avg;
			std::pair<bool, int> insolationTime;
			std::pair<bool, int> insolationTime_max;
			std::pair<bool, int> uv_max;

			std::pair<bool, float> wind_avg;
			std::pair<bool, float> windgust_max;
			std::pair<bool, std::vector<int>> winddir;
			std::pair<bool, float> etp;

			std::pair<bool, float> diff_outsideTemp_avg;
			std::pair<bool, float> diff_outsideTemp_min_min;
			std::pair<bool, float> diff_outsideTemp_max_max;
			std::pair<bool, float> diff_rainfall;
			std::pair<bool, int> diff_insolationTime;
		};

		bool insertDataPoint(const CassUuid& station, int year, int month, const Values& values);

		bool getDailyValues(const CassUuid& station, int year, int month, Values& values);

		bool insertDataPointInTimescaleDB(const CassUuid& station, const date::year_month& yearmonth, const Values& values);
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
		static constexpr char SELECT_DAILY_VALUES_STMT[] =
			"SELECT "
			"meteodata_v2.avg(outsidetemp_avg)	AS outsidetemp, "
			"MAX(outsidetemp_max)			AS outsidetemp_max_max, "
			"MIN(outsidetemp_max)			AS outsidetemp_max_min, "
			"MAX(outsidetemp_min)			AS outsidetemp_min_max, "
			"MIN(outsidetemp_min)			AS outsidetemp_min_min, "
			"meteodata_v2.avg(windspeed_avg)	AS wind_avg, "
			"MAX(windgust_max)			AS windgust_max, "
			"meteodata_v2.sum(dayrain)		AS rainfall, "
			"MAX(dayrain)				AS rainfall_max, "
			"MAX(rainrate_max)			AS rainrate_max, "
			"meteodata_v2.sum(dayet)		AS etp, "
			"MIN(barometer_min)			AS barometer_min, "
			"meteodata_v2.avg(barometer_avg)	AS barometer_avg, "
			"MAX(barometer_max)			AS barometer_max, "
			"MIN(outsidehum_min)			AS outsidehum_min, "
			"MAX(outsidehum_max)			AS outsidehum_max, "
			"MAX(solarrad_max)			AS solarrad_max, "
			"meteodata_v2.avg(solarrad_avg)		AS solarrad_avg, "
			"MAX(uv_max)				AS uv_max, "
			"meteodata_v2.sum(insolation_time)	AS insolation_time, "
			"MAX(insolation_time)			AS insolation_time_max "
			" FROM meteodata_v2.minmax WHERE station = ? AND monthyear = ?";
			//" FROM meteodata.minmax WHERE station = ? AND date >= ? AND date < ?";

		static constexpr char SELECT_DAILY_VALUES_POSTGRESQL[] = "select_daily_values";
		static constexpr char SELECT_DAILY_VALUES_POSTGRESQL_STMT[] =
			"SELECT "
			"AVG(outsidetemp_avg)	AS outsidetemp, "
			"MAX(outsidetemp_max)	AS outsidetemp_max_max, "
			"MIN(outsidetemp_max)	AS outsidetemp_max_min, "
			"MAX(outsidetemp_min)	AS outsidetemp_min_max, "
			"MIN(outsidetemp_min)	AS outsidetemp_min_min, "
			"AVG(windspeed_avg)	AS wind_avg, "
			"MAX(windgust_max)	AS windgust_max, "
			"SUM(dayrain)		AS rainfall, "
			"MAX(dayrain)		AS rainfall_max, "
			"MAX(rainrate_max)	AS rainrate_max, "
			"SUM(dayet)		AS etp, "
			"MIN(barometer_min)	AS barometer_min, "
			"AVG(barometer_avg)	AS barometer_avg, "
			"MAX(barometer_max)	AS barometer_max, "
			"MIN(outsidehum_min)	AS outsidehum_min, "
			"MAX(outsidehum_max)	AS outsidehum_max, "
			"AVG(solarrad_avg)	AS solarrad_avg, "
			"MAX(solarrad_max)	AS solarrad_max, "
			"MAX(uv_max)		AS uv_max, "
			"SUM(insolation_time)	AS insolation_time, "
			"MAX(insolation_time)	AS insolation_time_max "
			" FROM meteodata.minmax WHERE station = $1 AND "
			" day >= date_trunc('month', $2::timestamptz) AND day < date_trunc('month', $2::timestamptz + INTERVAL 'P1M')";

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
			"barometer_max,"
			"barometer_min,"
			"barometer_avg,"
			"etp,"
			"outsidehum_max,"
			"outsidehum_min,"
			"outsidetemp_avg,"
			"outsidetemp_max_max,"
			"outsidetemp_max_min,"
			"outsidetemp_min_max,"
			"outsidetemp_min_min,"
			"rainfall,"
			"rainfall_max,"
			"rainrate_max,"
			"solarrad_max,"
			"solarrad_avg,"
			"uv_max,"
			"winddir,"
			"wind_speed_avg,"
			"windgust_speed_max,"
			"insolation_time,"
			"insolation_time_max,"
			"diff_outside_temperature_avg,"
			"diff_outside_temperature_min_min,"
			"diff_outside_temperature_max_max,"
			"diff_rainfall,"
			"diff_insolation_time)"
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
			"?,"
			"?,"
			"?,"
			"?,"
			"?,"
			"?,"
			"?,"
			"?,"
			"?)";

		static constexpr char UPSERT_DATAPOINT_POSTGRESQL[] = "upsert_minmax_datapoint";
		static constexpr char UPSERT_DATAPOINT_POSTGRESQL_STMT[] =
			"INSERT INTO meteodata.month_minmax ("
			"station,"
			"yearmonth,"
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
			"rainfall_max,"
			"rainrate_max,"
			"solarrad_avg,"
			"solarrad_max,"
			"uv_max,"
			"winddir,"
			"wind_speed_avg,"
			"windgust_speed_max,"
			"insolation_time,"
			"insolation_time_max,"
			"diff_outside_temperature_avg,"
			"diff_outside_temperature_min_min,"
			"diff_outside_temperature_max_max,"
			"diff_rainfall,"
			"diff_insolation_time "
			") VALUES ("
			"$1,"
			"date_trunc('month', $2::timestamptz),"
			"$3,"
			"$4,"
			"$5,"
			"$6,"
			"$7,"
			"$8,"
			"$9,"
			"$10,"
			"$11,"
			"$12,"
			"$13,"
			"$14,"
			"$15,"
			"$16,"
			"$17,"
			"$18,"
			"$19,"
			"$20,"
			"$21,"
			"$22,"
			"$23,"
			"$24,"
			"$25,"
			"$26,"
			"$27,"
			"$28,"
			"$29 "
			") ON CONFLICT (station, yearmonth) DO UPDATE "
			" SET "
			"barometer_avg=$3,"
			"barometer_max=$4,"
			"barometer_min=$5,"
			"etp=$6,"
			"outsidehum_max=$7,"
			"outsidehum_min=$8,"
			"outsidetemp_avg=$9,"
			"outsidetemp_max_max=$10,"
			"outsidetemp_max_min=$11,"
			"outsidetemp_min_max=$12,"
			"outsidetemp_min_min=$13,"
			"rainfall=$14,"
			"rainfall_max=$15,"
			"rainrate_max=$16,"
			"solarrad_avg=$17,"
			"solarrad_max=$18,"
			"uv_max=$19,"
			"winddir=$20,"
			"wind_speed_avg=$21,"
			"windgust_speed_max=$22,"
			"insolation_time=$23,"
			"insolation_time_max=$24,"
			"diff_outside_temperature_avg=$25,"
			"diff_outside_temperature_min_min=$26,"
			"diff_outside_temperature_max_max=$27,"
			"diff_rainfall=$28,"
			"diff_insolation_time=29 "
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

		void doInsertDataPointInTimescaleDB(const CassUuid& station, const date::year_month& yearmonth, const Values& values, pqxx::transaction_base& tx);
};
}

#endif
