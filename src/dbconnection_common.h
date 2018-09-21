/**
 * @file dbconnection_common.h
 * @brief Definition of the DbConnectionCommon class
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

#ifndef DBCONNECTION_COMMON_H
#define DBCONNECTION_COMMON_H

#include <ctime>
#include <functional>
#include <tuple>
#include <vector>
#include <string>
#include <utility>

#include <cassandra.h>
#include <date.h>

namespace meteodata {

namespace chrono = std::chrono;

class DbConnectionCommon
{
	public:
		/**
		 * @brief Construct a connection to the database
		 *
		 * @param user the username to use
		 * @param password the password corresponding to the username
		 */
		DbConnectionCommon(const std::string& address = "127.0.0.1", const std::string& user = "", const std::string& password = "");
		/**
		 * @brief Close the connection and destroy the database handle
		 */
		virtual ~DbConnectionCommon() = default;


		bool getAllStations(std::vector<CassUuid>& stations);
		bool getWindValues(const CassUuid& station, const date::sys_days& date, std::vector<std::pair<int,float>>& values);

	protected:
		/**
		 * @brief The Cassandra session data
		 */
		std::unique_ptr<CassSession, std::function<void(CassSession*)>> _session;
		/**
		 * @brief The Cassandra cluster
		 */
		std::unique_ptr<CassCluster, std::function<void(CassCluster*)>> _cluster;

		/**
		 * @brief Retrieve an integer from a Cassandra result row
		 *
		 * @param[in] row A Cassandra row, resulting from a SELECT query
		 * @param[in] column The column index of the value to be retrieved in the \a row
		 * @param[out] value A pair (bool,int), the first element is
		 * false if the value in the row is null, the second element contains the
		 * integer if the first element is true
		 */
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

		/**
		 * @brief Retrieve a float from a Cassandra result row
		 *
		 * @param[in] row A Cassandra row, resulting from a SELECT query
		 * @param[in] column The column index of the value to be retrieved in the \a row
		 * @param[out] value A pair (bool,float), the first element is
		 * false if the value in the row is null, the second element contains the
		 * integer if the first element is true
		 */
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

		/**
		 * @brief Bind a integer into a Cassandra prepared statement
		 *
		 * @param[in] stmt A Cassandra prepared statement
		 * @param[in] column The column index of the value to be bound in \a stmt
		 * @param[in] value A pair (bool,int), the first element is
		 * false if the value to be inserted is null, the second
		 * element contains the int if the first element is true
		 */
		inline void bindCassandraInt(CassStatement* stmt, int column, const std::pair<bool, int>& value)
		{
			if (value.first)
				cass_statement_bind_int32(stmt, column, value.second);
		}

		/**
		 * @brief Bind a float into a Cassandra prepared statement
		 *
		 * @param[in] stmt A Cassandra prepared statement
		 * @param[in] column The column index of the value to be bound in \a stmt
		 * @param[in] value A pair (bool,float), the first element is
		 * false if the value to be inserted is null, the second
		 * element contains the float if the first element is true
		 */
		inline void bindCassandraFloat(CassStatement* stmt, int column, const std::pair<bool, float>& value)
		{
			if (value.first)
				cass_statement_bind_float(stmt, column, value.second);
		}

		/**
		 * @brief Bind a list of integers into a Cassandra prepared statement
		 *
		 * @param[in] stmt A Cassandra prepared statement
		 * @param[in] column The column index of the value to be bound in \a stmt
		 * @param[in] value A pair (bool,list), the first element is
		 * false if the value to be inserted is null, the second
		 * element contains the list if the first element is true
		 */
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

		/**
		 * @brief Convert a date object to a Cassandra date integer
		 *
		 * @param[in] d The date to convert
		 *
		 * @return The corresponding value that can be used in a Cassandra statement
		 */
		inline uint32_t from_sysdays_to_CassandraDate(const date::sys_days& d)
		{
			date::sys_time<chrono::seconds> tp = d;
			return cass_date_from_epoch(tp.time_since_epoch().count());
		}

		/**
		 * @brief Convert a month and year to a Cassandra date range
		 *
		 * @param[in] y The year
		 * @param[in] m The month
		 *
		 * This utility function builds pairs of the form (y-m-1, y-m-<last day of month>)
		 * where the elements are Cassandra date integers than can be used in prepared
		 * statements.
		 * *
		 * @return A pair of Cassandra date values that can be used in a Cassandra statement
		 */
		inline std::pair<uint32_t,uint32_t> from_monthyear_to_CassandraDates(int y, int m)
		{
			date::sys_time<chrono::seconds> begin = date::sys_days{date::year{y}/m/1};
			date::sys_time<chrono::seconds> end = begin + date::months{1};
			return std::make_pair(cass_date_from_epoch(begin.time_since_epoch().count()), cass_date_from_epoch(end.time_since_epoch().count()));
		}

		/**
		 * @brief Convert a datetime object to a Cassandra datetime integer
		 *
		 * @tparam T The resolution of the datetime object
		 * @param d The datetime object to convert
		 *
		 * @return A Cassandra datetime object that can be used in a Cassandra statement
		 */
		template<typename T>
		inline int64_t from_systime_to_CassandraDateTime(const date::sys_time<T>& d)
		{
			date::sys_time<chrono::milliseconds> tp = d;
			return tp.time_since_epoch().count();
		}

	private:
		/**
		 * @brief The raw query string to select all stations from the database
		 */
		static constexpr char SELECT_ALL_STATIONS_STMT[] = "SELECT id FROM meteodata.stations";
		/**
		 * @brief The raw query string to select all MeteoFrance stations in the SYNOP network from the database
		 */
		static constexpr char SELECT_ALL_STATIONS_FR_STMT[] = "SELECT id FROM meteodata.stationsFR WHERE type=0";
		/**
		 * @brief The first prepared statement for the getAllStations() method
		 */
		std::unique_ptr<const CassPrepared, std::function<void(const CassPrepared*)>> _selectAllStations;
		/**
		 * @brief The second prepared statement for the getAllStations() method
		 */
		std::unique_ptr<const CassPrepared, std::function<void(const CassPrepared*)>> _selectAllStationsFr;
		/**
		 * @brief The raw query string to select all wind observations for a given station on a given day
		 */
		static constexpr char SELECT_WIND_VALUES_STMT[] =
			"SELECT "
			"winddir,"
			"windspeed "
			" FROM meteodata_v2.meteo WHERE station = ? AND day = ?";
		/**
		 * @brief The prepared statement for the getWindValues() method
		 */
		std::unique_ptr<const CassPrepared, std::function<void(const CassPrepared*)>> _selectWindValues;

		/**
		 * @brief Prepare the Cassandra query/insert statements
		 */
		virtual void prepareStatements();
};
}

#endif
