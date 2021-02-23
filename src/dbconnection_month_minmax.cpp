/**
 * @file dbconnection_month_minmax.cpp
 * @brief Implementation of the DbConnectionMonthMinmax class
 * @author Laurent Georget
 * @date 2018-07-10
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
#include <chrono>
#include <unordered_map>

#include <cassandra.h>
#include <syslog.h>
#include <unistd.h>

#include "dbconnection_month_minmax.h"
#include "dbconnection_common.h"
#include "cassandra_stmt_ptr.h"

using namespace date;

namespace meteodata {

constexpr char DbConnectionMonthMinmax::INSERT_DATAPOINT_STMT[];
constexpr char DbConnectionMonthMinmax::SELECT_DAILY_VALUES_STMT[];

namespace chrono = std::chrono;

DbConnectionMonthMinmax::DbConnectionMonthMinmax(const std::string& address, const std::string& user, const std::string& password) :
	DbConnectionCommon(address, user, password)
{
	prepareStatements();
}

void DbConnectionMonthMinmax::prepareStatements()
{
	prepareOneStatement(_selectDailyValues, SELECT_DAILY_VALUES_STMT);
	prepareOneStatement(_insertDataPoint, INSERT_DATAPOINT_STMT);
}

bool DbConnectionMonthMinmax::getDailyValues(const CassUuid& uuid, int year, int month, DbConnectionMonthMinmax::Values& values)
{
	CassFuture* query;
	CassStatement* statement = cass_prepared_bind(_selectDailyValues.get());

	cass_statement_bind_uuid(statement, 0, uuid);
	cass_statement_bind_int32(statement, 1, year * 100 + month);
	query = cass_session_execute(_session.get(), statement);
	cass_statement_free(statement);

	const CassResult* result = cass_future_get_result(query);
	bool ret = false;
	if (result) {
		const CassRow* row = cass_result_first_row(result);
		if (row) {
			int param = 0;
			storeCassandraFloat(row, param++, values.outsideTemp_avg);
			storeCassandraFloat(row, param++, values.outsideTemp_max_max);
			storeCassandraFloat(row, param++, values.outsideTemp_max_min);
			storeCassandraFloat(row, param++, values.outsideTemp_min_max);
			storeCassandraFloat(row, param++, values.outsideTemp_min_min);
			storeCassandraFloat(row, param++, values.wind_avg);
			storeCassandraFloat(row, param++, values.windgust_max);
			storeCassandraFloat(row, param++, values.rainfall);
			storeCassandraFloat(row, param++, values.rainfall_max);
			storeCassandraFloat(row, param++, values.rainrate_max);
			storeCassandraFloat(row, param++, values.etp);
			storeCassandraFloat(row, param++, values.barometer_min);
			storeCassandraFloat(row, param++, values.barometer_avg);
			storeCassandraFloat(row, param++, values.barometer_max);
			storeCassandraInt(row, param++, values.outsideHum_min);
			storeCassandraInt(row, param++, values.outsideHum_max);
			storeCassandraInt(row, param++, values.solarRad_max);
			storeCassandraInt(row, param++, values.solarRad_avg);
			storeCassandraInt(row, param++, values.uv_max);
			storeCassandraInt(row, param++, values.insolationTime);
			storeCassandraInt(row, param++, values.insolationTime_max);
			ret = true;
		}
	}
	cass_result_free(result);
	cass_future_free(query);

	return ret;
}


bool DbConnectionMonthMinmax::insertDataPoint(const CassUuid& station, int year, int month, const Values& values)
{
	CassFuture* query;
	CassStatement* statement = cass_prepared_bind(_insertDataPoint.get());
	int param = 0;
	cass_statement_bind_uuid(statement,  param++, station);
	cass_statement_bind_int32(statement, param++, year);
	cass_statement_bind_int32(statement, param++, month);
	bindCassandraFloat(statement, param++, values.barometer_avg);
	bindCassandraFloat(statement, param++, values.barometer_max);
	bindCassandraFloat(statement, param++, values.barometer_min);
	bindCassandraFloat(statement, param++, values.etp);
	bindCassandraInt(statement, param++, values.outsideHum_max);
	bindCassandraInt(statement, param++, values.outsideHum_min);
	bindCassandraFloat(statement, param++, values.outsideTemp_avg);
	bindCassandraFloat(statement, param++, values.outsideTemp_max_max);
	bindCassandraFloat(statement, param++, values.outsideTemp_max_min);
	bindCassandraFloat(statement, param++, values.outsideTemp_min_max);
	bindCassandraFloat(statement, param++, values.outsideTemp_min_min);
	bindCassandraFloat(statement, param++, values.rainfall);
	bindCassandraFloat(statement, param++, values.rainfall_max);
	bindCassandraFloat(statement, param++, values.rainrate_max);
	bindCassandraFloat(statement, param++, values.solarRad_avg);
	bindCassandraFloat(statement, param++, values.solarRad_max);
	bindCassandraInt(statement, param++, values.uv_max);
	bindCassandraList(statement, param++, values.winddir);
	bindCassandraFloat(statement, param++, values.wind_avg);
	bindCassandraFloat(statement, param++, values.windgust_max);
	bindCassandraInt(statement, param++, values.insolationTime);
	bindCassandraInt(statement, param++, values.insolationTime_max);
	bindCassandraFloat(statement, param++, values.diff_outsideTemp_avg);
	bindCassandraFloat(statement, param++, values.diff_outsideTemp_min_min);
	bindCassandraFloat(statement, param++, values.diff_outsideTemp_max_max);
	bindCassandraFloat(statement, param++, values.diff_rainfall);
	bindCassandraFloat(statement, param++, values.diff_insolationTime);
	query = cass_session_execute(_session.get(), statement);
	cass_statement_free(statement);

	const CassResult* result = cass_future_get_result(query);
	bool ret = true;
	if (!result) {
		const char* error_message;
		size_t error_message_length;
		cass_future_error_message(query, &error_message, &error_message_length);
		ret = false;
		cass_result_free(result);
	}
	cass_future_free(query);

	return ret;
}
}
