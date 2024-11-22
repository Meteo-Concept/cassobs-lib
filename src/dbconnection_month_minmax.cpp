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

#include <date/date.h>
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
constexpr char DbConnectionMonthMinmax::SELECT_DAILY_VALUES_POSTGRESQL[];
constexpr char DbConnectionMonthMinmax::SELECT_DAILY_VALUES_POSTGRESQL_STMT[];
constexpr char DbConnectionMonthMinmax::UPSERT_DATAPOINT_POSTGRESQL[];
constexpr char DbConnectionMonthMinmax::UPSERT_DATAPOINT_POSTGRESQL_STMT[];

namespace chrono = std::chrono;

DbConnectionMonthMinmax::DbConnectionMonthMinmax(
		const std::string& address, const std::string& user, const std::string& password,
		const std::string& pgAddress, const std::string& pgUser, const std::string& pgPassword
	):
	DbConnectionCommon(address, user, password),
	_pqConnection{"host=" + pgAddress + " user=" + pgUser + " password=" + pgPassword + " dbname=meteodata"}
{
	DbConnectionMonthMinmax::prepareStatements();
}

void DbConnectionMonthMinmax::prepareStatements()
{
	prepareOneStatement(_selectDailyValues, SELECT_DAILY_VALUES_STMT);
	prepareOneStatement(_insertDataPoint, INSERT_DATAPOINT_STMT);
	_pqConnection.prepare(SELECT_DAILY_VALUES_POSTGRESQL, SELECT_DAILY_VALUES_POSTGRESQL_STMT);
	_pqConnection.prepare(UPSERT_DATAPOINT_POSTGRESQL, UPSERT_DATAPOINT_POSTGRESQL_STMT);
}

bool DbConnectionMonthMinmax::getDailyValues(const CassUuid& uuid, int year, int month, DbConnectionMonthMinmax::Values& values)
{
	std::lock_guard locked{_pqTransactionMutex};
	pqxx::work tx{_pqConnection};

	try {
		char u[CASS_UUID_STRING_LENGTH];
		cass_uuid_string(uuid, u);
		char date[11];
		sprintf(date, "%04d-%02d-01", year, month);
		pqxx::row r = tx.exec_prepared1(SELECT_DAILY_VALUES_POSTGRESQL,
			u,
			date
		);

		values.outsideTemp_avg     = { !r[ 0].is_null(), r[ 0].as<float>(0.f) };
		values.outsideTemp_max_max = { !r[ 1].is_null(), r[ 1].as<float>(0.f) };
		values.outsideTemp_max_min = { !r[ 2].is_null(), r[ 2].as<float>(0.f) };
		values.outsideTemp_min_max = { !r[ 3].is_null(), r[ 3].as<float>(0.f) };
		values.outsideTemp_min_min = { !r[ 4].is_null(), r[ 4].as<float>(0.f) };
		values.wind_avg            = { !r[ 5].is_null(), r[ 5].as<float>(0.f) };
		values.windgust_max        = { !r[ 6].is_null(), r[ 6].as<float>(0.f) };
		values.rainfall            = { !r[ 7].is_null(), r[ 7].as<float>(0.f) };
		values.rainfall_max        = { !r[ 8].is_null(), r[ 8].as<float>(0.f) };
		values.rainrate_max        = { !r[ 9].is_null(), r[ 9].as<float>(0.f) };
		values.etp                 = { !r[10].is_null(), r[10].as<float>(0.f) };
		values.barometer_min       = { !r[11].is_null(), r[11].as<float>(0.f) };
		values.barometer_avg       = { !r[12].is_null(), r[12].as<float>(0.f) };
		values.barometer_max       = { !r[13].is_null(), r[13].as<float>(0.f) };
		values.outsideHum_min      = { !r[14].is_null(), r[14].as<float>(0.f) };
		values.outsideHum_max      = { !r[15].is_null(), r[15].as<float>(0.f) };
		values.solarRad_avg        = { !r[16].is_null(), r[16].as<float>(0.f) };
		values.solarRad_max        = { !r[17].is_null(), r[17].as<float>(0.f) };
		values.uv_max              = { !r[18].is_null(), r[18].as<float>(0.f) };
		values.insolationTime      = { !r[19].is_null(), r[19].as<float>(0.f) };
		values.insolationTime_max  = { !r[20].is_null(), r[20].as<float>(0.f) };

		tx.commit();
		return true;
	} catch (const pqxx::pqxx_exception& e) {
		std::cerr << e.base().what() << std::endl;
		return false;
	}
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

bool DbConnectionMonthMinmax::insertDataPointInTimescaleDB(const CassUuid& station, const date::year_month& yearmonth, const Values& values)
{
	std::lock_guard locked{_pqTransactionMutex};
	pqxx::work tx{_pqConnection};
	try {
		doInsertDataPointInTimescaleDB(station, yearmonth, values, tx);
		tx.commit();
	} catch (const pqxx::pqxx_exception& e) {
		std::cerr << e.base().what() << std::endl;
		return false;
	}
	return true;
}

void DbConnectionMonthMinmax::doInsertDataPointInTimescaleDB(const CassUuid& station, const date::year_month& yearmonth, const Values& values, pqxx::transaction_base& tx)
{
	char uuid[CASS_UUID_STRING_LENGTH];
	cass_uuid_string(station, uuid);
	tx.exec_prepared0(UPSERT_DATAPOINT_POSTGRESQL,
		uuid,
		date::format("%Y-%m-01", yearmonth),
		values.barometer_avg.first ? &values.barometer_avg.second : nullptr,
		values.barometer_min.first ? &values.barometer_min.second : nullptr,
		values.barometer_max.first ? &values.barometer_max.second : nullptr,
		values.etp.first ? &values.etp.second : nullptr,
		values.outsideHum_max.first ? &values.outsideHum_max.second : nullptr,
		values.outsideHum_min.first ? &values.outsideHum_min.second : nullptr,
		values.outsideTemp_avg.first ? &values.outsideTemp_avg.second : nullptr,
		values.outsideTemp_max_max.first ? &values.outsideTemp_max_max.second : nullptr,
		values.outsideTemp_max_min.first ? &values.outsideTemp_max_min.second : nullptr,
		values.outsideTemp_min_max.first ? &values.outsideTemp_min_max.second : nullptr,
		values.outsideTemp_min_min.first ? &values.outsideTemp_min_min.second : nullptr,
		values.rainfall.first ? &values.rainfall.second : nullptr,
		values.rainfall_max.first ? &values.rainfall_max.second : nullptr,
		values.rainrate_max.first ? &values.rainrate_max.second : nullptr,
		values.solarRad_max.first ? &values.solarRad_max.second : nullptr,
		values.solarRad_avg.first ? &values.solarRad_avg.second : nullptr,
		values.uv_max.first ? &values.uv_max.second : nullptr,
		values.winddir.first ? &values.winddir.second : nullptr,
		values.wind_avg.first ? &values.wind_avg.second : nullptr,
		values.windgust_max.first ? &values.windgust_max.second : nullptr,
		values.insolationTime.first ? &values.insolationTime.second : nullptr,
		values.insolationTime_max.first ? &values.insolationTime_max.second : nullptr,
		values.diff_outsideTemp_avg.first ? &values.diff_outsideTemp_avg.second : nullptr,
		values.diff_outsideTemp_min_min.first ? &values.diff_outsideTemp_min_min.second : nullptr,
		values.diff_outsideTemp_max_max.first ? &values.diff_outsideTemp_max_max.second : nullptr,
		values.diff_rainfall.first ? &values.diff_rainfall.second : nullptr,
		values.diff_insolationTime.first ? &values.diff_insolationTime.second : nullptr
	);
}

}
