/**
 * @file dbconnection_minmax.cpp
 * @brief Implementation of the DbConnectionMinmax class
 * @author Laurent Georget
 * @date 2017-10-25
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

#include <iostream>
#include <mutex>
#include <exception>
#include <chrono>
#include <pqxx/except.hxx>
#include <vector>
#include <string>

#include <cassandra.h>
#include <syslog.h>
#include <unistd.h>
#include <pqxx/pqxx>

#include "dbconnection_minmax.h"
#include "dbconnection_common.h"
#include "cassandra_stmt_ptr.h"

using namespace date;

namespace meteodata {

constexpr char DbConnectionMinmax::INSERT_DATAPOINT_STMT[];
constexpr char DbConnectionMinmax::UPSERT_DATAPOINT_POSTGRESQL[];
constexpr char DbConnectionMinmax::UPSERT_DATAPOINT_POSTGRESQL_STMT[];
constexpr char DbConnectionMinmax::SELECT_VALUES_ALL_DAY_STMT[];
constexpr char DbConnectionMinmax::SELECT_VALUES_ALL_DAY_POSTGRESQL[];
constexpr char DbConnectionMinmax::SELECT_VALUES_ALL_DAY_POSTGRESQL_STMT[];
constexpr char DbConnectionMinmax::SELECT_VALUES_BEFORE_6H_STMT[];
constexpr char DbConnectionMinmax::SELECT_VALUES_AFTER_6H_STMT[];
constexpr char DbConnectionMinmax::SELECT_VALUES_FROM_6H_TO_6H_POSTGRESQL[];
constexpr char DbConnectionMinmax::SELECT_VALUES_FROM_6H_TO_6H_POSTGRESQL_STMT[];
constexpr char DbConnectionMinmax::SELECT_VALUES_BEFORE_18H_STMT[];
constexpr char DbConnectionMinmax::SELECT_VALUES_AFTER_18H_STMT[];
constexpr char DbConnectionMinmax::SELECT_VALUES_FROM_18H_TO_18H_POSTGRESQL[];
constexpr char DbConnectionMinmax::SELECT_VALUES_FROM_18H_TO_18H_POSTGRESQL_STMT[];
constexpr char DbConnectionMinmax::SELECT_YEARLY_VALUES_STMT[];
constexpr char DbConnectionMinmax::SELECT_YEARLY_VALUES_POSTGRESQL[];
constexpr char DbConnectionMinmax::SELECT_YEARLY_VALUES_POSTGRESQL_STMT[];

namespace chrono = std::chrono;


DbConnectionMinmax::DbConnectionMinmax(
		const std::string& address, const std::string& user, const std::string& password,
		const std::string& pgAddress, const std::string& pgUser, const std::string& pgPassword) :
	DbConnectionCommon(address, user, password),
	_pqConnection{"host=" + pgAddress + " user=" + pgUser + " password=" + pgPassword + " dbname=meteodata"}
{
	DbConnectionMinmax::prepareStatements();
}

void DbConnectionMinmax::prepareStatements()
{
	prepareOneStatement(_selectValuesBefore6h, SELECT_VALUES_BEFORE_6H_STMT);
	prepareOneStatement(_selectValuesAfter6h, SELECT_VALUES_AFTER_6H_STMT);
	prepareOneStatement(_selectValuesAllDay, SELECT_VALUES_ALL_DAY_STMT);
	prepareOneStatement(_selectValuesBefore18h, SELECT_VALUES_BEFORE_18H_STMT);
	prepareOneStatement(_selectValuesAfter18h, SELECT_VALUES_AFTER_18H_STMT);
	prepareOneStatement(_selectYearlyValues, SELECT_YEARLY_VALUES_STMT);
	prepareOneStatement(_insertDataPoint, INSERT_DATAPOINT_STMT);
	_pqConnection.prepare(UPSERT_DATAPOINT_POSTGRESQL, UPSERT_DATAPOINT_POSTGRESQL_STMT);
	_pqConnection.prepare(SELECT_YEARLY_VALUES_POSTGRESQL, SELECT_YEARLY_VALUES_POSTGRESQL_STMT);
	_pqConnection.prepare(SELECT_VALUES_FROM_6H_TO_6H_POSTGRESQL, SELECT_VALUES_FROM_6H_TO_6H_POSTGRESQL_STMT);
	_pqConnection.prepare(SELECT_VALUES_FROM_18H_TO_18H_POSTGRESQL, SELECT_VALUES_FROM_18H_TO_18H_POSTGRESQL_STMT);
	_pqConnection.prepare(SELECT_VALUES_ALL_DAY_POSTGRESQL, SELECT_VALUES_ALL_DAY_POSTGRESQL_STMT);
}

bool DbConnectionMinmax::getValues6hTo6h(const CassUuid& uuid, const date::sys_days& date, DbConnectionMinmax::Values& values)
{
	std::lock_guard locked{_pqTransactionMutex};
	pqxx::work tx{_pqConnection};

	try {
		char u[CASS_UUID_STRING_LENGTH];
		cass_uuid_string(uuid, u);
		pqxx::row r = tx.exec_prepared1(SELECT_VALUES_FROM_6H_TO_6H_POSTGRESQL,
			u,
			date::format("%F %T%z", date)
		);

		values.insideTemp_max   = { !r[0].is_null(), r[0].as<float>(0.f) };
		values.leafTemp_max[0]  = { !r[1].is_null(), r[1].as<float>(0.f) };
		values.leafTemp_max[1]  = { !r[2].is_null(), r[2].as<float>(0.f) };
		values.outsideTemp_max  = { !r[3].is_null(), r[3].as<float>(0.f) };
		values.soilTemp_max[0]  = { !r[4].is_null(), r[4].as<float>(0.f) };
		values.soilTemp_max[1]  = { !r[5].is_null(), r[5].as<float>(0.f) };
		values.soilTemp_max[2]  = { !r[6].is_null(), r[6].as<float>(0.f) };
		values.soilTemp_max[3]  = { !r[7].is_null(), r[7].as<float>(0.f) };
		values.extraTemp_max[0] = { !r[8].is_null(), r[8].as<float>(0.f) };
		values.extraTemp_max[1] = { !r[9].is_null(), r[9].as<float>(0.f) };
		values.extraTemp_max[2] = { !r[10].is_null(), r[10].as<float>(0.f) };
		values.rainfall         = { !r[11].is_null(), r[11].as<float>(0.f) };
		values.rainrate_max     = { !r[12].is_null(), r[12].as<float>(0.f) };

		tx.commit();
		return true;
	} catch (const pqxx::pqxx_exception& e) {
		std::cerr << e.base().what() << std::endl;
		return false;
	}
}

bool DbConnectionMinmax::getValues18hTo18h(const CassUuid& uuid, const date::sys_days& date, DbConnectionMinmax::Values& values)
{
	std::lock_guard locked{_pqTransactionMutex};
	pqxx::work tx{_pqConnection};

	try {
		char u[CASS_UUID_STRING_LENGTH];
		cass_uuid_string(uuid, u);
		pqxx::row r = tx.exec_prepared1(SELECT_VALUES_FROM_18H_TO_18H_POSTGRESQL,
			u,
			date::format("%F %T%z", date)
		);

		values.insideTemp_min   = { !r[0].is_null(), r[0].as<float>(0.f) };
		values.leafTemp_min[0]  = { !r[1].is_null(), r[1].as<float>(0.f) };
		values.leafTemp_min[1]  = { !r[2].is_null(), r[2].as<float>(0.f) };
		values.outsideTemp_min  = { !r[3].is_null(), r[3].as<float>(0.f) };
		values.soilTemp_min[0]  = { !r[4].is_null(), r[4].as<float>(0.f) };
		values.soilTemp_min[1]  = { !r[5].is_null(), r[5].as<float>(0.f) };
		values.soilTemp_min[2]  = { !r[6].is_null(), r[6].as<float>(0.f) };
		values.soilTemp_min[3]  = { !r[7].is_null(), r[7].as<float>(0.f) };
		values.extraTemp_min[0] = { !r[8].is_null(), r[8].as<float>(0.f) };
		values.extraTemp_min[1] = { !r[9].is_null(), r[9].as<float>(0.f) };
		values.extraTemp_min[2] = { !r[10].is_null(), r[10].as<float>(0.f) };

		tx.commit();
		return true;
	} catch (const pqxx::pqxx_exception& e) {
		std::cerr << e.base().what() << std::endl;
		return false;
	}
}

bool DbConnectionMinmax::getValues0hTo0h(const CassUuid& uuid, const date::sys_days& date, DbConnectionMinmax::Values& values)
{
	std::lock_guard locked{_pqTransactionMutex};
	pqxx::work tx{_pqConnection};

	try {
		char u[CASS_UUID_STRING_LENGTH];
		cass_uuid_string(uuid, u);
		pqxx::row r = tx.exec_prepared1(SELECT_VALUES_ALL_DAY_POSTGRESQL,
			u,
			date::format("%F %T%z", date)
		);

		values.barometer_min        = { !r[ 0].is_null(), r[ 0].as<float>(0.f) };
		values.barometer_max        = { !r[ 1].is_null(), r[ 1].as<float>(0.f) };
		values.barometer_avg        = { !r[ 2].is_null(), r[ 2].as<float>(0.f) };
		values.leafWetnesses_min[0] = { !r[ 3].is_null(), r[ 3].as<float>(0.f) };
		values.leafWetnesses_max[0] = { !r[ 4].is_null(), r[ 4].as<float>(0.f) };
		values.leafWetnesses_avg[0] = { !r[ 5].is_null(), r[ 5].as<float>(0.f) };
		values.leafWetnesses_min[1] = { !r[ 6].is_null(), r[ 6].as<float>(0.f) };
		values.leafWetnesses_max[1] = { !r[ 7].is_null(), r[ 7].as<float>(0.f) };
		values.leafWetnesses_avg[1] = { !r[ 8].is_null(), r[ 8].as<float>(0.f) };
		values.soilMoistures_min[0] = { !r[ 9].is_null(), r[ 9].as<float>(0.f) };
		values.soilMoistures_max[0] = { !r[10].is_null(), r[10].as<float>(0.f) };
		values.soilMoistures_avg[0] = { !r[11].is_null(), r[11].as<float>(0.f) };
		values.soilMoistures_min[1] = { !r[12].is_null(), r[12].as<float>(0.f) };
		values.soilMoistures_max[1] = { !r[13].is_null(), r[13].as<float>(0.f) };
		values.soilMoistures_avg[1] = { !r[14].is_null(), r[14].as<float>(0.f) };
		values.soilMoistures_min[2] = { !r[15].is_null(), r[15].as<float>(0.f) };
		values.soilMoistures_max[2] = { !r[16].is_null(), r[16].as<float>(0.f) };
		values.soilMoistures_avg[2] = { !r[17].is_null(), r[17].as<float>(0.f) };
		values.soilMoistures_min[3] = { !r[18].is_null(), r[18].as<float>(0.f) };
		values.soilMoistures_max[3] = { !r[19].is_null(), r[19].as<float>(0.f) };
		values.soilMoistures_avg[3] = { !r[20].is_null(), r[20].as<float>(0.f) };
		values.insideHum_min        = { !r[21].is_null(), r[21].as<float>(0.f) };
		values.insideHum_max        = { !r[22].is_null(), r[22].as<float>(0.f) };
		values.insideHum_avg        = { !r[23].is_null(), r[23].as<float>(0.f) };
		values.outsideHum_min       = { !r[24].is_null(), r[24].as<float>(0.f) };
		values.outsideHum_max       = { !r[25].is_null(), r[25].as<float>(0.f) };
		values.outsideHum_avg       = { !r[26].is_null(), r[26].as<float>(0.f) };
		values.extraHum_min[0]      = { !r[27].is_null(), r[27].as<float>(0.f) };
		values.extraHum_max[0]      = { !r[28].is_null(), r[28].as<float>(0.f) };
		values.extraHum_avg[0]      = { !r[29].is_null(), r[29].as<float>(0.f) };
		values.extraHum_min[1]      = { !r[30].is_null(), r[30].as<float>(0.f) };
		values.extraHum_max[1]      = { !r[31].is_null(), r[31].as<float>(0.f) };
		values.extraHum_avg[1]      = { !r[32].is_null(), r[32].as<float>(0.f) };
		values.solarRad_max         = { !r[33].is_null(), r[33].as<float>(0.f) };
		values.solarRad_avg         = { !r[34].is_null(), r[34].as<float>(0.f) };
		values.uv_max               = { !r[35].is_null(), r[35].as<float>(0.f) };
		values.uv_avg               = { !r[36].is_null(), r[36].as<float>(0.f) };
		values.windgust_max         = { !r[37].is_null(), r[37].as<float>(0.f) };
		values.windgust_avg         = { !r[38].is_null(), r[38].as<float>(0.f) };
		values.windspeed_max        = { !r[39].is_null(), r[39].as<float>(0.f) };
		values.windspeed_avg        = { !r[40].is_null(), r[40].as<float>(0.f) };
		values.dewpoint_min         = { !r[41].is_null(), r[41].as<float>(0.f) };
		values.dewpoint_max         = { !r[42].is_null(), r[42].as<float>(0.f) };
		values.dewpoint_avg         = { !r[43].is_null(), r[43].as<float>(0.f) };
		values.et                   = { !r[44].is_null(), r[44].as<float>(0.f) };
		values.insolation_time      = { !r[45].is_null(), r[45].as<float>(0.f) };
		if (!r[46].is_null() && (!values.rainfall.first || values.rainfall.second < r[46].as<float>(0.0f))) {
			values.rainfall  = { true, r[46].as<float>(0.f) };
		}
		if (!r[47].is_null() && (!values.insolation_time.first || values.insolation_time.second < r[47].as<float>(0.0f))) {
			values.insolation_time  = { true, r[47].as<float>(0.f) };
		}
		if (!r[48].is_null() && (!values.outsideTemp_max.first || values.outsideTemp_max.second < r[48].as<float>(0.0f))) {
			values.outsideTemp_max  = { true, r[48].as<float>(0.f) };
		}
		if (!r[49].is_null() && (!values.outsideTemp_min.first || values.outsideTemp_min.second < r[49].as<float>(0.0f))) {
			values.outsideTemp_min  = { true, r[49].as<float>(0.f) };
		}

		tx.commit();
		return true;
	} catch (const pqxx::pqxx_exception& e) {
		std::cerr << e.base().what() << std::endl;
		return false;
	}
}

bool DbConnectionMinmax::getYearlyValues(const CassUuid& uuid, const date::sys_days& date, std::pair<bool, float>& rain, std::pair<bool, float>& et)
{
	std::lock_guard locked{_pqTransactionMutex};
	pqxx::work tx{_pqConnection};

	try {
		char u[CASS_UUID_STRING_LENGTH];
		cass_uuid_string(uuid, u);
		pqxx::row r = tx.exec_prepared1(SELECT_YEARLY_VALUES_POSTGRESQL,
			u,
			date::format("%F", date)
		);

		rain = { !r[0].is_null(), r[0].as<float>(0.f) };
		et   = { !r[1].is_null(), r[1].as<float>(0.f) };

		tx.commit();
		return true;
	} catch (const pqxx::pqxx_exception& e) {
		std::cerr << e.base().what() << std::endl;
		return false;
	}
}

bool DbConnectionMinmax::insertDataPoint(const CassUuid& station, const date::sys_days& date, const Values& values)
{
	CassFuture* query;
	CassStatement* statement = cass_prepared_bind(_insertDataPoint.get());
	int param = 0;
	auto ymd = year_month_day{date};
	cass_statement_bind_uuid(statement,  param++, station);
	cass_statement_bind_int32(statement,  param++, int(ymd.year()) * 100 + unsigned(ymd.month()));
	cass_statement_bind_uint32(statement, param++, cass_date_from_epoch(date::sys_seconds(date).time_since_epoch().count()));
	bindCassandraFloat(statement, param++, values.barometer_min);
	bindCassandraFloat(statement, param++, values.barometer_max);
	bindCassandraFloat(statement, param++, values.barometer_avg);
	bindCassandraFloat(statement, param++, values.dayEt);
	bindCassandraFloat(statement, param++, values.monthEt);
	bindCassandraFloat(statement, param++, values.yearEt);
	bindCassandraFloat(statement, param++, values.dayRain);
	bindCassandraFloat(statement, param++, values.monthRain);
	bindCassandraFloat(statement, param++, values.yearRain);
	bindCassandraFloat(statement, param++, values.dewpoint_max);
	bindCassandraFloat(statement, param++, values.dewpoint_avg);
	bindCassandraInt(statement, param++, values.insideHum_min);
	bindCassandraInt(statement, param++, values.insideHum_max);
	bindCassandraInt(statement, param++, values.insideHum_avg);
	bindCassandraFloat(statement, param++, values.insideTemp_min);
	bindCassandraFloat(statement, param++, values.insideTemp_max);
	bindCassandraFloat(statement, param++, values.insideTemp_avg);
	for (int i=0 ; i<2 ; i++) {
		bindCassandraFloat(statement, param++, values.leafTemp_min[i]);
		bindCassandraFloat(statement, param++, values.leafTemp_max[i]);
		bindCassandraFloat(statement, param++, values.leafTemp_avg[i]);
	}
	for (int i=0 ; i<2 ; i++) {
		bindCassandraInt(statement, param++, values.leafWetnesses_min[i]);
		bindCassandraInt(statement, param++, values.leafWetnesses_max[i]);
		bindCassandraInt(statement, param++, values.leafWetnesses_avg[i]);
	}
	bindCassandraInt(statement, param++, values.outsideHum_min);
	bindCassandraInt(statement, param++, values.outsideHum_max);
	bindCassandraInt(statement, param++, values.outsideHum_avg);
	bindCassandraFloat(statement, param++, values.outsideTemp_min);
	bindCassandraFloat(statement, param++, values.outsideTemp_max);
	bindCassandraFloat(statement, param++, values.outsideTemp_avg);
	bindCassandraFloat(statement, param++, values.rainrate_max);
	for (int i=0 ; i<4 ; i++) {
		bindCassandraInt(statement, param++, values.soilMoistures_min[i]);
		bindCassandraInt(statement, param++, values.soilMoistures_max[i]);
		bindCassandraInt(statement, param++, values.soilMoistures_avg[i]);
	}
	for (int i=0 ; i<4 ; i++) {
		bindCassandraFloat(statement, param++, values.soilTemp_min[i]);
		bindCassandraFloat(statement, param++, values.soilTemp_max[i]);
		bindCassandraFloat(statement, param++, values.soilTemp_avg[i]);
	}
	for (int i=0 ; i<3 ; i++) {
		bindCassandraFloat(statement, param++, values.extraTemp_min[i]);
		bindCassandraFloat(statement, param++, values.extraTemp_max[i]);
		bindCassandraFloat(statement, param++, values.extraTemp_avg[i]);
	}
	for (int i=0 ; i<2 ; i++) {
		bindCassandraInt(statement, param++, values.extraHum_min[i]);
		bindCassandraInt(statement, param++, values.extraHum_max[i]);
		bindCassandraInt(statement, param++, values.extraHum_avg[i]);
	}
	bindCassandraInt(statement, param++, values.solarRad_max);
	bindCassandraInt(statement, param++, values.solarRad_avg);
	bindCassandraInt(statement, param++, values.uv_max);
	bindCassandraInt(statement, param++, values.uv_avg);
	bindCassandraList(statement, param++, values.winddir);
	bindCassandraFloat(statement, param++, values.windgust_max);
	bindCassandraFloat(statement, param++, values.windgust_avg);
	bindCassandraFloat(statement, param++, values.windspeed_max);
	bindCassandraFloat(statement, param++, values.windspeed_avg);
	bindCassandraInt(statement, param++, values.insolation_time);
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

bool DbConnectionMinmax::insertDataPointInTimescaleDB(const CassUuid& station, const date::sys_days& date, const Values& values)
{
	std::lock_guard locked{_pqTransactionMutex};
	pqxx::work tx{_pqConnection};
	try {
		doInsertDataPointInTimescaleDB(station, date, values, tx);
		tx.commit();
	} catch (const pqxx::pqxx_exception& e) {
		std::cerr << e.base().what() << std::endl;
		return false;
	}
	return true;
}

void DbConnectionMinmax::doInsertDataPointInTimescaleDB(const CassUuid& station, const date::sys_days& date, const Values& values, pqxx::transaction_base& tx)
{
	char uuid[CASS_UUID_STRING_LENGTH];
	cass_uuid_string(station, uuid);
	tx.exec_prepared0(UPSERT_DATAPOINT_POSTGRESQL,
		uuid,
		date::format("%F", date),
		values.barometer_min.first ? &values.barometer_min.second : nullptr,
		values.barometer_max.first ? &values.barometer_max.second : nullptr,
		values.barometer_avg.first ? &values.barometer_avg.second : nullptr,
		values.dayEt.first ? &values.dayEt.second : nullptr,
		values.monthEt.first ? &values.monthEt.second : nullptr,
		values.yearEt.first ? &values.yearEt.second : nullptr,
		values.dayRain.first ? &values.dayRain.second : nullptr,
		values.monthRain.first ? &values.monthRain.second : nullptr,
		values.yearRain.first ? &values.yearRain.second : nullptr,
		values.dewpoint_min.first ? &values.dewpoint_max.second : nullptr,
		values.dewpoint_avg.first ? &values.dewpoint_avg.second : nullptr,
		values.insideHum_min.first ? &values.insideHum_min.second : nullptr,
		values.insideHum_max.first ? &values.insideHum_max.second : nullptr,
		values.insideHum_avg.first ? &values.insideHum_avg.second : nullptr,
		values.insideTemp_min.first ? &values.insideTemp_min.second : nullptr,
		values.insideTemp_max.first ? &values.insideTemp_max.second : nullptr,
		values.insideTemp_avg.first ? &values.insideTemp_avg.second : nullptr,
		values.leafTemp_min[0].first ? &values.leafTemp_min[0].second : nullptr,
		values.leafTemp_max[0].first ? &values.leafTemp_max[0].second : nullptr,
		values.leafTemp_avg[0].first ? &values.leafTemp_avg[0].second : nullptr,
		values.leafTemp_min[1].first ? &values.leafTemp_min[1].second : nullptr,
		values.leafTemp_max[1].first ? &values.leafTemp_max[1].second : nullptr,
		values.leafTemp_avg[1].first ? &values.leafTemp_avg[1].second : nullptr,
		values.leafWetnesses_min[0].first ? &values.leafWetnesses_min[0].second : nullptr,
		values.leafWetnesses_max[0].first ? &values.leafWetnesses_max[0].second : nullptr,
		values.leafWetnesses_avg[0].first ? &values.leafWetnesses_avg[0].second : nullptr,
		values.leafWetnesses_min[1].first ? &values.leafWetnesses_min[1].second : nullptr,
		values.leafWetnesses_max[1].first ? &values.leafWetnesses_max[1].second : nullptr,
		values.leafWetnesses_avg[1].first ? &values.leafWetnesses_avg[1].second : nullptr,
		values.outsideHum_min.first ? &values.outsideHum_min.second : nullptr,
		values.outsideHum_max.first ? &values.outsideHum_max.second : nullptr,
		values.outsideHum_avg.first ? &values.outsideHum_avg.second : nullptr,
		values.outsideTemp_min.first ? &values.outsideTemp_min.second : nullptr,
		values.outsideTemp_max.first ? &values.outsideTemp_max.second : nullptr,
		values.outsideTemp_avg.first ? &values.outsideTemp_avg.second : nullptr,
		values.rainrate_max.first ? &values.rainrate_max.second : nullptr,
		values.soilMoistures_min[0].first ? &values.soilMoistures_min[0].second : nullptr,
		values.soilMoistures_max[0].first ? &values.soilMoistures_max[0].second : nullptr,
		values.soilMoistures_avg[0].first ? &values.soilMoistures_avg[0].second : nullptr,
		values.soilMoistures_min[1].first ? &values.soilMoistures_min[1].second : nullptr,
		values.soilMoistures_max[1].first ? &values.soilMoistures_max[1].second : nullptr,
		values.soilMoistures_avg[1].first ? &values.soilMoistures_avg[1].second : nullptr,
		values.soilMoistures_min[2].first ? &values.soilMoistures_min[2].second : nullptr,
		values.soilMoistures_max[2].first ? &values.soilMoistures_max[2].second : nullptr,
		values.soilMoistures_avg[2].first ? &values.soilMoistures_avg[2].second : nullptr,
		values.soilMoistures_min[3].first ? &values.soilMoistures_min[3].second : nullptr,
		values.soilMoistures_max[3].first ? &values.soilMoistures_max[3].second : nullptr,
		values.soilMoistures_avg[3].first ? &values.soilMoistures_avg[3].second : nullptr,
		values.soilTemp_min[0].first ? &values.soilTemp_min[0].second : nullptr,
		values.soilTemp_max[0].first ? &values.soilTemp_max[0].second : nullptr,
		values.soilTemp_avg[0].first ? &values.soilTemp_avg[0].second : nullptr,
		values.soilTemp_min[1].first ? &values.soilTemp_min[1].second : nullptr,
		values.soilTemp_max[1].first ? &values.soilTemp_max[1].second : nullptr,
		values.soilTemp_avg[1].first ? &values.soilTemp_avg[1].second : nullptr,
		values.soilTemp_min[2].first ? &values.soilTemp_min[2].second : nullptr,
		values.soilTemp_max[2].first ? &values.soilTemp_max[2].second : nullptr,
		values.soilTemp_avg[2].first ? &values.soilTemp_avg[2].second : nullptr,
		values.soilTemp_min[3].first ? &values.soilTemp_min[3].second : nullptr,
		values.soilTemp_max[3].first ? &values.soilTemp_max[3].second : nullptr,
		values.soilTemp_avg[3].first ? &values.soilTemp_avg[3].second : nullptr,
		values.extraTemp_min[0].first ? &values.extraTemp_min[0].second : nullptr,
		values.extraTemp_max[0].first ? &values.extraTemp_max[0].second : nullptr,
		values.extraTemp_avg[0].first ? &values.extraTemp_avg[0].second : nullptr,
		values.extraTemp_min[1].first ? &values.extraTemp_min[1].second : nullptr,
		values.extraTemp_max[1].first ? &values.extraTemp_max[1].second : nullptr,
		values.extraTemp_avg[1].first ? &values.extraTemp_avg[1].second : nullptr,
		values.extraTemp_min[2].first ? &values.extraTemp_min[2].second : nullptr,
		values.extraTemp_max[2].first ? &values.extraTemp_max[2].second : nullptr,
		values.extraTemp_avg[2].first ? &values.extraTemp_avg[2].second : nullptr,
		values.extraHum_min[0].first ? &values.extraHum_min[0].second : nullptr,
		values.extraHum_max[0].first ? &values.extraHum_max[0].second : nullptr,
		values.extraHum_avg[0].first ? &values.extraHum_avg[0].second : nullptr,
		values.extraHum_min[1].first ? &values.extraHum_min[1].second : nullptr,
		values.extraHum_max[1].first ? &values.extraHum_max[1].second : nullptr,
		values.extraHum_avg[1].first ? &values.extraHum_avg[1].second : nullptr,
		values.solarRad_max.first ? &values.solarRad_max.second : nullptr,
		values.solarRad_avg.first ? &values.solarRad_avg.second : nullptr,
		values.uv_max.first ? &values.uv_max.second : nullptr,
		values.uv_avg.first ? &values.uv_avg.second : nullptr,
		values.winddir.first ? &values.winddir.second : nullptr,
		values.windgust_max.first ? &values.windgust_max.second : nullptr,
		values.windgust_avg.first ? &values.windgust_avg.second : nullptr,
		values.windspeed_max.first ? &values.windspeed_max.second : nullptr,
		values.windspeed_avg.first ? &values.windspeed_avg.second : nullptr,
		values.insolation_time.first ? &values.insolation_time.second : nullptr
	);
}

}
