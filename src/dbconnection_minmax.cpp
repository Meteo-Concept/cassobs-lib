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
#include <vector>
#include <string>

#include <cassandra.h>
#include <syslog.h>
#include <unistd.h>

#include "dbconnection_minmax.h"
#include "dbconnection_common.h"
#include "cassandra_stmt_ptr.h"

using namespace date;

namespace meteodata {

constexpr char DbConnectionMinmax::INSERT_DATAPOINT_STMT[];
constexpr char DbConnectionMinmax::SELECT_VALUES_ALL_DAY_STMT[];
constexpr char DbConnectionMinmax::SELECT_VALUES_BEFORE_6H_STMT[];
constexpr char DbConnectionMinmax::SELECT_VALUES_AFTER_6H_STMT[];
constexpr char DbConnectionMinmax::SELECT_VALUES_BEFORE_18H_STMT[];
constexpr char DbConnectionMinmax::SELECT_VALUES_AFTER_18H_STMT[];
constexpr char DbConnectionMinmax::SELECT_YEARLY_VALUES_STMT[];

namespace chrono = std::chrono;

DbConnectionMinmax::DbConnectionMinmax(const std::string& address, const std::string& user, const std::string& password) :
	DbConnectionCommon(address, user, password)
{
	prepareStatements();
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
}

bool DbConnectionMinmax::getValues6hTo6h(const CassUuid& uuid, const date::sys_days& date, DbConnectionMinmax::Values& values)
{
	CassFuture* query;
	CassStatement* statement = cass_prepared_bind(_selectValuesAfter6h.get());
	std::cerr << "Statement prepared" << std::endl;
	cass_statement_bind_uuid(statement, 0, uuid);
	cass_statement_bind_uint32(statement, 1, from_sysdays_to_CassandraDate(date));
	auto morning = date + chrono::hours(6);
	cass_statement_bind_int64(statement, 2, from_systime_to_CassandraDateTime(morning));
	query = cass_session_execute(_session.get(), statement);
	std::cerr << "Executed statement" << std::endl;
	cass_statement_free(statement);

	std::pair<bool, float> insideTemp_max[2];
	std::pair<bool, float> leafTemp_max[2][2];
	std::pair<bool, float> outsideTemp_max[2];
	std::pair<bool, float> real_outsideTemp_max[2];
	std::pair<bool, float> soilTemp_max[2][4];
	std::pair<bool, float> extraTemp_max[2][3];
	std::pair<bool, float> rainfall[2];
	std::pair<bool, float> rainrate_max[2];

	const CassResult* result = cass_future_get_result(query);
	bool ret = false;
	if (result) {
		const CassRow* row = cass_result_first_row(result);
		if (row) {
			int param = 0;
			storeCassandraFloat(row, param++, insideTemp_max[0]);
			for (int i=0 ; i<2 ; i++)
				storeCassandraFloat(row, param++,  leafTemp_max[0][i]);
			storeCassandraFloat(row, param++, outsideTemp_max[0]);
			storeCassandraFloat(row, param++, real_outsideTemp_max[0]);
			for (int i=0 ; i<4 ; i++)
				storeCassandraFloat(row, param++, soilTemp_max[0][i]);
			for (int i=0 ; i<3 ; i++)
				storeCassandraFloat(row, param++, extraTemp_max[0][i]);
			storeCassandraFloat(row, param++, rainfall[0]);
			storeCassandraFloat(row, param++, rainrate_max[0]);
			ret = true;
		}
	}
	cass_result_free(result);
	cass_future_free(query);

	auto nextDay = date + date::days(1);
	if (nextDay > chrono::system_clock::now()) {
		values.insideTemp_max = insideTemp_max[0];
		for (int i=0 ; i<2 ; i++)
			values.leafTemp_max[i] = leafTemp_max[0][i];
		computeMax(values.outsideTemp_max, outsideTemp_max[0], real_outsideTemp_max[0]);
		for (int i=0 ; i<2 ; i++)
			values.soilTemp_max[i] = soilTemp_max[0][i];
		for (int i=0 ; i<2 ; i++)
			values.extraTemp_max[i] = extraTemp_max[0][i];
		values.rainfall = rainfall[0];
		values.rainrate_max = rainrate_max[0];

		// return here immediately since there are no values for tomorrow
		return ret;
	}

	statement = cass_prepared_bind(_selectValuesBefore6h.get());
	std::cerr << "Statement prepared" << std::endl;
	cass_statement_bind_uuid(statement, 0, uuid);
	cass_statement_bind_uint32(statement, 1, from_sysdays_to_CassandraDate(date + date::days(1)));
	auto nextMorning = date + date::days(1) + chrono::hours(6);
	cass_statement_bind_int64(statement, 2, from_systime_to_CassandraDateTime(nextMorning));
	query = cass_session_execute(_session.get(), statement);
	std::cerr << "Executed statement" << std::endl;
	cass_statement_free(statement);

	result = cass_future_get_result(query);
	ret = false;
	if (result) {
		const CassRow* row = cass_result_first_row(result);
		if (row) {
			int param = 0;
			storeCassandraFloat(row, param++, insideTemp_max[1]);
			for (int i=0 ; i<2 ; i++)
				storeCassandraFloat(row, param++,  leafTemp_max[1][i]);
			storeCassandraFloat(row, param++, outsideTemp_max[1]);
			storeCassandraFloat(row, param++, real_outsideTemp_max[1]);
			for (int i=0 ; i<4 ; i++)
				storeCassandraFloat(row, param++, soilTemp_max[1][i]);
			for (int i=0 ; i<3 ; i++)
				storeCassandraFloat(row, param++, extraTemp_max[1][i]);
			storeCassandraFloat(row, param++, rainfall[1]);
			storeCassandraFloat(row, param++, rainrate_max[1]);
			ret = true;
		}
	}

	cass_result_free(result);
	cass_future_free(query);

	computeMax(values.insideTemp_max, insideTemp_max[0], insideTemp_max[1]);
	for (int i=0 ; i<2 ; i++)
		computeMax(values.leafTemp_max[i], leafTemp_max[0][i], leafTemp_max[1][i]);
	computeMax(values.outsideTemp_max,
			computeMax(outsideTemp_max[0], real_outsideTemp_max[0]),
			computeMax(outsideTemp_max[1], real_outsideTemp_max[1])
		  );
	for (int i=0 ; i<2 ; i++)
		computeMax(values.soilTemp_max[i], soilTemp_max[0][i], soilTemp_max[1][i]);
	for (int i=0 ; i<2 ; i++)
		computeMax(values.extraTemp_max[i], extraTemp_max[0][i], extraTemp_max[1][i]);
	compute(values.rainfall, rainfall[0], rainfall[1], std::plus<float>());
	computeMax(values.rainrate_max, rainrate_max[0], rainrate_max[1]);

	return ret;
}

bool DbConnectionMinmax::getValues18hTo18h(const CassUuid& uuid, const date::sys_days& date, DbConnectionMinmax::Values& values)
{
	CassFuture* query;
	CassStatement* statement = cass_prepared_bind(_selectValuesAfter18h.get());
	std::cerr << "Statement prepared" << std::endl;
	cass_statement_bind_uuid(statement, 0, uuid);
	cass_statement_bind_uint32(statement, 1, from_sysdays_to_CassandraDate(date - date::days(1)));
	auto previousEvening = date - date::days(1) + chrono::hours(18);
	cass_statement_bind_int64(statement, 2, from_systime_to_CassandraDateTime(previousEvening));
	query = cass_session_execute(_session.get(), statement);
	std::cerr << "Executed statement" << std::endl;
	cass_statement_free(statement);

	std::pair<bool, float> insideTemp_min[2];
	std::pair<bool, float> leafTemp_min[2][2];
	std::pair<bool, float> outsideTemp_min[2];
	std::pair<bool, float> real_outsideTemp_min[2];
	std::pair<bool, float> soilTemp_min[2][4];
	std::pair<bool, float> extraTemp_min[2][3];

	const CassResult* result = cass_future_get_result(query);
	bool ret = false;
	if (result) {
		const CassRow* row = cass_result_first_row(result);
		if (row) {
			int param = 0;
			storeCassandraFloat(row, param++, insideTemp_min[0]);
			for (int i=0 ; i<2 ; i++)
				storeCassandraFloat(row, param++,  leafTemp_min[0][i]);
			storeCassandraFloat(row, param++, outsideTemp_min[0]);
			storeCassandraFloat(row, param++, real_outsideTemp_min[0]);
			for (int i=0 ; i<4 ; i++)
				storeCassandraFloat(row, param++, soilTemp_min[0][i]);
			for (int i=0 ; i<3 ; i++)
				storeCassandraFloat(row, param++, extraTemp_min[0][i]);
			ret = true;
		}
	}
	cass_result_free(result);
	cass_future_free(query);

	statement = cass_prepared_bind(_selectValuesBefore18h.get());
	std::cerr << "Statement prepared" << std::endl;
	cass_statement_bind_uuid(statement, 0, uuid);
	cass_statement_bind_uint32(statement, 1, from_sysdays_to_CassandraDate(date));
	auto evening = date + chrono::hours(18);
	cass_statement_bind_int64(statement, 2, from_systime_to_CassandraDateTime(evening));
	query = cass_session_execute(_session.get(), statement);
	std::cerr << "Executed statement" << std::endl;
	cass_statement_free(statement);

	result = cass_future_get_result(query);
	ret = false;
	if (result) {
		const CassRow* row = cass_result_first_row(result);
		if (row) {
			int param = 0;
			storeCassandraFloat(row, param++, insideTemp_min[1]);
			for (int i=0 ; i<2 ; i++)
				storeCassandraFloat(row, param++,  leafTemp_min[1][i]);
			storeCassandraFloat(row, param++, outsideTemp_min[1]);
			storeCassandraFloat(row, param++, real_outsideTemp_min[1]);
			for (int i=0 ; i<4 ; i++)
				storeCassandraFloat(row, param++, soilTemp_min[1][i]);
			for (int i=0 ; i<3 ; i++)
				storeCassandraFloat(row, param++, extraTemp_min[1][i]);
			ret = true;
		}
	}

	cass_result_free(result);
	cass_future_free(query);

	computeMin(values.insideTemp_min, insideTemp_min[0], insideTemp_min[1]);
	for (int i=0 ; i<2 ; i++)
		computeMin(values.leafTemp_min[i], leafTemp_min[0][i], leafTemp_min[1][i]);
	computeMin(values.outsideTemp_min,
			computeMin(outsideTemp_min[0], real_outsideTemp_min[0]),
			computeMin(outsideTemp_min[1], real_outsideTemp_min[1])
		);
	for (int i=0 ; i<2 ; i++)
		computeMin(values.soilTemp_min[i], soilTemp_min[0][i], soilTemp_min[1][i]);
	for (int i=0 ; i<2 ; i++)
		computeMin(values.extraTemp_min[i], extraTemp_min[0][i], extraTemp_min[1][i]);

	return ret;
}

bool DbConnectionMinmax::getValues0hTo0h(const CassUuid& uuid, const date::sys_days& date, DbConnectionMinmax::Values& values)
{
	CassFuture* query;
	CassStatement* statement = cass_prepared_bind(_selectValuesAllDay.get());
	std::cerr << "Statement prepared" << std::endl;
	cass_statement_bind_uuid(statement, 0, uuid);
//	cass_statement_bind_int64(statement, 1, date::sys_time<chrono::milliseconds>(date).time_since_epoch().count());
//	cass_statement_bind_int64(statement, 2, date::sys_time<chrono::milliseconds>(date + date::days(1)).time_since_epoch().count());
	cass_statement_bind_uint32(statement, 1, from_sysdays_to_CassandraDate(date));
//	bindCassandraInt(statement, 2, (date + date::days(1)).time_since_epoch().count());
//	date::sys_time<chrono::milliseconds> previousEvening = date - date::days(1) + chrono::hours(18);
//	date::sys_time<chrono::milliseconds> evening         = date + chrono::hours(18);
//	cass_statement_bind_int64(statement, 3, previousEvening.time_since_epoch().count());
//	cass_statement_bind_int64(statement, 4, evening.time_since_epoch().count());
	query = cass_session_execute(_session.get(), statement);
	std::cerr << "Executed statement" << std::endl;
	cass_statement_free(statement);

	const CassResult* result = cass_future_get_result(query);
	bool ret = false;
	if (result) {
		const CassRow* row = cass_result_first_row(result);
		if (row) {
			int res = 0;
			storeCassandraFloat(row, res++, values.barometer_min);
			storeCassandraFloat(row, res++, values.barometer_max);
			storeCassandraFloat(row, res++, values.barometer_avg);
			for (int i=0 ; i<2 ; i++) {
				storeCassandraInt(row, res++, values.leafWetnesses_min[i]);
				storeCassandraInt(row, res++, values.leafWetnesses_max[i]);
				storeCassandraInt(row, res++, values.leafWetnesses_avg[i]);
			}
			for (int i=0 ; i<4 ; i++) {
				storeCassandraInt(row, res++, values.soilMoistures_min[i]);
				storeCassandraInt(row, res++, values.soilMoistures_max[i]);
				storeCassandraInt(row, res++, values.soilMoistures_avg[i]);
			}
			storeCassandraInt(row, res++, values.insideHum_min);
			storeCassandraInt(row, res++, values.insideHum_max);
			storeCassandraInt(row, res++, values.insideHum_avg);
			storeCassandraInt(row, res++, values.outsideHum_min);
			storeCassandraInt(row, res++, values.outsideHum_max);
			storeCassandraInt(row, res++, values.outsideHum_avg);
			for (int i=0 ; i<2 ; i++) {
				storeCassandraInt(row, res++, values.extraHum_min[i]);
				storeCassandraInt(row, res++, values.extraHum_max[i]);
				storeCassandraInt(row, res++, values.extraHum_avg[i]);
			}
			storeCassandraInt(row, res++, values.solarRad_max);
			storeCassandraInt(row, res++, values.solarRad_avg);
			storeCassandraInt(row, res++, values.uv_max);
			storeCassandraInt(row, res++, values.uv_avg);
			storeCassandraFloat(row, res++, values.windgust_max);
			storeCassandraFloat(row, res++, values.windgust_avg);
			storeCassandraFloat(row, res++, values.windspeed_max);
			storeCassandraFloat(row, res++, values.windspeed_avg);
			storeCassandraFloat(row, res++, values.dewpoint_min);
			storeCassandraFloat(row, res++, values.dewpoint_max);
			storeCassandraFloat(row, res++, values.dewpoint_avg);
			storeCassandraFloat(row, res++, values.et);
			storeCassandraInt(row, res++, values.insolation_time);

			// Overwrite rainfall and insolation_time if cumulative values are available
			const CassValue* raw = cass_row_get_column(row, res++);
			if (!cass_value_is_null(raw)) {
				values.rainfall.first = true;
				cass_value_get_float(raw, &values.rainfall.second);
			}
			raw = cass_row_get_column(row, res++);
			if (!cass_value_is_null(raw)) {
				values.insolation_time.first = true;
				cass_value_get_int32(raw, &values.insolation_time.second);
			}

			// Overwrite Tx and Tn if true values are available
			// Check also that the new value is indeed greater (resp. lower)
			// than then the computed max (resp. min), in case the station
			// measures the min-max in a mignight-to-midnight window
			raw = cass_row_get_column(row, res++);
			if (!cass_value_is_null(raw)) {
				float newMax;
				cass_value_get_float(raw, &newMax);
				if (!values.outsideTemp_max.first || values.outsideTemp_max.second < newMax) {
					values.outsideTemp_max.first = true;
					values.outsideTemp_max.second = newMax;
				}
			}
			raw = cass_row_get_column(row, res++);
			if (!cass_value_is_null(raw)) {
				float newMin;
				cass_value_get_float(raw, &newMin);
				if (!values.outsideTemp_min.first || values.outsideTemp_min.second > newMin) {
					values.outsideTemp_min.first = true;
					values.outsideTemp_min.second = newMin;
				}
			}

			ret = true;
		}
	}
	cass_result_free(result);
	cass_future_free(query);

	return ret;
}

bool DbConnectionMinmax::getYearlyValues(const CassUuid& uuid, const date::sys_days& date, std::pair<bool, float>& rain, std::pair<bool, float>& et)
{
	CassFuture* query;
	CassStatement* statement = cass_prepared_bind(_selectYearlyValues.get());
	std::cerr << "Statement prepared" << std::endl;
	cass_statement_bind_uuid(statement, 0, uuid);
	year_month_day ymd{date};
	cass_statement_bind_int32(statement, 1, int(ymd.year()) * 100 + unsigned(ymd.month()));
	cass_statement_bind_uint32(statement, 2,from_sysdays_to_CassandraDate(date));
	query = cass_session_execute(_session.get(), statement);
	std::cerr << "Executed statement" << std::endl;
	cass_statement_free(statement);

	const CassResult* result = cass_future_get_result(query);
	bool ret = false;
	if (result) {
		const CassRow* row = cass_result_first_row(result);
		if (row) {
			storeCassandraFloat(row, 0, rain);
			storeCassandraFloat(row, 1, et);
			ret = true;
		}
	}
	cass_result_free(result);
	cass_future_free(query);

	return ret;
}

bool DbConnectionMinmax::insertDataPoint(const CassUuid& station, const date::sys_days& date, const Values& values)
{
	CassFuture* query;
	std::cerr << "About to insert data point in database" << std::endl;
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
		std::cerr << "Error from Cassandra: " << error_message << std::endl;
		ret = false;
		cass_result_free(result);
	}
	cass_future_free(query);

	return ret;
}
}
