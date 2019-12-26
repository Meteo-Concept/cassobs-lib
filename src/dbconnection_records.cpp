/**
 * @file dbconnection_records.cpp
 * @brief Implementation of the DbConnectionRecords class
 * @author Laurent Georget
 * @date 2019-12-19
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

#include <ctime>
#include <iostream>
#include <mutex>
#include <exception>
#include <chrono>
#include <vector>
#include <string>

#include <cassandra.h>
#include <syslog.h>
#include <unistd.h>
#include <date.h>

#include "dbconnection_records.h"
#include "dbconnection_common.h"
#include "cassandra_stmt_ptr.h"
#include "monthly_records.h"

using namespace date;

namespace meteodata {

using sys = std::chrono::system_clock;

constexpr char DbConnectionRecords::INSERT_DATAPOINT_STMT[];
constexpr char DbConnectionRecords::SELECT_VALUES_FOR_ALL_DAYS_IN_MONTH_STMT[];
constexpr char DbConnectionRecords::SELECT_CURRENT_RECORDS_STMT[];

namespace chrono = std::chrono;

DbConnectionRecords::DbConnectionRecords(const std::string& address, const std::string& user, const std::string& password) :
	DbConnectionCommon(address, user, password)
{
	prepareStatements();
}

void DbConnectionRecords::prepareStatements()
{
	prepareOneStatement(_selectValuesForAllDaysInMonth, SELECT_VALUES_FOR_ALL_DAYS_IN_MONTH_STMT);
	prepareOneStatement(_selectCurrentRecords, SELECT_CURRENT_RECORDS_STMT);
	prepareOneStatement(_insertDataPoint, INSERT_DATAPOINT_STMT);
}

void storeCassandraFloatAndListOfDays(const CassRow* row, int column, float& value, std::set<date::sys_days>& dates)
{
	const CassValue* raw = cass_row_get_column(row, column);
	if (!cass_value_is_null(raw)) {
		CassIterator* tupleIterator = cass_iterator_from_tuple(raw);
		cass_iterator_next(tupleIterator);
		const CassValue* cassValue = cass_iterator_get_value(tupleIterator);
		cass_value_get_float(cassValue, &value);
		cass_iterator_next(tupleIterator);
		cassValue = cass_iterator_get_value(tupleIterator);
		CassIterator* dateIterator = cass_iterator_from_collection(cassValue);
		while (cass_iterator_next(dateIterator)) {
			cass_uint32_t date;
			const CassValue* dateValue = cass_iterator_get_value(dateIterator);
			cass_value_get_uint32(dateValue, &date);
			std::time_t time = static_cast<time_t>(cass_date_time_to_epoch(date, 0));
			dates.insert(date::floor<days>(sys::from_time_t(time)));
		}
		cass_iterator_free(dateIterator);
		cass_iterator_free(tupleIterator);
	}
}

void storeCassandraFloatAndListOfYears(const CassRow* row, int column, float& value, std::set<int>& years)
{
	const CassValue* raw = cass_row_get_column(row, column);
	if (!cass_value_is_null(raw)) {
		CassIterator* tupleIterator = cass_iterator_from_tuple(raw);
		cass_iterator_next(tupleIterator);
		const CassValue* cassValue = cass_iterator_get_value(tupleIterator);
		cass_value_get_float(cassValue, &value);
		cass_iterator_next(tupleIterator);
		cassValue = cass_iterator_get_value(tupleIterator);
		CassIterator* dateIterator = cass_iterator_from_collection(cassValue);
		while (cass_iterator_next(dateIterator)) {
			cass_int32_t year;
			const CassValue* yearValue = cass_iterator_get_value(dateIterator);
			cass_value_get_int32(yearValue, &year);
			years.insert(static_cast<int>(year));
		}
		cass_iterator_free(dateIterator);
		cass_iterator_free(tupleIterator);
	}
}

void storeCassandraIntAndListOfYears(const CassRow* row, int column, int& value, std::set<int>& years)
{
	const CassValue* raw = cass_row_get_column(row, column);
	if (!cass_value_is_null(raw)) {
		CassIterator* tupleIterator = cass_iterator_from_tuple(raw);
		cass_iterator_next(tupleIterator);
		const CassValue* cassValue = cass_iterator_get_value(tupleIterator);
		cass_value_get_int32(cassValue, &value);
		cass_iterator_next(tupleIterator);
		cassValue = cass_iterator_get_value(tupleIterator);
		CassIterator* dateIterator = cass_iterator_from_collection(cassValue);
		while (cass_iterator_next(dateIterator)) {
			cass_int32_t year;
			const CassValue* yearValue = cass_iterator_get_value(dateIterator);
			cass_value_get_int32(yearValue, &year);
			years.insert(static_cast<int>(year));
		}
		cass_iterator_free(dateIterator);
		cass_iterator_free(tupleIterator);
	}
}

bool DbConnectionRecords::getCurrentRecords(const CassUuid& station, date::month month, MonthlyRecords& values)
{
	using namespace date;

	CassFuture* query;
	CassStatement* statement = cass_prepared_bind(_selectCurrentRecords.get());

	std::cerr << "Statement prepared" << std::endl;
	cass_statement_bind_uuid(statement, 0, station);
	unsigned int m = unsigned(month);
	cass_statement_bind_int32(statement, 1, m);
	query = cass_session_execute(_session.get(), statement);
	std::cerr << "Executed statement" << std::endl;
	cass_statement_free(statement);

	values.setMonth(month);
	const CassResult* result = cass_future_get_result(query);
	CassIterator* it = cass_iterator_from_result(result);
	while (cass_iterator_next(it)) {
		const CassRow* row = cass_iterator_get_row(it);
		float tempFloat;
		int tempInt;
		std::set<date::sys_days> tempDays;
		std::set<int> tempYears;

		int param = 0;
		storeCassandraFloatAndListOfDays(row, param++, tempFloat, tempDays);
		values.setRecord(MonthlyRecords::DayRecord::OUTSIDE_TEMP_MAX_MAX, tempFloat, std::move(tempDays));

		storeCassandraFloatAndListOfDays(row, param++, tempFloat, tempDays);
		values.setRecord(MonthlyRecords::DayRecord::OUTSIDE_TEMP_MIN_MIN, tempFloat, std::move(tempDays));

		storeCassandraFloatAndListOfDays(row, param++, tempFloat, tempDays);
		values.setRecord(MonthlyRecords::DayRecord::OUTSIDE_TEMP_MAX_MIN, tempFloat, std::move(tempDays));

		storeCassandraFloatAndListOfDays(row, param++, tempFloat, tempDays);
		values.setRecord(MonthlyRecords::DayRecord::OUTSIDE_TEMP_MIN_MAX, tempFloat, std::move(tempDays));

		storeCassandraFloatAndListOfDays(row, param++, tempFloat, tempDays);
		values.setRecord(MonthlyRecords::DayRecord::OUTSIDE_TEMP_AMPL_MAX, tempFloat, std::move(tempDays));

		storeCassandraIntAndListOfYears(row, param++, tempInt, tempYears);
		values.setRecord(MonthlyRecords::MonthRecord::OUTSIDE_TEMP_MAX_OVER_30, tempInt, std::move(tempYears));

		storeCassandraIntAndListOfYears(row, param++, tempInt, tempYears);
		values.setRecord(MonthlyRecords::MonthRecord::OUTSIDE_TEMP_MAX_OVER_25, tempInt, std::move(tempYears));

		storeCassandraIntAndListOfYears(row, param++, tempInt, tempYears);
		values.setRecord(MonthlyRecords::MonthRecord::OUTSIDE_TEMP_MAX_UNDER_0, tempInt, std::move(tempYears));

		storeCassandraIntAndListOfYears(row, param++, tempInt, tempYears);
		values.setRecord(MonthlyRecords::MonthRecord::OUTSIDE_TEMP_MIN_UNDER_0, tempInt, std::move(tempYears));

		storeCassandraIntAndListOfYears(row, param++, tempInt, tempYears);
		values.setRecord(MonthlyRecords::MonthRecord::OUTSIDE_TEMP_MIN_UNDER_MINUS_5, tempInt, std::move(tempYears));

		storeCassandraIntAndListOfYears(row, param++, tempInt, tempYears);
		values.setRecord(MonthlyRecords::MonthRecord::OUTSIDE_TEMP_MIN_UNDER_MINUS_10, tempInt, std::move(tempYears));

		storeCassandraIntAndListOfYears(row, param++, tempInt, tempYears);
		values.setRecord(MonthlyRecords::MonthRecord::OUTSIDE_TEMP_AVG_MAX, tempFloat, std::move(tempYears));

		storeCassandraFloatAndListOfYears(row, param++, tempFloat, tempYears);
		values.setRecord(MonthlyRecords::MonthRecord::OUTSIDE_TEMP_AVG_MIN, tempFloat, std::move(tempYears));

		storeCassandraFloatAndListOfYears(row, param++, tempFloat, tempYears);
		values.setRecord(MonthlyRecords::MonthRecord::OUTSIDE_TEMP_MAX_AVG_MAX, tempFloat, std::move(tempYears));

		storeCassandraFloatAndListOfYears(row, param++, tempFloat, tempYears);
		values.setRecord(MonthlyRecords::MonthRecord::OUTSIDE_TEMP_MAX_AVG_MIN, tempFloat, std::move(tempYears));

		storeCassandraFloatAndListOfYears(row, param++, tempFloat, tempYears);
		values.setRecord(MonthlyRecords::MonthRecord::OUTSIDE_TEMP_MIN_AVG_MAX, tempFloat, std::move(tempYears));

		storeCassandraFloatAndListOfYears(row, param++, tempFloat, tempYears);
		values.setRecord(MonthlyRecords::MonthRecord::OUTSIDE_TEMP_MIN_AVG_MIN, tempFloat, std::move(tempYears));

		storeCassandraFloatAndListOfDays(row, param++, tempFloat, tempDays);
		values.setRecord(MonthlyRecords::DayRecord::DAYRAIN_MAX, tempFloat, std::move(tempDays));

		storeCassandraFloatAndListOfYears(row, param++, tempFloat, tempYears);
		values.setRecord(MonthlyRecords::MonthRecord::MONTHRAIN_MAX, tempFloat, std::move(tempYears));

		storeCassandraFloatAndListOfYears(row, param++, tempFloat, tempYears);
		values.setRecord(MonthlyRecords::MonthRecord::MONTHRAIN_MIN, tempFloat, std::move(tempYears));

		storeCassandraIntAndListOfYears(row, param++, tempInt, tempYears);
		values.setRecord(MonthlyRecords::MonthRecord::DAYRAIN_OVER_1, tempInt, std::move(tempYears));

		storeCassandraIntAndListOfYears(row, param++, tempInt, tempYears);
		values.setRecord(MonthlyRecords::MonthRecord::DAYRAIN_OVER_5, tempInt, std::move(tempYears));

		storeCassandraIntAndListOfYears(row, param++, tempInt, tempYears);
		values.setRecord(MonthlyRecords::MonthRecord::DAYRAIN_OVER_10, tempInt, std::move(tempYears));

		storeCassandraFloatAndListOfYears(row, param++, tempFloat, tempYears);
		values.setRecord(MonthlyRecords::MonthRecord::MONTHINSOLATION_MAX, tempFloat, std::move(tempYears));

		storeCassandraFloatAndListOfYears(row, param++, tempFloat, tempYears);
		values.setRecord(MonthlyRecords::MonthRecord::MONTHINSOLATION_MIN, tempFloat, std::move(tempYears));

		storeCassandraIntAndListOfYears(row, param++, tempInt, tempYears);
		values.setRecord(MonthlyRecords::MonthRecord::DAYINSOLATION_OVER_1, tempInt, std::move(tempYears));

		storeCassandraIntAndListOfYears(row, param++, tempInt, tempYears);
		values.setRecord(MonthlyRecords::MonthRecord::DAYINSOLATION_OVER_5, tempInt, std::move(tempYears));

		storeCassandraIntAndListOfYears(row, param++, tempInt, tempYears);
		values.setRecord(MonthlyRecords::MonthRecord::DAYINSOLATION_AT_0, tempInt, std::move(tempYears));

		storeCassandraFloatAndListOfDays(row, param++, tempFloat, tempDays);
		values.setRecord(MonthlyRecords::DayRecord::GUST_MAX, tempFloat, std::move(tempDays));

		storeCassandraFloatAndListOfYears(row, param++, tempFloat, tempYears);
		values.setRecord(MonthlyRecords::MonthRecord::WINDSPEED_AVG_MAX, tempFloat, std::move(tempYears));

		storeCassandraFloatAndListOfYears(row, param++, tempFloat, tempYears);
		values.setRecord(MonthlyRecords::MonthRecord::WINDSPEED_AVG_MIN, tempFloat, std::move(tempYears));
	}
	cass_iterator_free(it);
	cass_result_free(result);
	cass_future_free(query);

	return true;
}

bool DbConnectionRecords::getValuesForAllDaysInMonth(const CassUuid& uuid, int year, int month, MonthlyRecords& values)
{
	CassFuture* query;
	CassStatement* statement = cass_prepared_bind(_selectValuesForAllDaysInMonth.get());

	std::cerr << "Statement prepared" << std::endl;
	cass_statement_bind_uuid(statement, 0, uuid);
	std::cerr << "Period of interest: " << (year * 100 + month) << std::endl;
	cass_statement_bind_int32(statement, 1, year * 100 + month);
	query = cass_session_execute(_session.get(), statement);
	std::cerr << "Executed statement" << std::endl;
	cass_statement_free(statement);

	const CassResult* result = cass_future_get_result(query);
	if (!result) {
		cass_future_free(query);
		return false;
	}

	CassIterator* it = cass_iterator_from_result(result);
	while (cass_iterator_next(it)) {
		struct MonthlyRecords::DayValues dayValues;

		const CassRow* row = cass_iterator_get_row(it);
		storeCassandraDate(row, 0,  dayValues.day);
		storeCassandraFloat(row, 1, dayValues.outsideTemp_max);
		storeCassandraFloat(row, 2, dayValues.outsideTemp_min);
		storeCassandraFloat(row, 3, dayValues.outsideTemp_avg);
		storeCassandraFloat(row, 4, dayValues.dayrain);
		storeCassandraFloat(row, 5, dayValues.windSpeed_avg);
		storeCassandraFloat(row, 6, dayValues.windGust_max);
		storeCassandraInt(row, 7, dayValues.insolationTime);

		using namespace date;
		values.addDayValues(std::move(dayValues));
	}
	cass_iterator_free(it);
	cass_result_free(result);
	cass_future_free(query);

	return true;
}

bool DbConnectionRecords::insertDataPoint(const CassUuid& station, MonthlyRecords& values)
{
	CassFuture* query;
	std::cerr << "About to insert data point in database" << std::endl;
	CassStatement* statement = cass_prepared_bind(_insertDataPoint.get());
	std::cerr << "statement: " << statement << std::endl;
	values.populateRecordInsertionQuery(statement, station);
	std::cerr << "statement populated" << std::endl;
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
	}
	cass_result_free(result);
	cass_future_free(query);

	return ret;
}
}
