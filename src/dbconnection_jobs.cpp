/**
 * @file dbconnection_jobs.cpp
 * @brief Implementation of the DbConnectionJobs class
 * @author Laurent Georget
 * @date 2020-07-17
 */
/*
 * Copyright (C) 2023  SAS Météo Concept <contact@meteo-concept.fr>
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
#include <cstring>
#include <functional>
#include <tuple>
#include <memory>
#include <vector>
#include <utility>
#include <map>

#include <mysql.h>
#include <date/date.h>

#include "dbconnection_jobs.h"

namespace meteodata {

DbConnectionJobs::DbConnectionJobs(const std::string& host, const std::string& user, const std::string& password, const std::string& database) :
	_db{mysql_init(nullptr), &mysql_close},
	_retrieveJobStmt{nullptr, &mysql_stmt_close},
	_publishJobStmt{nullptr, &mysql_stmt_close},
	_reserveJobStmt{nullptr, &mysql_stmt_close},
	_markJobAsFinishedStmt{nullptr, &mysql_stmt_close}
{
	if (!mysql_real_connect(_db.get(),
		host.empty() ? nullptr : host.c_str(),
		user.empty() ? nullptr : user.c_str(),
		password.empty() ? nullptr : password.c_str(),
		database.empty() ? nullptr : database.c_str(),
		0,
		"/var/run/mysqld/mysqld.sock",
		0)
	) {
		panic("Cannot connect to the database");
	}
	mysql_set_character_set(_db.get(), "utf8mb4");

	DbConnectionJobs::prepareStatements();
}

void DbConnectionJobs::panic(const std::string& msg)
{
	std::cerr << msg << ": " << mysql_error(_db.get()) << std::endl;
	throw std::runtime_error("DB Fatal error");
}

void DbConnectionJobs::panic(MYSQL_STMT* stmt, const std::string& msg)
{
	std::cerr << msg << ": " << mysql_stmt_error(stmt) << " (" << mysql_stmt_errno(stmt) << ")" << std::endl;
	throw std::runtime_error("DB Fatal error");
}

void DbConnectionJobs::prepareStatements()
{
	_retrieveJobStmt.reset(mysql_stmt_init(_db.get()));
	if (mysql_stmt_prepare(_retrieveJobStmt.get(), RETRIEVE_JOB, sizeof(RETRIEVE_JOB)))
		panic("Could not prepare statement \"retrieveJob\"");

	_publishJobStmt.reset(mysql_stmt_init(_db.get()));
	if (mysql_stmt_prepare(_publishJobStmt.get(), PUBLISH_JOB, sizeof(PUBLISH_JOB)))
		panic("Could not prepare statement \"publishJob\"");

	_reserveJobStmt.reset(mysql_stmt_init(_db.get()));
	if (mysql_stmt_prepare(_reserveJobStmt.get(), RESERVE_JOB, sizeof(RESERVE_JOB)))
		panic("Could not prepare statement \"reserveJob\"");

	_markJobAsFinishedStmt.reset(mysql_stmt_init(_db.get()));
	if (mysql_stmt_prepare(_markJobAsFinishedStmt.get(), MARK_JOB_AS_FINISHED, sizeof(MARK_JOB_AS_FINISHED)))
		panic("Could not prepare statement \"markJobAsFinishedJob\"");
}

std::optional<DbConnectionJobs::StationJob> DbConnectionJobs::retrieveStationJob(const char* job)
{
	mysql_autocommit(_db.get(), false);
	auto rollback = [this](int* p) {
		mysql_rollback(_db.get());
		mysql_autocommit(_db.get(), true);
		delete p;
	};

	// We use a unique ptr with a dummy value just to guarantee that the
	// deleter is eventually called (a kind of poor man's scope_fail which
	// is not available in the standard library yet).
	std::unique_ptr<int, decltype(rollback)> sentinel{new int{}, rollback};

	MYSQL_STMT* stmt = _retrieveJobStmt.get();

	MYSQL_BIND params[1];
	std::memset(params, 0, sizeof(MYSQL_BIND) * 1);
	params[0].buffer_type = MYSQL_TYPE_VAR_STRING;
	params[0].buffer = const_cast<char*>(job);
	params[0].buffer_length = std::strlen(job);

	if (mysql_stmt_bind_param(stmt, params))
		panic(stmt, "Failed to bind params in statement \"retrieveJob\"");
	if (mysql_stmt_execute(stmt))
		panic(stmt, "Failed to execute statement \"retrieveJob\"");

	constexpr int NB_COLS = 6;
	my_bool isNull[NB_COLS];
	std::memset(isNull, 0, NB_COLS * sizeof(my_bool));
	size_t length[NB_COLS];
	std::memset(length, 0, NB_COLS * sizeof(size_t));
	my_bool error[NB_COLS];
	std::memset(error, 0, NB_COLS * sizeof(my_bool));

	long id = 0;
	char j[STRING_SIZE + 1];
	char st[CASS_UUID_STRING_LENGTH + 1];
	MYSQL_TIME b;
	MYSQL_TIME e;
	MYSQL_TIME submissionTime;

	MYSQL_BIND result[NB_COLS];
	std::memset(result, 0, NB_COLS * sizeof(MYSQL_BIND));
	// id (INT)
	result[0].buffer_type = MYSQL_TYPE_LONG;
	result[0].buffer = &id;
	result[0].is_null = &isNull[0];
	result[0].is_unsigned = false;
	result[0].length = &length[0];
	result[0].error = &error[0];
	// command (VARCHAR)
	result[1].buffer_type = MYSQL_TYPE_VAR_STRING;
	result[1].buffer = j;
	result[1].buffer_length = STRING_SIZE;
	result[1].is_null = &isNull[1];
	result[1].length = &length[1];
	result[1].error = &error[1];
	// station (VARCHAR)
	result[2].buffer_type = MYSQL_TYPE_VAR_STRING;
	result[2].buffer = st;
	result[2].buffer_length = CASS_UUID_STRING_LENGTH;
	result[2].is_null = &isNull[2];
	result[2].length = &length[2];
	result[2].error = &error[2];
	// begin (DATETIME)
	result[3].buffer_type = MYSQL_TYPE_DATETIME;
	result[3].buffer = &b;
	result[3].is_null = &isNull[3];
	result[3].length = &length[3];
	result[3].error = &error[3];
	// e (DATETIME)
	result[4].buffer_type = MYSQL_TYPE_DATETIME;
	result[4].buffer = &e;
	result[4].is_null = &isNull[4];
	result[4].length = &length[4];
	result[4].error = &error[4];
	// submissionTime (DATETIME)
	result[5].buffer_type = MYSQL_TYPE_DATETIME;
	result[5].buffer = &submissionTime;
	result[5].is_null = &isNull[5];
	result[5].length = &length[5];
	result[5].error = &error[5];

	if (mysql_stmt_bind_result(stmt, result))
		panic(stmt, "Cannot bind result in statement \"retrieveJob\"");
	if (mysql_stmt_store_result(stmt))
		panic(stmt, "Cannot fetch the result\n");

	auto status = mysql_stmt_fetch(stmt);

	if (status == MYSQL_NO_DATA) {
		return {};
	} else if (status == MYSQL_DATA_TRUNCATED) {
		// XXX swallowed
	} else if (status != 0) {
		panic(stmt, "Fetching the next row failed");
	}

	StationJob stationJob{ 0, "", 0, {}, {}, {} };
	if (    isNull[0] || error[0] ||
		isNull[1] || error[1] ||
		isNull[2] || error[2] ||
		isNull[3] || error[3]  ||
		isNull[4] || error[4]  ||
		isNull[5] || error[5] ) {
		return {};
	}
	stationJob.id = id;
	stationJob.job = std::string(j, length[1]);
	cass_uuid_from_string(st, &stationJob.station);
	stationJob.begin = mysql2date(b);
	stationJob.end = mysql2date(e);
	stationJob.submissionDatetime = mysql2date(submissionTime);

	if (mysql_stmt_free_result(stmt))
		panic("Cannot free result of statement \"retrieveJob\"");

	MYSQL_STMT* reserveJobStmt = _reserveJobStmt.get();

	std::memset(params, 0, sizeof(MYSQL_BIND) * 1);
	params[0].buffer_type = MYSQL_TYPE_LONG;
	params[0].buffer = &stationJob.id;

	if (mysql_stmt_bind_param(reserveJobStmt, params))
		panic(reserveJobStmt, "Failed to bind params in statement \"reserveJob\"");
	if (mysql_stmt_execute(reserveJobStmt))
		panic(reserveJobStmt, "Failed to execute statement \"reserveJob\"");

	// Release the sentinel value to delete it manually and commit the
	// transaction instead of rolling it back
	if (mysql_commit(_db.get()))
		panic("Failed to commit the transaction");
	mysql_autocommit(_db.get(), true);
	delete sentinel.release();

	return stationJob;
}

bool DbConnectionJobs::markJobAsFinished(int jobId, time_t completionTimestamp,
		int statusCode)
{
	MYSQL_STMT* stmt = _markJobAsFinishedStmt.get();

	constexpr int NB_PARAMS = 3;
	MYSQL_BIND params[NB_PARAMS];
	std::memset(params, 0, sizeof(MYSQL_BIND) * NB_PARAMS);
	params[0].buffer_type = MYSQL_TYPE_LONG;
	params[0].buffer = &completionTimestamp;
	params[0].is_unsigned = 0;

	params[1].buffer_type = MYSQL_TYPE_LONG;
	params[1].buffer = &statusCode;
	params[1].is_unsigned = 0;

	params[2].buffer_type = MYSQL_TYPE_LONG;
	params[2].buffer = &jobId;
	params[2].is_unsigned = 0;

	if (mysql_stmt_bind_param(stmt, params)) {
		panic(stmt, "Failed to bind params in statement \"markJobAsFinished\"");
		return false;
	}
	if (mysql_stmt_execute(stmt)) {
		panic(stmt, "Failed to execute statement \"markJobAsFinished\"");
		return false;
	}

	return true;
}

bool DbConnectionJobs::publishStationJob(const char* jobType,
		const CassUuid& station, time_t begin, time_t end)
{
	MYSQL_STMT* stmt = _publishJobStmt.get();

	MYSQL_BIND params[4];
	std::memset(params, 0, sizeof(MYSQL_BIND) * 4);
	params[0].buffer_type = MYSQL_TYPE_VAR_STRING;
	params[0].buffer = const_cast<char*>(jobType);
	params[0].buffer_length = std::strlen(jobType);

	params[1].buffer_type = MYSQL_TYPE_VAR_STRING;
	char st[CASS_UUID_STRING_LENGTH];
	cass_uuid_string(station, st);
	params[1].buffer = st;
	params[1].buffer_length = CASS_UUID_STRING_LENGTH;

	params[2].buffer_type = MYSQL_TYPE_LONG;
	params[2].buffer = &begin;
	params[2].is_unsigned = 0;

	params[3].buffer_type = MYSQL_TYPE_LONG;
	params[3].buffer = &end;
	params[3].is_unsigned = 0;

	if (mysql_stmt_bind_param(stmt, params)) {
		panic(stmt, "Failed to bind params in statement \"publishJob\"");
		return false;
	}
	if (mysql_stmt_execute(stmt)) {
		panic(stmt, "Failed to execute statement \"publishJob\"");
		return false;
	}

	return true;
}

std::optional<DbConnectionJobs::StationJob> DbConnectionJobs::retrieveMinmax()
{
	return retrieveStationJob(JobType::MINMAX);
}

std::optional<DbConnectionJobs::StationJob> DbConnectionJobs::retrieveMonthMinmax()
{
	return retrieveStationJob(JobType::MONTH_MINMAX);
}

std::optional<DbConnectionJobs::StationJob> DbConnectionJobs::retrieveAnomalyMonitoring()
{
	return retrieveStationJob(JobType::ANOMALY_MONITORING);
}

bool DbConnectionJobs::publishMinmax(const CassUuid& station, time_t beginning, time_t end)
{
	return publishStationJob(JobType::MINMAX, station, beginning, end);
}

bool DbConnectionJobs::publishMonthMinmax(const CassUuid& station, time_t beginning, time_t end)
{
	return publishStationJob(JobType::MONTH_MINMAX, station, beginning, end);
}

bool DbConnectionJobs::publishAnomalyMonitoring(const CassUuid& station, time_t beginning, time_t end)
{
	return publishStationJob(JobType::ANOMALY_MONITORING, station, beginning, end);
}

date::sys_seconds DbConnectionJobs::mysql2date(const MYSQL_TIME& d)
{
	using namespace date;
	namespace chrono = std::chrono;
	// ignore seconds
	return sys_days{year{static_cast<int>(d.year)}/month{d.month}/day{d.day}} + chrono::hours{d.hour} + chrono::minutes{d.minute};
}

}
