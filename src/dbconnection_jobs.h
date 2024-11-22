/**
 * @file dbconnection_jobs.h
 * @brief Definition of the DbConnectionJobs class
 * @author Laurent Georget
 * @date 2023-07-17
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

#ifndef DBCONNECTION_JOBS_H
#define DBCONNECTION_JOBS_H

#include <cstdint>
#include <ctime>
#include <functional>
#include <tuple>
#include <memory>
#include <tuple>
#include <vector>
#include <string>
#include <map>

#include <mysql.h>
#include <date/date.h>
#include <optional>

#include "message.h"
#include "dbconnection_common.h"
#include "observation.h"

namespace meteodata {
/**
 * @brief A handle to the database to insert and retrieves jobs or tasks to
 * complete
 */
class DbConnectionJobs
{
public:
	/**
	 * @brief Construct a connection to the database
	 *
	 * @param user the username to use
	 * @param password the password corresponding to the username
	 */
	DbConnectionJobs(const std::string& address = "127.0.0.1", const std::string& user = "", const std::string& password = "", const std::string& database = "observations2020");
	/**
	 * @brief Close the connection and destroy the database handle
	 */
	virtual ~DbConnectionJobs() = default;

	struct JobType {
		static constexpr char MINMAX[] = "minmax";
		static constexpr char MONTH_MINMAX[] = "month_minmax";
		static constexpr char ANOMALY_MONITORING[] = "anomaly_monitoring";
	};

	struct StationJob
	{
		long id;
		std::string job;
		CassUuid station;
		date::sys_seconds submissionDatetime;
		date::sys_seconds begin;
		date::sys_seconds end;
	};

	/**
	 * @brief Publish a minmax job
	 *
	 * @param station The station's UUID
	 * @param begin The beginning of the period to (re)compute
	 * @param end The end of the period to (re)compute
	 *
	 * @return The boolean value true if everything went well, false if an error occurred
	 */
	bool publishMinmax(const CassUuid& station, time_t beginning, time_t end);
	/**
	 * @brief Retrieve the next available minmax job
	 *
	 * @return A station job if one could be found or an empty value
	 */
	std::optional<StationJob> retrieveMinmax();

	/**
	 * @brief Publish a minmax job
	 *
	 * @param station The station's UUID
	 * @param begin The beginning of the period to (re)compute
	 * @param end The end of the period to (re)compute
	 *
	 * @return The boolean value true if everything went well, false if an error occurred
	 */
	bool publishMonthMinmax(const CassUuid& station, time_t beginning, time_t end);
	/**
	 * @brief Publish a monthly minmax job
	 *
	 * @return A station job if one could be found or an empty value
	 */
	std::optional<StationJob> retrieveMonthMinmax();

	/**
	 * @brief Publish an anomaly monitoring job
	 *
	 * @param station The station's UUID
	 * @param begin The beginning of the period to (re)compute
	 * @param end The end of the period to (re)compute
	 *
	 * @return The boolean value true if everything went well, false if an error occurred
	 */
	bool publishAnomalyMonitoring(const CassUuid& station, time_t beginning, time_t end);
	/**
	 * @brief Publish an anomaly monitoring job
	 *
	 * @return A station job if one could be found or an empty value
	 */
	std::optional<StationJob> retrieveAnomalyMonitoring();

	/**
	 * @brief Register a job in the database as finished, with a completion
	 * date and a status code (0 if everything went well, an error code
	 * otherwise)
	 *
	 * @param jobId The job id in the database, as retrieved from a
	 * "retrieve" query
	 * @param completionTimestamp A timestamp of when the job was done (or
	 * unsuccesfully attempted and deemed not doable)
	 * @param statusCode The exit code of the program that did the job
	 */
	bool markJobAsFinished(int jobId, time_t completionDatetime,
			int statusCode);


private:
	std::unique_ptr<MYSQL, decltype(&mysql_close)> _db;

	static constexpr char RESERVE_JOB[] =
			"UPDATE jobs SET started_at = NOW() WHERE jobs.id = ?";
	std::unique_ptr<MYSQL_STMT, decltype(&mysql_stmt_close)> _reserveJobStmt;

	static constexpr char MARK_JOB_AS_FINISHED[] =
			"UPDATE jobs SET completed_at = FROM_UNIXTIME(?), status_code = ? WHERE jobs.id = ?";
	std::unique_ptr<MYSQL_STMT, decltype(&mysql_stmt_close)> _markJobAsFinishedStmt;

	static constexpr char RETRIEVE_JOB[] =
			"SELECT j.id, j.command, j.station, j.begin, j.end, j.submitted_at "
			" FROM jobs as j "
			" WHERE j.command = ? AND j.started_at IS NULL "
			" ORDER BY j.submitted_at LIMIT 1 FOR UPDATE SKIP LOCKED";
	std::unique_ptr<MYSQL_STMT, decltype(&mysql_stmt_close)> _retrieveJobStmt;

	static constexpr char PUBLISH_JOB[] =
			"INSERT INTO jobs (command, station, begin, end) "
			" VALUES (?, ?, FROM_UNIXTIME(?), FROM_UNIXTIME(?)) ";
	std::unique_ptr<MYSQL_STMT, decltype(&mysql_stmt_close)> _publishJobStmt;

	void prepareStatements();
	void panic(const std::string& msg);
	static void panic(MYSQL_STMT* stmt, const std::string& msg);

	static date::sys_seconds mysql2date(const MYSQL_TIME& d);
	std::optional<StationJob> retrieveStationJob(const char* jobType);
	bool publishStationJob(const char* jobType, const CassUuid& station,
						  time_t begin, time_t end);

	static constexpr size_t STRING_SIZE=191;
};

}

#endif
