/*
 * Copyright (C) 2024  SAS Météo Concept <contact@meteo-concept.fr>
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

#include <chrono>
#include <iostream>
#include <fstream>
#include <cstdlib>

#include <date/date.h>
#include "../src/dbconnection_observations.h"
#include "../src/download.h"

/**
 * @brief The configuration file default path
 */
#define DEFAULT_CONFIG_FILE ".db_credentials"

using namespace std::chrono;
using namespace meteodata;
using namespace date;

/**
 * @brief Entry point
 *
 * @param argc the number of arguments passed on the command line
 * @param argv the arguments passed on the command line
 *
 * @return 0 if everything went well, and either an "errno-style" error code
 * or 255 otherwise
 */
int main()
{
	std::string dataAddress{std::getenv("CASSANDRA_HOST")};
	std::string dataUser{std::getenv("CASSANDRA_USER")};
	std::string dataPassword{std::getenv("CASSANDRA_PASSWORD")};
	std::string pgAddress{std::getenv("POSTGRES_HOST")};
	std::string pgUser{std::getenv("POSTGRES_USER")};
	std::string pgPassword{std::getenv("POSTGRES_PASSWORD")};

	cass_log_set_level(CASS_LOG_INFO);
	CassLogCallback logCallback =
		[](const CassLogMessage *message, void*) -> void {
			std::string logLevel =
				message->severity == CASS_LOG_CRITICAL ? "Critical error" :
				message->severity == CASS_LOG_ERROR    ? "Error" :
				message->severity == CASS_LOG_WARN     ? "Warning" :
				message->severity == CASS_LOG_INFO     ? "Notice" :
									 "Debug";

			std::cerr << "[" << logLevel << "] " << message->message << "(from " << message->function << ", in " << message->file << ", line " << message->line << std::endl;
		};
	cass_log_set_callback(logCallback, NULL);

	DbConnectionObservations db(dataAddress, dataUser, dataPassword, pgAddress, pgUser, pgPassword);
	Observation obs;
	CassUuid uuid;
	cass_uuid_from_string("00000000-0000-0000-0000-111111111111", &uuid);
	time_t now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());

	db.insertDownload(uuid, now, "test", "{}", false, "new");
	std::vector<Download> ds;
	db.selectDownloadsByStation(uuid, "test", ds);
	std::cerr << "Found " << ds.size() << " stations" << std::endl;
	for (const Download& d : ds) {
		char uuid_str[CASS_UUID_STRING_LENGTH];
		cass_uuid_string(d.station, uuid_str);
		std::cerr << uuid_str << " - " << d.connector << " - " << d.datetime << " - " << d.content << "\n";
		db.updateDownloadStatus(uuid, chrono::system_clock::to_time_t(d.datetime), false, "completed");
	}
}
