/*
 * Copyright (C) 2022  SAS Météo Concept <contact@meteo-concept.fr>
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
#include <fstream>
#include <cstdlib>

#include <date.h>
#include "../src/dbconnection_minmax.h"

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

	DbConnectionMinmax db(dataAddress, dataUser, dataPassword);
	DbConnectionMinmax::Values values;
	CassUuid uuid;
	cass_uuid_from_string("8217b396-2735-4de4-946b-fad1d8857d1b", &uuid);
	if (db.getValues6hTo6h(uuid, sys_days{2019_y/3/9}, values))
		std::cout << "Between 2019-03-09 at 6h UTC and 2019-03-10 at 6h UTC: " << "Tx=" << values.outsideTemp_max.second << "°C ; RR=" << values.rainfall.second << "mm" << std::endl;
	else
		std::cout << "Getting the minmax between 2019-03-09 at 6h UTC and 2019-03-10 at 6h UTC failed" << std::endl;

	if (db.getValues18hTo18h(uuid, sys_days{2019_y/3/9}, values))
		std::cout << "Between 2019-03-08 at 18h UTC and 2019-03-09 at 18h UTC: " << "Tn=" << values.outsideTemp_min.second << "°C" << std::endl;
	else
		std::cout << "Getting the minmax between 2019-03-08 at 18h UTC and 2019-03-09 at 18h UTC failed" << std::endl;

	if (db.getValues0hTo0h(uuid, sys_days{2019_y/3/9}, values))
		std::cout << "Between 2019-03-09 at 0h UTC and 2019-03-10 at 0h UTC: " << "wind=" << values.windspeed_avg.second << "km/h ; gust=" << values.windgust_max.second << "km/h" << std::endl;
	else
		std::cout << "Getting the minmax between 2019-03-09 at 0h UTC and 2019-03-10 at 0h UTC failed" << std::endl;
}
