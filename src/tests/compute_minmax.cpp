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

#include <date/date.h>
#include "../src/dbconnection_minmax.h"

using namespace std::chrono;
using namespace meteodata;
using namespace date;
using namespace date::literals;

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
	std::string dataAddress{std::getenv("CASSANDRA_HOST") ?: "127.0.0.1"};
	std::string dataUser{std::getenv("CASSANDRA_USER") ?: ""};
	std::string dataPassword{std::getenv("CASSANDRA_PASSWORD") ?: ""};
	std::string pqAddress{std::getenv("POSTGRES_HOST") ?: "127.0.0.1"};
	std::string pqUser{std::getenv("POSTGRES_USER") ?: ""};
	std::string pqPassword{std::getenv("POSTGRES_PASSWORD") ?: ""};

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

	DbConnectionMinmax db(dataAddress, dataUser, dataPassword, pqAddress, pqUser, pqPassword);
	DbConnectionMinmax::Values values;
	CassUuid uuid;

	sys_days target{2024_y/11/21};

	cass_uuid_from_string("8217b396-2735-4de4-946b-fad1d8857d1b", &uuid);
	if (db.getValues6hTo6h(uuid, target, values)) {
		std::cout << "Between " << format("%Y-%m-%d at %Hh UTC", target + 6h)
			  << " and "    << format("%Y-%m-%d at %Hh UTC", target + 30h) << ": ";
		if (values.outsideTemp_max.first) {
			std::cout << "Tx=" << values.outsideTemp_max.second << "°C\n";
		} else {
			std::cout << "Tx is null\n";
		}
	} else {
		std::cout << "Getting the values from 6h to 6h failed" << std::endl;
		return 1;
	}

	if (db.getValues18hTo18h(uuid, target, values)) {
		std::cout << "Between " << format("%Y-%m-%d at %Hh UTC", target - 6h)
			  << " and "    << format("%Y-%m-%d at %Hh UTC", target + 18h) << ": ";
		if (values.outsideTemp_min.first) {
			std::cout << "Tn=" << values.outsideTemp_min.second << "°C\n";
		} else {
			std::cout << "Tn is null\n";
		}
	} else {
		std::cout << "Getting the values from 18h to 18h failed" << std::endl;
		return 1;
	}

	if (db.getValues0hTo0h(uuid, target, values)) {
		std::cout << "Between " << format("%Y-%m-%d at %Hh UTC", target)
			  << " and "    << format("%Y-%m-%d at %Hh UTC", target + 24h) << ": ";
		if (values.windgust_max.first) {
			std::cout << "gust=" << values.windgust_max.second << "km/h\n";
		} else {
			std::cout << "gust is null\n";
		}
	} else {
		std::cout << "Getting the values from 0h to 0h failed" << std::endl;
		return 1;
	}

	if (db.getYearlyValues(uuid, target - date::days{1}, values.yearRain, values.yearEt)) {
		std::cout << "For day " << format("%Y-%m-%d at %Hh UTC", target) << ": ";
		if (values.yearRain.first) {
			std::cout << "yearly rain=" << values.yearRain.second << "mm\n";
			values.yearRain.second += values.rainfall.second;
			values.yearEt.second += values.et.second;
		} else {
			std::cout << "yearly rain is null\n";
		}
	} else {
		std::cout << "Getting the yearly values failed" << std::endl;
		return 1;
	}

	if (db.insertDataPointInTimescaleDB(uuid, target, values)) {
		std::cout << "Inserting for day " << format("%Y-%m-%d", target);
	} else {
		std::cout << "Inserting the values failed" << std::endl;
		return 1;
	}
}
