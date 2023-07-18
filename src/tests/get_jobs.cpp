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
#include <fstream>
#include <cstdlib>
#include <thread>

#include <date.h>
#include <cassandra.h>
#include "../src/dbconnection_jobs.h"

using namespace meteodata;

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
	using namespace date;
	using namespace std::chrono_literals;

	DbConnectionJobs db;

	std::cerr << "DB ready" << std::endl;

	CassUuid u;
	cass_uuid_from_string("00000000-0000-0000-0000-111111111111", &u);

	time_t now = std::time(nullptr);
	time_t begin = now - 3600 * 24 * 5;
	time_t end = now - 3600 * 24;
	db.publishMinmax(u, begin, end);

	std::cerr << "Job published" << std::endl;

	auto j = db.retrieveMinmax();
	char st[CASS_UUID_STRING_LENGTH];
	cass_uuid_string(j->station, st);
	if (j) {
		std::cout <<
			"id: " << j->id << "\n" <<
			"jobType: " << j->job << "\n" <<
			"station: " << st << "\n" <<
			"begin: " << j->begin << "\n" <<
			"end: " << j->end << "\n";

		std::cout << "Wait a little, while the job is processed..." << std::endl;
		std::this_thread::sleep_for(3s);
		std::cout << "Done" << std::endl;
		db.markJobAsFinished(j->id, std::time(nullptr), 0);
	} else {
		std::cout << "Not found!" << std::endl;
		return -1;
	}
}
