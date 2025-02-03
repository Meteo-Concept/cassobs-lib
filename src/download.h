/**
 * @file download.h
 * @brief Definition of the Download class
 * @author Laurent Georget
 * @date 2025-02-02
 */
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

#ifndef DOWNLOAD_H
#define DOWNLOAD_H

#include <string>

#include <cassandra.h>
#include <date/date.h>

namespace meteodata {

struct Download
{
	CassUuid station;
	date::sys_seconds datetime;
	std::string connector;
	std::string content;
	bool inserted;
	std::string jobState;
};

}

#endif
