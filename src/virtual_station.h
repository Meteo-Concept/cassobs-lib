/**
 * @file virtual_station.h
 * @brief Definition of the VirtualStation class
 * @author Laurent Georget
 * @date 2024-04-12
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

#ifndef VIRTUAL_STATION_H
#define VIRTUAL_STATION_H

#include <map>
#include <string>
#include <vector>

#include <cassandra.h>

namespace meteodata {

struct VirtualStation
{
	CassUuid station;
	int period;
	std::vector<std::pair<CassUuid, std::vector<std::string>>> sources;
};

}

#endif
