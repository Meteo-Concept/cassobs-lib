/**
 * @file nbiot_station.h
 * @brief Definition of the NbiotStation class
 * @author Laurent Georget
 * @date 2024-06-21
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

#ifndef NBIOT_STATION_H
#define NBIOT_STATION_H

#include <string>

#include <cassandra.h>

namespace meteodata {

struct NbiotStation
{
	CassUuid station;
	std::string imei;
	std::string imsi;
	std::string hmacKey;
	std::string sensorType;
};

}

#endif
