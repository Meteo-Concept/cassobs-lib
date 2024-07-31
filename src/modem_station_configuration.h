/**
 * @file modem_station_configuration.h
 * @brief Definition of the ModemStationConfiguration class
 * @author Laurent Georget
 * @date 2024-07-26
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

#ifndef MODEM_STATION_CONFIGURATION_H
#define MODEM_STATION_CONFIGURATION_H

#include <string>

#include <cassandra.h>

namespace meteodata {

struct ModemStationConfiguration
{
	CassUuid station;
	int id;
	std::string config;
	time_t addedOn;
};

}

#endif

