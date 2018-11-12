/**
 * @file observation.h
 * @brief Definition of the Observation class
 * @author Laurent Georget
 * @date 2018-10-02
 */
/*
 * Copyright (C) 2018  SAS Météo Concept <contact@meteo-concept.fr>
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

#ifndef OBSERVATION_H
#define OBSERVATION_H

#include <ctime>
#include <string>

#include <cassandra.h>
#include <date.h>

namespace meteodata {

namespace chrono = std::chrono;

class Observation
{
	CassUuid station;
	date::sys_days day;
	date::sys_seconds time;
	std::pair<bool,float> barometer        = {false,0};
	std::pair<bool,float> dewpoint         = {false,0};
	std::pair<bool,int  > extrahum[2]      = {{false,0}};
	std::pair<bool,float> extratemp[3]     = {{false,0}};
	std::pair<bool,float> heatindex        = {false,0};
	std::pair<bool,int  > insidehum        = {false,0};
	std::pair<bool,float> insidetemp       = {false,0};
	std::pair<bool,float> leaftemp[2]      = {{false,0}};
	std::pair<bool,int  > leafwetnesses[2] = {{false,0}};
	std::pair<bool,int  > outsidehum       = {false,0};
	std::pair<bool,float> outsidetemp      = {false,0};
	std::pair<bool,float> rainrate         = {false,0};
	std::pair<bool,float> rainfall         = {false,0};
	std::pair<bool,float> et               = {false,0};
	std::pair<bool,float> soilmoistures[4] = {{false,0}};
	std::pair<bool,float> soiltemp[4]      = {{false,0}};
	std::pair<bool,int  > solarrad         = {false,0};
	std::pair<bool,int  > thswindex        = {false,0};
	std::pair<bool,int  > uv               = {false,0};
	std::pair<bool,float> windchill        = {false,0};
	std::pair<bool,int  > winddir          = {false,0};
	std::pair<bool,float> windgust         = {false,0};
	std::pair<bool,float> windspeed        = {false,0};
	std::pair<bool,int  > insolation_time  = {false,0};

public:
	template<typename ColumnType>
	ColumnType get(const std::string& column) const
	{
		throw std::runtime_error("Unsupported type requested");
	}

	void setStation(CassUuid st);
	void setTimestamp(date::sys_seconds timestamp);
	void set(const std::string& column, float value);
	void set(const std::string& column, int value);

	bool isPresent(const std::string& column) const;
};

template<>
CassUuid Observation::get<CassUuid>(const std::string& column) const;

template<>
int Observation::get<int>(const std::string& column) const;

template<>
float Observation::get<float>(const std::string& column) const;

template<>
date::sys_days Observation::get<date::sys_days>(const std::string& column) const;

template<>
date::sys_seconds Observation::get<date::sys_seconds>(const std::string& column) const;

}

#endif
