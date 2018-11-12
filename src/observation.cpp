/**
 * @file observation.cpp
 * @brief Implementation of the Observation class
 * @author Laurent Georget
 * @date 2018-09-21
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

#include <exception>

#include <cassandra.h>
#include <date.h>

#include "observation.h"

namespace meteodata {

void Observation::setStation(CassUuid st)
{
	station = st;
}

void Observation::setTimestamp(date::sys_seconds timestamp)
{
	day = date::floor<date::days>(timestamp);
	time = timestamp;
}

void Observation::set(const std::string& column, float value)
{
	if (column == "barometer" || column == "pressure") {
		barometer.first = true;
		barometer.second = value;
	} else if (column == "dewpoint" || column == "dew_point") {
		dewpoint.first = true;
		dewpoint.second = value;
	} else if (column == "extratemp1" || column == "extra_temperature1") {
		extratemp[0].first = true;
		extratemp[0].second = value;
	} else if (column == "extratemp2" || column == "extra_temperature2") {
		extratemp[1].first = true;
		extratemp[1].second = value;
	} else if (column == "extratemp3" || column == "extra_temperature3") {
		extratemp[2].first = true;
		extratemp[2].second = value;
	} else if (column == "heatindex") {
		heatindex.first = true;
		heatindex.second = value;
	} else if (column == "insidetemp" || column == "inside_temperature") {
		insidetemp.first = true;
		insidetemp.second = value;
	} else if (column == "leaftemp1" || column == "leaf_temperature1") {
		leaftemp[0].first = true;
		leaftemp[0].second = value;
	} else if (column == "leaftemp2" || column == "leaf_temperature2") {
		leaftemp[1].first = true;
		leaftemp[1].second = value;
	} else if (column == "outsidetemp" || column == "outside_temperature") {
		outsidetemp.first = true;
		outsidetemp.second = value;
	} else if (column == "rainrate" || column == "rain_rate") {
		rainrate.first = true;
		rainrate.second = value;
	} else if (column == "rainfall") {
		rainfall.first = true;
		rainfall.second = value;
	} else if (column == "et" || column == "etp" || column == "evapotranspiration") {
		et.first = true;
		et.second = value;
	} else if (column == "soiltemp1" || column == "soil_temp1" || column == "soil_temperature1") {
		soiltemp[0].first = true;
		soiltemp[0].second = value;
	} else if (column == "soiltemp2" || column == "soil_temp2" || column == "soil_temperature2") {
		soiltemp[1].first = true;
		soiltemp[1].second = value;
	} else if (column == "soiltemp3" || column == "soil_temp3" || column == "soil_temperature3") {
		soiltemp[2].first = true;
		soiltemp[2].second = value;
	} else if (column == "soiltemp4" || column == "soil_temp4" || column == "soil_temperature4") {
		soiltemp[3].first = true;
		soiltemp[3].second = value;
	} else if (column == "thswindex" || column == "thsw_index") {
		thswindex.first = true;
		thswindex.second = value;
	} else if (column == "windchill") {
		windchill.first = true;
		windchill.second = value;
	} else if (column == "windgust" || column == "windgust_speed") {
		windgust.first = true;
		windgust.second = value;
	} else if (column == "windspeed" || column == "wind_speed") {
		windspeed.first = true;
		windspeed.second = value;
	} else {
		throw std::runtime_error("Column '" + column + "' does not exist or is not a float");
	}
}

void Observation::set(const std::string& column, int value)
{
	if (column == "extrahum1" || column == "extra_humidity1") {
		extrahum[0].first = true;
		extrahum[0].second = value;
	} else if (column == "extrahum2" || column == "extra_humidity2") {
		extrahum[1].first = true;
		extrahum[1].second = value;
	} else if (column == "insidehum" || column == "inside_humidity") {
		insidehum.first = true;
		insidehum.second = value;
	} else if (column == "leafwetnesses1" || column == "leaf_wetness1") {
		leafwetnesses[0].first = true;
		leafwetnesses[0].second = value;
	} else if (column == "leafwetnesses2" || column == "leaf_wetness2") {
		leafwetnesses[1].first = true;
		leafwetnesses[1].second = value;
	} else if (column == "soilmoistures1" || column == "soil_moisture1") { // mind the absence of -s
		soilmoistures[0].first = true;
		soilmoistures[0].second = value;
	} else if (column == "soilmoistures2" || column == "soil_moisture2") {
		soilmoistures[1].first = true;
		soilmoistures[1].second = value;
	} else if (column == "soilmoistures3" || column == "soil_moisture3") {
		soilmoistures[2].first = true;
		soilmoistures[2].second = value;
	} else if (column == "soilmoistures4" || column == "soil_moisture4") {
		soilmoistures[3].first = true;
		soilmoistures[3].second = value;
	} else if (column == "outsidehum" || column == "outside_humidity") {
		outsidehum.first = true;
		outsidehum.second = value;
	} else if (column == "uv" || column == "uv_index") {
		uv.first = true;
		uv.second = value;
	} else if (column == "winddir" || column == "wind_direction") {
		winddir.first = true;
		winddir.second = value;
	} else if (column == "solarrad" || column == "solar_radiation") {
		solarrad.first = true;
		solarrad.second = value;
	} else if (column == "insolation_time") {
		insolation_time.first = true;
		insolation_time.second = value;
	} else {
		throw std::runtime_error("Column '" + column + "' does not exist or is not an integer");
	}
}

template<>
CassUuid Observation::get<CassUuid>(const std::string& column) const
{
	if (column == "station" || column == "uuid") {
		return station;
	} else {
		throw std::runtime_error("Column '" + column + "' does not exist or is not a UUID");
	}
}

template<>
float Observation::get<float>(const std::string& column) const
{
	if (column == "barometer" || column == "pressure") {
		return barometer.second;
	} else if (column == "dewpoint" || column == "dew_point") {
		return dewpoint.second;
	} else if (column == "extratemp1" || column == "extra_temperature1") {
		return extratemp[0].second;
	} else if (column == "extratemp2" || column == "extra_temperature2") {
		return extratemp[1].second;
	} else if (column == "extratemp3" || column == "extra_temperature3") {
		return extratemp[2].second;
	} else if (column == "heatindex") {
		return heatindex.second;
	} else if (column == "insidetemp" || column == "inside_temperature") {
		return insidetemp.second;
	} else if (column == "leaftemp1" || column == "leaf_temperature1") {
		return leaftemp[0].second;
	} else if (column == "leaftemp2" || column == "leaf_temperature2") {
		return leaftemp[1].second;
	} else if (column == "outsidetemp" || column == "outside_temperature") {
		return outsidetemp.second;
	} else if (column == "rainrate" || column == "rain_rate") {
		return rainrate.second;
	} else if (column == "rainfall") {
		return rainfall.second;
	} else if (column == "et" || column == "etp" || column == "evapotranspiration") {
		return et.second;
	} else if (column == "soiltemp1" || column == "soil_temp1" || column == "soil_temperature1") {
		return soiltemp[0].second;
	} else if (column == "soiltemp2" || column == "soil_temp2" || column == "soil_temperature2") {
		return soiltemp[1].second;
	} else if (column == "soiltemp3" || column == "soil_temp3" || column == "soil_temperature3") {
		return soiltemp[2].second;
	} else if (column == "soiltemp4" || column == "soil_temp4" || column == "soil_temperature4") {
		return soiltemp[3].second;
	} else if (column == "thswindex" || column == "thsw_index") {
		return thswindex.second;
	} else if (column == "windchill") {
		return windchill.second;
	} else if (column == "windgust" || column == "windgust_speed") {
		return windgust.second;
	} else if (column == "windspeed" || column == "wind_speed") {
		return windspeed.second;
	} else {
		throw std::runtime_error("Column '" + column + "' does not exist or is not a float");
	}
}

template<>
int Observation::get<int>(const std::string& column) const
{
	if (column == "extrahum1" || column == "extra_humidity1") {
		return extrahum[0].second;
	} else if (column == "extrahum2" || column == "extra_humidity2") {
		return extrahum[1].second;
	} else if (column == "insidehum" || column == "inside_humidity") {
		return insidehum.second;
	} else if (column == "leafwetnesses1" || column == "leaf_wetness1") {
		return leafwetnesses[0].second;
	} else if (column == "leafwetnesses2" || column == "leaf_wetness2") {
		return leafwetnesses[1].second;
	} else if (column == "soilmoistures1" || column == "soil_moisture1") { // mind the absence of -s
		return soilmoistures[0].second;
	} else if (column == "soilmoistures2" || column == "soil_moisture2") {
		return soilmoistures[1].second;
	} else if (column == "soilmoistures3" || column == "soil_moisture3") {
		return soilmoistures[2].second;
	} else if (column == "soilmoistures4" || column == "soil_moisture4") {
		return soilmoistures[3].second;
	} else if (column == "outsidehum" || column == "outside_humidity") {
		return outsidehum.second;
	} else if (column == "uv" || column == "uv_index") {
		return uv.second;
	} else if (column == "solarrad" || column == "solar_radiation") {
		return solarrad.second;
	} else if (column == "winddir" || column == "wind_direction") {
		return winddir.second;
	} else if (column == "insolation_time") {
		return insolation_time.second;
	} else {
		throw std::runtime_error("Column '" + column + "' does not exist or is not an integer");
	}
}

bool Observation::isPresent(const std::string& column) const
{
	if (column == "station" || column == "uuid" || column == "time" ||
			column == "date" || column == "day") {
		return true;
	} else if (column == "barometer" || column == "pressure") {
		return barometer.first;
	} else if (column == "dewpoint" || column == "dew_point") {
		return dewpoint.first;
	} else if (column == "extratemp1" || column == "extra_temperature1") {
		return extratemp[0].first;
	} else if (column == "extratemp2" || column == "extra_temperature2") {
		return extratemp[1].first;
	} else if (column == "extratemp3" || column == "extra_temperature3") {
		return extratemp[2].first;
	} else if (column == "heatindex") {
		return heatindex.first;
	} else if (column == "insidetemp" || column == "inside_temperature") {
		return insidetemp.first;
	} else if (column == "leaftemp1" || column == "leaf_temperature1") {
		return leaftemp[0].first;
	} else if (column == "leaftemp2" || column == "leaf_temperature2") {
		return leaftemp[1].first;
	} else if (column == "outsidetemp" || column == "outside_temperature") {
		return outsidetemp.first;
	} else if (column == "rainrate" || column == "rain_rate") {
		return rainrate.first;
	} else if (column == "rainfall") {
		return rainfall.first;
	} else if (column == "et" || column == "etp" || column == "evapotranspiration") {
		return et.first;
	} else if (column == "soilmoistures1" || column == "soil_moisture1") { // mind the absence of -s
		return soilmoistures[0].first;
	} else if (column == "soilmoistures2" || column == "soil_moisture2") {
		return soilmoistures[1].first;
	} else if (column == "soilmoistures3" || column == "soil_moisture3") {
		return soilmoistures[2].first;
	} else if (column == "soilmoistures4" || column == "soil_moisture4") {
		return soilmoistures[3].first;
	} else if (column == "soiltemp1" || column == "soil_temp1" || column == "soil_temperature1") {
		return soiltemp[0].first;
	} else if (column == "soiltemp2" || column == "soil_temp2" || column == "soil_temperature2") {
		return soiltemp[1].first;
	} else if (column == "soiltemp3" || column == "soil_temp3" || column == "soil_temperature3") {
		return soiltemp[2].first;
	} else if (column == "soiltemp4" || column == "soil_temp4" || column == "soil_temperature4") {
		return soiltemp[3].first;
	} else if (column == "windchill") {
		return windchill.first;
	} else if (column == "windgust" || column == "windgust_speed") {
		return windgust.first;
	} else if (column == "windspeed" || column == "wind_speed") {
		return windspeed.first;
	} else if (column == "extrahum1" || column == "extra_humidity1") {
		return extrahum[0].first;
	} else if (column == "extrahum2" || column == "extra_humidity2") {
		return extrahum[1].first;
	} else if (column == "insidehum" || column == "inside_humidity") {
		return insidehum.first;
	} else if (column == "leafwetnesses1" || column == "leaf_wetness1") {
		return leafwetnesses[0].first;
	} else if (column == "leafwetnesses2" || column == "leaf_wetness2") {
		return leafwetnesses[1].first;
	} else if (column == "outsidehum" || column == "outside_humidity") {
		return outsidehum.first;
	} else if (column == "thswindex" || column == "thsw_index") {
		return thswindex.first;
	} else if (column == "solarrad" || column == "solar_radiation") {
		return solarrad.first;
	} else if (column == "uv" || column == "uv_index") {
		return uv.first;
	} else if (column == "winddir" || column == "wind_direction") {
		return winddir.first;
	} else if (column == "insolation_time") {
		return insolation_time.first;
	} else {
		throw std::runtime_error("Column '" + column + "' does not exist");
	}
}

template<>
date::sys_days Observation::get<date::sys_days>(const std::string& column) const
{
	if (column == "day" || column == "date") {
		return day;
	} else {
		throw std::runtime_error("Column '" + column + "' does not exist or is not a date");
	}
}

template<>
date::sys_seconds Observation::get<date::sys_seconds>(const std::string& column) const
{
	if (column == "day" || column == "date") {
		return day;
	} else if (column == "time") {
		return time;
	} else {
		throw std::runtime_error("Column '" + column + "' does not exist or is not a timestamp");
	}
}

}
