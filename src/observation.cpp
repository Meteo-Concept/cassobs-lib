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
#include "filter.h"

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

bool Observation::isValidIntVariable(const std::string& variable)
{
	return std::find(VALID_VAR_INTS.begin(), VALID_VAR_INTS.end(), variable) != VALID_VAR_INTS.end();
}

bool Observation::isValidFloatVariable(const std::string& variable)
{
	return std::find(VALID_VAR_FLOATS.begin(), VALID_VAR_FLOATS.end(), variable) != VALID_VAR_FLOATS.end();
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
	} else if (column == "min_windspeed" || column == "min_wind_speed") {
		min_windspeed.first = true;
		min_windspeed.second = value;
	} else if (column == "windspeed" || column == "wind_speed") {
		windspeed.first = true;
		windspeed.second = value;
	} else if (column == "min_outside_temperature") {
		min_outside_temperature.first = true;
		min_outside_temperature.second = value;
	} else if (column == "max_outside_temperature") {
		max_outside_temperature.first = true;
		max_outside_temperature.second = value;
	} else if (column == "soilmoistures10cm" || column == "soil_moisture_10cm") {
		soilmoistures10cm.first = true;
		soilmoistures10cm.second = value;
	} else if (column == "soilmoistures20cm" || column == "soil_moisture_20cm") {
		soilmoistures20cm.first = true;
		soilmoistures20cm.second = value;
	} else if (column == "soilmoistures30cm" || column == "soil_moisture_30cm") {
		soilmoistures30cm.first = true;
		soilmoistures30cm.second = value;
	} else if (column == "soilmoistures40cm" || column == "soil_moisture_40cm") {
		soilmoistures40cm.first = true;
		soilmoistures40cm.second = value;
	} else if (column == "soilmoistures50cm" || column == "soil_moisture_50cm") {
		soilmoistures50cm.first = true;
		soilmoistures50cm.second = value;
	} else if (column == "soilmoistures60cm" || column == "soil_moisture_60cm") {
		soilmoistures60cm.first = true;
		soilmoistures60cm.second = value;
	} else if (column == "soiltemp10cm" || column == "soil_temp_10cm" || column == "soil_temperature_10cm") {
		soiltemp10cm.first = true;
		soiltemp10cm.second = value;
	} else if (column == "soiltemp20cm" || column == "soil_temp_20cm" || column == "soil_temperature_20cm") {
		soiltemp20cm.first = true;
		soiltemp20cm.second = value;
	} else if (column == "soiltemp30cm" || column == "soil_temp_30cm" || column == "soil_temperature_30cm") {
		soiltemp30cm.first = true;
		soiltemp30cm.second = value;
	} else if (column == "soiltemp40cm" || column == "soil_temp_40cm" || column == "soil_temperature_40cm") {
		soiltemp40cm.first = true;
		soiltemp40cm.second = value;
	} else if (column == "soiltemp50cm" || column == "soil_temp_50cm" || column == "soil_temperature_50cm") {
		soiltemp50cm.first = true;
		soiltemp50cm.second = value;
	} else if (column == "soiltemp60cm" || column == "soil_temp_60cm" || column == "soil_temperature_60cm") {
		soiltemp60cm.first = true;
		soiltemp60cm.second = value;
	} else if (column == "leafwetness_percent1" || column == "leaf_wetness_percent1") {
		leafwetness_percent1.first = true;
		leafwetness_percent1.second = value;
	} else if (column == "voltage_battery") {
		voltage_battery.first = true;
		voltage_battery.second = value;
	} else if (column == "voltage_solar_panel") {
		voltage_solar_panel.first = true;
		voltage_solar_panel.second = value;
	} else if (column == "voltage_backup") {
		voltage_backup.first = true;
		voltage_backup.second = value;
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
	} else if (column == "leafwetness_timeratio1") {
		leafwetness_timeratio1.first = true;
		leafwetness_timeratio1.second = value;
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
	} else if (column == "min_windspeed" || column == "min_wind_speed") {
		return min_windspeed.second;
	} else if (column == "windspeed" || column == "wind_speed") {
		return windspeed.second;
	} else if (column == "min_outside_temperature") {
		return min_outside_temperature.second;
	} else if (column == "max_outside_temperature") {
		return max_outside_temperature.second;
	} else if (column == "soilmoistures10cm" || column == "soil_moisture_10cm") {
		return soilmoistures10cm.second;
	} else if (column == "soilmoistures20cm" || column == "soil_moisture_20cm") {
		return soilmoistures20cm.second;
	} else if (column == "soilmoistures30cm" || column == "soil_moisture_30cm") {
		return soilmoistures30cm.second;
	} else if (column == "soilmoistures40cm" || column == "soil_moisture_40cm") {
		return soilmoistures40cm.second;
	} else if (column == "soilmoistures50cm" || column == "soil_moisture_50cm") {
		return soilmoistures50cm.second;
	} else if (column == "soilmoistures60cm" || column == "soil_moisture_60cm") {
		return soilmoistures60cm.second;
	} else if (column == "soiltemp10cm" || column == "soil_temp_10cm" || column == "soil_temperature_10cm") {
		return soiltemp10cm.second;
	} else if (column == "soiltemp20cm" || column == "soil_temp_20cm" || column == "soil_temperature_20cm") {
		return soiltemp20cm.second;
	} else if (column == "soiltemp30cm" || column == "soil_temp_30cm" || column == "soil_temperature_30cm") {
		return soiltemp30cm.second;
	} else if (column == "soiltemp40cm" || column == "soil_temp_40cm" || column == "soil_temperature_40cm") {
		return soiltemp40cm.second;
	} else if (column == "soiltemp50cm" || column == "soil_temp_50cm" || column == "soil_temperature_50cm") {
		return soiltemp50cm.second;
	} else if (column == "soiltemp60cm" || column == "soil_temp_60cm" || column == "soil_temperature_60cm") {
		return soiltemp60cm.second;
	} else if (column == "leafwetness_percent1" || column == "leaf_wetness_percent1") {
		return leafwetness_percent1.second;
	} else if (column == "voltage_battery") {
		return voltage_battery.second;
	} else if (column == "voltage_solar_panel") {
		return voltage_solar_panel.second;
	} else if (column == "voltage_backup") {
		return voltage_backup.second;
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
	} else if (column == "leafwetness_timeratio1") {
		return leafwetness_timeratio1.second;
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
	} else if (column == "min_windspeed" || column == "min_wind_speed") {
		return min_windspeed.first;
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
	} else if (column == "min_outside_temperature") {
		return min_outside_temperature.first;
	} else if (column == "max_outside_temperature") {
		return max_outside_temperature.first;
	} else if (column == "leafwetness_timeratio1") {
		return leafwetness_timeratio1.first;
	} else if (column == "soilmoistures10cm" || column == "soil_moisture_10cm") {
		return soilmoistures10cm.first;
	} else if (column == "soilmoistures20cm" || column == "soil_moisture_20cm") {
		return soilmoistures20cm.first;
	} else if (column == "soilmoistures30cm" || column == "soil_moisture_30cm") {
		return soilmoistures30cm.first;
	} else if (column == "soilmoistures40cm" || column == "soil_moisture_40cm") {
		return soilmoistures40cm.first;
	} else if (column == "soilmoistures50cm" || column == "soil_moisture_50cm") {
		return soilmoistures50cm.first;
	} else if (column == "soilmoistures60cm" || column == "soil_moisture_60cm") {
		return soilmoistures60cm.first;
	} else if (column == "soiltemp10cm" || column == "soil_temp_10cm" || column == "soil_temperature_10cm") {
		return soiltemp10cm.first;
	} else if (column == "soiltemp20cm" || column == "soil_temp_20cm" || column == "soil_temperature_20cm") {
		return soiltemp20cm.first;
	} else if (column == "soiltemp30cm" || column == "soil_temp_30cm" || column == "soil_temperature_30cm") {
		return soiltemp30cm.first;
	} else if (column == "soiltemp40cm" || column == "soil_temp_40cm" || column == "soil_temperature_40cm") {
		return soiltemp40cm.first;
	} else if (column == "soiltemp50cm" || column == "soil_temp_50cm" || column == "soil_temperature_50cm") {
		return soiltemp50cm.first;
	} else if (column == "soiltemp60cm" || column == "soil_temp_60cm" || column == "soil_temperature_60cm") {
		return soiltemp60cm.first;
	} else if (column == "leafwetness_percent1" || column == "leaf_wetness_percent1") {
		return leafwetness_percent1.first;
	} else if (column == "voltage_battery") {
		return voltage_battery.first;
	} else if (column == "voltage_solar_panel") {
		return voltage_solar_panel.first;
	} else if (column == "voltage_backup") {
		return voltage_backup.first;
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

void Observation::filterOutImpossibleValues()
{
	/*************************************************************/
	barometer.first = barometer.first
		&& barometer.second >= Filter::MIN_BAROMETER
		&& barometer.second <= Filter::MAX_BAROMETER;
	/*************************************************************/
	dewpoint.first = dewpoint.first
		&& dewpoint.second >= Filter::MIN_AIR_TEMPERATURE
		&& dewpoint.second <= Filter::MAX_AIR_TEMPERATURE;
	/*************************************************************/
	for (auto& hum : extrahum) {
		hum.first = hum.first
			&& hum.second >= Filter::MIN_HUMIDITY
			&& hum.second <= Filter::MAX_HUMIDITY;
	}
	/*************************************************************/
	for (auto& temp : extratemp) {
		temp.first = temp.first
			&& temp.second >= Filter::MIN_AIR_TEMPERATURE
			&& temp.second <= Filter::MAX_AIR_TEMPERATURE;
	}
	/*************************************************************/
	heatindex.first = heatindex.first
		&& heatindex.second >= Filter::MIN_AIR_TEMPERATURE
		&& heatindex.second <= Filter::MAX_AIR_TEMPERATURE;
	/*************************************************************/
	// Do not record inside hum
	/*************************************************************/
	// Do not record inside temp
	/*************************************************************/
	for (int i=0 ; i<2 ; i++) {
		leaftemp[i].first = leaftemp[i].first
			&& leaftemp[i].second >= Filter::MIN_AIR_TEMPERATURE
			&& leaftemp[i].second <= Filter::MAX_AIR_TEMPERATURE;
		leafwetnesses[i].first = leafwetnesses[i].first
			&& leafwetnesses[i].second >= Filter::MIN_AIR_TEMPERATURE
			&& leafwetnesses[i].second <= Filter::MAX_AIR_TEMPERATURE;
	}
	/*************************************************************/
	// special case for humidity, try to cap the value if it's not too much
	// above 100%, some sensors can derive rather quickly but we should not
	// discard the measurement outright
	if (outsidehum.second > Filter::MAX_HUMIDITY && outsidehum.second <= Filter::MAX_HUMIDITY * 1.2)
		outsidehum.second = Filter::MAX_HUMIDITY;
	outsidehum.first = outsidehum.first
		&& outsidehum.second >= Filter::MIN_HUMIDITY
		&& outsidehum.second <= Filter::MAX_HUMIDITY;
	/*************************************************************/
	outsidetemp.first = outsidetemp.first
		&& outsidetemp.second >= Filter::MIN_AIR_TEMPERATURE
		&& outsidetemp.second <= Filter::MAX_AIR_TEMPERATURE;
	/*************************************************************/
	rainrate.first = rainrate.first
		&& rainrate.second >= Filter::MIN_RAINRATE
		&& rainrate.second <= Filter::MAX_RAINRATE;
	/*************************************************************/
	rainfall.first = rainfall.first
		&& rainfall.second >= Filter::MIN_RAINFALL
		&& rainfall.second <= Filter::MAX_RAINFALL;
	/*************************************************************/
	et.first = et.first
		&& et.second >= Filter::MIN_ET
		&& et.second <= Filter::MAX_ET;
	/*************************************************************/
	for (auto& soilmoisture : soilmoistures) {
		soilmoisture.first = soilmoisture.first
			&& soilmoisture.second >= Filter::MIN_SOILMOISTURE
			&& soilmoisture.second <= Filter::MAX_SOILMOISTURE;
	}
	/*************************************************************/
	for (auto& i : soiltemp) {
		i.first = i.first
			&& i.second >= Filter::MIN_SOIL_TEMPERATURE
			&& i.second <= Filter::MAX_SOIL_TEMPERATURE;
	}
	/*************************************************************/
	solarrad.first = solarrad.first
		&& solarrad.second >= Filter::MIN_SOLARRAD
		&& solarrad.second <= Filter::MAX_SOLARRAD;
	/*************************************************************/
	thswindex.first = thswindex.first
		&& thswindex.second >= Filter::MIN_AIR_TEMPERATURE
		&& thswindex.second <= Filter::MAX_AIR_TEMPERATURE;
	/*************************************************************/
	uv.first = uv.first
		&& uv.second >= Filter::MIN_UV
		&& uv.second <= Filter::MAX_UV;
	/*************************************************************/
	windchill.first = windchill.first
		&& windchill.second >= Filter::MIN_AIR_TEMPERATURE
		&& windchill.second <= Filter::MAX_AIR_TEMPERATURE;
	/*************************************************************/
	winddir.first = winddir.first
		&& winddir.second >= Filter::MIN_WINDDIR
		&& winddir.second <= Filter::MAX_WINDDIR;
	/*************************************************************/
	windgust.first = windgust.first
		&& windgust.second >= Filter::MIN_WINDGUST_SPEED
		&& windgust.second <= Filter::MAX_WINDGUST_SPEED;
	/*************************************************************/
	min_windspeed.first = min_windspeed.first
		&& min_windspeed.second >= Filter::MIN_WIND_SPEED
		&& min_windspeed.second <= Filter::MAX_WIND_SPEED;
	/*************************************************************/
	windspeed.first = windspeed.first
		&& windspeed.second >= Filter::MIN_WIND_SPEED
		&& windspeed.second <= Filter::MAX_WIND_SPEED;
	/*************************************************************/
	// no treatment for insolation time
	/*************************************************************/
	min_outside_temperature.first = min_outside_temperature.first
		&& min_outside_temperature.second >= Filter::MIN_AIR_TEMPERATURE
		&& min_outside_temperature.second <= Filter::MAX_AIR_TEMPERATURE;
	/*************************************************************/
	max_outside_temperature.first = max_outside_temperature.first
		&& max_outside_temperature.second >= Filter::MIN_AIR_TEMPERATURE
		&& max_outside_temperature.second <= Filter::MAX_AIR_TEMPERATURE;
	/*************************************************************/
	// no treatment for leafwetness_timeratio
	/*************************************************************/
	soilmoistures10cm.first = soilmoistures10cm.first
		&& soilmoistures10cm.second >= Filter::MIN_PERCENTAGE
		&& soilmoistures10cm.second <= Filter::MAX_PERCENTAGE;
	soilmoistures20cm.first = soilmoistures20cm.first
		&& soilmoistures20cm.second >= Filter::MIN_PERCENTAGE
		&& soilmoistures20cm.second <= Filter::MAX_PERCENTAGE;
	soilmoistures30cm.first = soilmoistures30cm.first
		&& soilmoistures30cm.second >= Filter::MIN_PERCENTAGE
		&& soilmoistures30cm.second <= Filter::MAX_PERCENTAGE;
	soilmoistures40cm.first = soilmoistures40cm.first
		&& soilmoistures40cm.second >= Filter::MIN_PERCENTAGE
		&& soilmoistures40cm.second <= Filter::MAX_PERCENTAGE;
	soilmoistures50cm.first = soilmoistures50cm.first
		&& soilmoistures50cm.second >= Filter::MIN_PERCENTAGE
		&& soilmoistures50cm.second <= Filter::MAX_PERCENTAGE;
	soilmoistures60cm.first = soilmoistures60cm.first
		&& soilmoistures60cm.second >= Filter::MIN_PERCENTAGE
		&& soilmoistures60cm.second <= Filter::MAX_PERCENTAGE;
	/*************************************************************/
	soiltemp10cm.first = soiltemp10cm.first
		&& soiltemp10cm.second >= Filter::MIN_SOIL_TEMPERATURE
		&& soiltemp10cm.second <= Filter::MAX_SOIL_TEMPERATURE;
	soiltemp20cm.first = soiltemp20cm.first
		&& soiltemp20cm.second >= Filter::MIN_SOIL_TEMPERATURE
		&& soiltemp20cm.second <= Filter::MAX_SOIL_TEMPERATURE;
	soiltemp30cm.first = soiltemp30cm.first
		&& soiltemp30cm.second >= Filter::MIN_SOIL_TEMPERATURE
		&& soiltemp30cm.second <= Filter::MAX_SOIL_TEMPERATURE;
	soiltemp40cm.first = soiltemp40cm.first
		&& soiltemp40cm.second >= Filter::MIN_SOIL_TEMPERATURE
		&& soiltemp40cm.second <= Filter::MAX_SOIL_TEMPERATURE;
	soiltemp50cm.first = soiltemp50cm.first
		&& soiltemp50cm.second >= Filter::MIN_SOIL_TEMPERATURE
		&& soiltemp50cm.second <= Filter::MAX_SOIL_TEMPERATURE;
	soiltemp60cm.first = soiltemp60cm.first
		&& soiltemp60cm.second >= Filter::MIN_SOIL_TEMPERATURE
		&& soiltemp60cm.second <= Filter::MAX_SOIL_TEMPERATURE;
	/*************************************************************/
	leafwetness_percent1.first = leafwetness_percent1.first
		&& leafwetness_percent1.second >= Filter::MIN_PERCENTAGE
		&& leafwetness_percent1.second <= Filter::MAX_PERCENTAGE;
	/*************************************************************/
	voltage_battery.first = voltage_battery.first
		&& voltage_battery.second >= Filter::MIN_VOLTAGE
		&& voltage_battery.second >= Filter::MAX_VOLTAGE;
	voltage_solar_panel.first = voltage_solar_panel.first
		&& voltage_solar_panel.second >= Filter::MIN_VOLTAGE
		&& voltage_solar_panel.second >= Filter::MAX_VOLTAGE;
	voltage_backup.first = voltage_backup.first
		&& voltage_backup.second >= Filter::MIN_VOLTAGE
		&& voltage_backup.second >= Filter::MAX_VOLTAGE;
	/*************************************************************/
}

}
