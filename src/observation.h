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
#include <array>

#include <cassandra.h>
#include <date.h>

namespace meteodata {

namespace chrono = std::chrono;

class Observation
{
public:
	CassUuid station;
	date::sys_days day;
	date::sys_seconds time;
	std::pair<bool,float> barometer                = {false,0};
	std::pair<bool,float> dewpoint                 = {false,0};
	std::pair<bool,int  > extrahum[2]              = {{false,0}};
	std::pair<bool,float> extratemp[3]             = {{false,0}};
	std::pair<bool,float> heatindex                = {false,0};
	std::pair<bool,int  > insidehum                = {false,0};
	std::pair<bool,float> insidetemp               = {false,0};
	std::pair<bool,float> leaftemp[2]              = {{false,0}};
	std::pair<bool,int  > leafwetnesses[2]         = {{false,0}};
	std::pair<bool,int  > outsidehum               = {false,0};
	std::pair<bool,float> outsidetemp              = {false,0};
	std::pair<bool,float> rainrate                 = {false,0};
	std::pair<bool,float> rainfall                 = {false,0};
	std::pair<bool,float> et                       = {false,0};
	std::pair<bool,float> soilmoistures[4]         = {{false,0}};
	std::pair<bool,float> soiltemp[4]              = {{false,0}};
	std::pair<bool,int  > solarrad                 = {false,0};
	std::pair<bool,int  > thswindex                = {false,0};
	std::pair<bool,int  > uv                       = {false,0};
	std::pair<bool,float> windchill                = {false,0};
	std::pair<bool,int  > winddir                  = {false,0};
	std::pair<bool,float> windgust                 = {false,0};
	std::pair<bool,float> windspeed                = {false,0};
	std::pair<bool,int  > insolation_time          = {false,0};
	std::pair<bool,float> min_outside_temperature  = {false,0};
	std::pair<bool,float> max_outside_temperature  = {false,0};
	std::pair<bool,int  > leafwetness_timeratio1   = {false,0};
	std::pair<bool,float> soilmoistures10cm        = {false,0};
	std::pair<bool,float> soilmoistures20cm        = {false,0};
	std::pair<bool,float> soilmoistures30cm        = {false,0};
	std::pair<bool,float> soilmoistures40cm        = {false,0};
	std::pair<bool,float> soilmoistures50cm        = {false,0};
	std::pair<bool,float> soilmoistures60cm        = {false,0};
	std::pair<bool,float> soiltemp10cm             = {false,0};
	std::pair<bool,float> soiltemp20cm             = {false,0};
	std::pair<bool,float> soiltemp30cm             = {false,0};
	std::pair<bool,float> soiltemp40cm             = {false,0};
	std::pair<bool,float> soiltemp50cm             = {false,0};
	std::pair<bool,float> soiltemp60cm             = {false,0};
	std::pair<bool,float> leafwetness_percent1     = {false,0};

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

	void filterOutImpossibleValues();

	static bool isValidIntVariable(const std::string& var);
	static bool isValidFloatVariable(const std::string& var);

private:
	static constexpr std::array<char const *, 28> VALID_VAR_INTS = {
		"extrahum1", "extra_humidity1",
		"extrahum2", "extra_humidity2",
		"insidehum", "inside_humidity",
		"leafwetnesses1", "leaf_wetness1",
		"leafwetnesses2", "leaf_wetness2",
		"soilmoistures1", "soil_moisture1",
		"soilmoistures2", "soil_moisture2",
		"soilmoistures3", "soil_moisture3",
		"soilmoistures4", "soil_moisture4",
		"outsidehum", "outside_humidity",
		"uv", "uv_index",
		"winddir", "wind_direction",
		"solarrad", "solar_radiation",
		"insolation_time",
		"leafwetness_timeratio1"
	};

	static constexpr std::array<char const *, 77> VALID_VAR_FLOATS = {
		"barometer", "pressure",
		"dewpoint", "dew_point",
		"extratemp1", "extra_temperature1",
		"extratemp2", "extra_temperature2",
		"extratemp3", "extra_temperature3",
		"heatindex",
		"insidetemp", "inside_temperature",
		"leaftemp1", "leaf_temperature1",
		"leaftemp2", "leaf_temperature2",
		"outsidetemp", "outside_temperature",
		"rainrate", "rain_rate",
		"rainfall",
		"et", "etp", "evapotranspiration",
		"soiltemp1", "soil_temp1", "soil_temperature1",
		"soiltemp2", "soil_temp2", "soil_temperature2",
		"soiltemp3", "soil_temp3", "soil_temperature3",
		"soiltemp4", "soil_temp4", "soil_temperature4",
		"thswindex", "thsw_index",
		"windchill",
		"windgust", "windgust_speed",
		"windspeed", "wind_speed",
		"min_outside_temperature",
		"max_outside_temperature",
		"soilmoistures10cm", "soil_moisture_10cm",
		"soilmoistures20cm", "soil_moisture_20cm",
		"soilmoistures30cm", "soil_moisture_30cm",
		"soilmoistures40cm", "soil_moisture_40cm",
		"soilmoistures50cm", "soil_moisture_50cm",
		"soilmoistures60cm", "soil_moisture_60cm",
		"soiltemp10cm", "soil_temp_10cm", "soil_temperature_10cm",
		"soiltemp20cm", "soil_temp_20cm", "soil_temperature_20cm",
		"soiltemp30cm", "soil_temp_30cm", "soil_temperature_30cm",
		"soiltemp40cm", "soil_temp_40cm", "soil_temperature_40cm",
		"soiltemp50cm", "soil_temp_50cm", "soil_temperature_50cm",
		"soiltemp60cm", "soil_temp_60cm", "soil_temperature_60cm",
		"leafwetness_percent1", "leaf_wetness_percent1"
	};
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
