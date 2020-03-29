/**
 * @file normals.h
 * @brief Definition of the Normals class
 * @author Laurent Georget
 * @date 2020-03-26
 */
/*
 * Copyright (C) 2020  SAS Météo Concept <contact@meteo-concept.fr>
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

#ifndef NORMALSS_H
#define NORMALSS_H

#include <functional>
#include <tuple>
#include <utility>
#include <map>
#include <vector>
#include <set>

#include <date.h>
#include <cassandra.h>

namespace meteodata {

/**
 * @brief A datastructure used to store the meteorological normals of a station
 */
class Normals
{
public:
	enum class NormalsVariables {
		NB_DAYS_WITH_SNOW,
		NB_DAYS_WITH_HAIL,
		NB_DAYS_WITH_STORM,
		NB_DAYS_WITH_FOG,
		NB_DAYS_GUST_OVER_28,
		NB_DAYS_GUST_OVER_16,
		WIND_SPEED,
		ETP,
		NB_DAYS_INSOLATION_OVER_80,
		NB_DAYS_INSOLATION_UNDER_20,
		NB_DAYS_INSOLATION_AT_0,
		INSOLATION_TIME,
		GLOBAL_IRRADIANCE,
		DJU,
		NB_DAYS_RR_OVER_10,
		NB_DAYS_RR_OVER_5,
		NB_DAYS_RR_OVER_1,
		TOTAL_RAINFALL,
		NB_DAYS_TN_UNDER_MINUS_10,
		NB_DAYS_TN_UNDER_MINUS_5,
		NB_DAYS_TN_UNDER_0,
		NB_DAYS_TX_UNDER_0,
		NB_DAYS_TX_OVER_25,
		NB_DAYS_TX_OVER_30,
		TN,
		TM,
		TX
	};

private:
	// One set of records for the yearly normals +
	// one set of record for each month
	std::array<std::map<DayRecord, float>, 13> _values;

public:
	inline void setMonthRecord(NormalsRecord record, date::month month, float value) {
		unsigned m{month};
		_values[m][record] = value;
	}

	inline std::tuple<bool, float> getMonthNormal(NormalsRecord record, date::month month) {
		unsigned m{month};
		auto it = _values[m].find(record);
		if (it->first)
			return { true, it->second };
		else
			return { false, 0.f };
	}

	inline void setYearNormal(NormalsRecord record, float value) {
		_values[0][record] = value;
	}

	inline std::tuple<bool, float> getYearNormal(NormalsRecord record) {
		auto it = _values[0].find(record);
		if (it->first)
			return { true, it->second };
		else
			return { false, 0.f };
	}
};

}

#endif
