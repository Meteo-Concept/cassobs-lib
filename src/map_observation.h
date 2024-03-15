/**
 * @file map_observation.h
 * @brief Definition of the MapObservation class
 * @author Laurent Georget
 * @date 2024-03-15
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

#ifndef MAP_OBSERVATION_H
#define MAP_OBSERVATION_H

#include <utility>

namespace meteodata {

class MapObservation
{
public:
	std::pair<bool, float> rainfall1h = { false, 0.0f };
	std::pair<bool, float> rainfall3h = { false, 0.0f };
	std::pair<bool, float> rainfall6h = { false, 0.0f };
	std::pair<bool, float> rainfall12h = { false, 0.0f };
	std::pair<bool, float> rainfall24h = { false, 0.0f };
	std::pair<bool, float> rainfall48h = { false, 0.0f };
	std::pair<bool, float> et1h = { false, 0.0f };
	std::pair<bool, float> et12h = { false, 0.0f };
	std::pair<bool, float> et24h = { false, 0.0f };
	std::pair<bool, float> et48h = { false, 0.0f };
	std::pair<bool, float> max_outside_temperature1h = { false, 0.0f };
	std::pair<bool, float> max_outside_temperature6h = { false, 0.0f };
	std::pair<bool, float> max_outside_temperature12h = { false, 0.0f };
	std::pair<bool, float> max_outside_temperature24h = { false, 0.0f };
	std::pair<bool, float> min_outside_temperature1h = { false, 0.0f };
	std::pair<bool, float> min_outside_temperature6h = { false, 0.0f };
	std::pair<bool, float> min_outside_temperature12h = { false, 0.0f };
	std::pair<bool, float> min_outside_temperature24h = { false, 0.0f };
	std::pair<bool, float> windgust1h = { false, 0.0f };
	std::pair<bool, float> windgust12h = { false, 0.0f };
	std::pair<bool, float> windgust24h = { false, 0.0f };
};

}

#endif
