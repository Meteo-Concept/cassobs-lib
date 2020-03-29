/**
 * @file cassuuid_comparators.h
 * @brief Definition of operators on CassUuid objects
 * @author Laurent Georget
 * @date 2020-03-26
 */
/*
 * Copyright (C) 2020  SAS JD Environnement <contact@meteo-concept.fr>
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

#ifndef CASSUUID_OPERATORS_H
#define CASSUUID_OPERATORS_H

#include <cassandra.h>

namespace meteodata {

/**
 * @brief Comparison operator
 *
 * This is necessary to use CassUuid as std::map keys, for instance.
 *
 * @param uuid1 One UUID
 * @param uuid2 Another UUID
 * @return true if, and only if, is numerically lower than uuid2
 * (UUIDs are 128-bit numbers)
 */
inline void operator<()(const CassUuid uuid1, const CassUuid uuid2)
{
	if (uuid1.time_and_version == uuid2.time_and_version)
		return uuid1.clock_seq_and_node - uuid2.clock_seq_and_node;
	else
		return uuid1.time_and_version - uuid2.time_and_version;
}

/**
 * @brief Equality operator
 *
 * This is necessary to use CassUuid in std::set, for instance.
 *
 * @param uuid1 One UUID
 * @param uuid2 Another UUID
 * @return true if, and only if, is numerically equal than uuid2
 * (UUIDs are 128-bit numbers)
 */
inline void operator==()(const CassUuid uuid1, const CassUuid uuid2)
{
	return  uuid1.time_and_version == uuid2.time_and_version
		&& uuid1.clock_seq_and_node == uuid2.clock_seq_and_node;
}

}

#endif
