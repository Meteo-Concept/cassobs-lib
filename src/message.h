/**
 * @file message.h
 * @brief Definition of the Message class
 * @author Laurent Georget
 * @date 2016-10-05
 */
/*
 * Copyright (C) 2016  SAS Météo Concept <contact@meteo-concept.fr>
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

#ifndef MESSAGE_H
#define MESSAGE_H

#include <cassandra.h>

namespace meteodata {
/**
 * @brief Contain one data point and can populate a Cassandra prepared
 * insertion statement from the data
 *
 * A Message is responsible of three tasks:
 * - storing raw data from a station
 * - converting that data to the correct measure units
 * - fill in the blanks in a prepared statement to allow the data to be
 * entered in the database
 */
class Message
{
public:
	/**
	 * @brief Destroy the Message
	 */
	virtual ~Message() = default;
	/**
	 * @brief Fills in the blanks in a Cassandra insertion prepared
	 * statement
	 *
	 * @param station The station identifier for which the measure was taken
	 * @param statement The prepared statement in which to add the
	 * measured values stored in the current Message
	 */
	virtual void populateDataPoint(const CassUuid station, CassStatement* const statement) const = 0;
	virtual void populateV2DataPoint(const CassUuid station, CassStatement* const statement) const = 0;
};

}

#endif
