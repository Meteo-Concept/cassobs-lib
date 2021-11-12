/**
 * @file dbconnection_month_minmax.cpp
 * @brief Implementation of the DbConnectionCommon class
 * @author Laurent Georget
 * @date 2018-07-12
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

#include <iostream>
#include <mutex>
#include <exception>
#include <chrono>
#include <unordered_map>
#include <functional>

#include <cassandra.h>
#include <syslog.h>
#include <unistd.h>

#include "dbconnection_common.h"

using namespace date;

namespace meteodata {

constexpr char DbConnectionCommon::SELECT_ALL_STATIONS_STMT[];
constexpr char DbConnectionCommon::SELECT_ALL_STATIONS_FR_STMT[];
constexpr char DbConnectionCommon::SELECT_WIND_VALUES_STMT[];
constexpr char DbConnectionCommon::SELECT_STATION_DETAILS_STMT[];
constexpr char DbConnectionCommon::SELECT_STATION_LOCATION_STMT[];

namespace chrono = std::chrono;

DbConnectionCommon::DbConnectionCommon(const std::string& address, const std::string& user, const std::string& password) :
	_session{cass_session_new(), cass_session_free},
	_cluster{cass_cluster_new(), cass_cluster_free}
{
	cass_cluster_set_contact_points(_cluster.get(), address.c_str());
	if (!user.empty() && !password.empty())
		cass_cluster_set_credentials_n(_cluster.get(), user.c_str(), user.length(), password.c_str(), password.length());
	CassFuture* futureConn = cass_session_connect(_session.get(), _cluster.get());
	CassError rc = cass_future_error_code(futureConn);
	cass_future_free(futureConn);
	if (rc != CASS_OK) {
		std::string desc("Impossible to connect to database: ");
		desc.append(cass_error_desc(rc));
		throw std::runtime_error(desc);
	} else {
		DbConnectionCommon::prepareStatements();
	}
}

void DbConnectionCommon::prepareOneStatement(CassandraStmtPtr& stmt, const std::string& query)
{
	CassFuture* prepareFuture = cass_session_prepare(_session.get(), query.c_str());
	CassError rc = cass_future_error_code(prepareFuture);
	if (rc != CASS_OK) {
		std::string desc("Could not prepare statement: ");
		desc.append(cass_error_desc(rc));
		throw std::runtime_error(desc);
	}
	stmt.reset(cass_future_get_prepared(prepareFuture));
	cass_future_free(prepareFuture);
}

void DbConnectionCommon::prepareStatements()
{
	prepareOneStatement(_selectAllStations, SELECT_ALL_STATIONS_STMT);
	prepareOneStatement(_selectAllStationsFr, SELECT_ALL_STATIONS_FR_STMT);
	prepareOneStatement(_selectStationDetails, SELECT_STATION_DETAILS_STMT);
	prepareOneStatement(_selectStationLocation, SELECT_STATION_LOCATION_STMT);
	prepareOneStatement(_selectWindValues, SELECT_WIND_VALUES_STMT);
}

bool DbConnectionCommon::getAllStations(std::vector<CassUuid>& stations)
{
	CassFuture* query;
	CassStatement* statement = cass_prepared_bind(_selectAllStations.get());
	query = cass_session_execute(_session.get(), statement);
	cass_statement_free(statement);

	const CassResult* result = cass_future_get_result(query);
	bool ret = false;
	if (result) {
		CassIterator* iterator = cass_iterator_from_result(result);
		CassUuid uuid;
		while (cass_iterator_next(iterator)) {
			const CassRow* row = cass_iterator_get_row(iterator);
			cass_value_get_uuid(cass_row_get_column(row,0), &uuid);
			stations.push_back(uuid);
		}
		ret = true;
		cass_iterator_free(iterator);
	}
	cass_result_free(result);
	cass_future_free(query);

	if (!ret)
		return ret;

	statement = cass_prepared_bind(_selectAllStationsFr.get());
	query = cass_session_execute(_session.get(), statement);
	cass_statement_free(statement);

	result = cass_future_get_result(query);
	ret = false;
	if (result) {
		CassIterator* iterator = cass_iterator_from_result(result);
		CassUuid uuid;
		while (cass_iterator_next(iterator)) {
			const CassRow* row = cass_iterator_get_row(iterator);
			cass_value_get_uuid(cass_row_get_column(row,0), &uuid);
			stations.push_back(uuid);
		}
		ret = true;
		cass_iterator_free(iterator);
	}
	cass_result_free(result);
	cass_future_free(query);

	return ret;
}

bool DbConnectionCommon::getStationDetails(const CassUuid& uuid, std::string& name, int& pollPeriod, time_t& lastArchiveDownloadTime)
{
	std::unique_ptr<CassStatement, void(&)(CassStatement*)> statement{
		cass_prepared_bind(_selectStationDetails.get()),
		cass_statement_free
	};
	cass_statement_bind_uuid(statement.get(), 0, uuid);
	std::unique_ptr<CassFuture, void(&)(CassFuture*)> query{
		cass_session_execute(_session.get(), statement.get()),
		cass_future_free
	};
	std::unique_ptr<const CassResult, void(&)(const CassResult*)> result{
		cass_future_get_result(query.get()),
		cass_result_free
	};

	bool ret = false;
	if (result) {
		const CassRow* row = cass_result_first_row(result.get());
		if (row) {
			const char *stationName;
			size_t size;
			cass_value_get_string(cass_row_get_column(row,0), &stationName, &size);
			cass_value_get_int32(cass_row_get_column(row,1), &pollPeriod);
			cass_int64_t timeMillisec;
			cass_value_get_int64(cass_row_get_column(row,2), &timeMillisec);
			lastArchiveDownloadTime = timeMillisec/1000;
			name.clear();
			name.insert(0, stationName, size);
			ret = true;
		}
	}

	return ret;
}

bool DbConnectionCommon::getStationLocation(const CassUuid& uuid, float& latitude, float& longitude, int& elevation)
{
	std::unique_ptr<CassStatement, void(&)(CassStatement*)> statement{
		cass_prepared_bind(_selectStationLocation.get()),
		cass_statement_free
	};
	cass_statement_bind_uuid(statement.get(), 0, uuid);
	std::unique_ptr<CassFuture, void(&)(CassFuture*)> query{
		cass_session_execute(_session.get(), statement.get()),
		cass_future_free
	};
	std::unique_ptr<const CassResult, void(&)(const CassResult*)> result{
		cass_future_get_result(query.get()),
		cass_result_free
	};

	bool ret = false;
	if (result) {
		const CassRow* row = cass_result_first_row(result.get());
		if (row) {
			cass_value_get_float(cass_row_get_column(row,0), &latitude);
			cass_value_get_float(cass_row_get_column(row,1), &longitude);
			cass_value_get_int32(cass_row_get_column(row,2), &elevation);
			ret = true;
		}
	}

	return ret;
}

bool DbConnectionCommon::getWindValues(const CassUuid& uuid, const date::sys_days& date, std::vector<std::pair<int,float>>& values)
{
	CassFuture* query;
	CassStatement* statement = cass_prepared_bind(_selectWindValues.get());
	cass_statement_bind_uuid(statement, 0, uuid);
	cass_statement_bind_uint32(statement, 1, from_sysdays_to_CassandraDate(date));
	query = cass_session_execute(_session.get(), statement);
	cass_statement_free(statement);

	bool ret = false;
	const CassResult* result = cass_future_get_result(query);
	CassIterator* it = cass_iterator_from_result(result);
	while(cass_iterator_next(it)) {
		const CassRow* row = cass_iterator_get_row(it);
		std::pair<bool,int> dir;
		std::pair<bool,float> speed;
		storeCassandraInt(row, 0, dir);
		storeCassandraFloat(row, 1, speed);
		if (dir.first && speed.first)
			values.emplace_back(dir.second, speed.second);
	}
	ret = true;
	cass_iterator_free(it);
	cass_result_free(result);
	cass_future_free(query);


	return ret;
}

	bool DbConnectionCommon::performSelect(const CassPrepared* stmt, const std::function<void(const CassRow*)>& rowHandler)
	{
		std::unique_ptr<CassStatement, void(&)(CassStatement*)> statement{
			cass_prepared_bind(stmt),
			cass_statement_free
		};

		cass_bool_t hasMorePages = cass_true;
		bool ret = true;
		while (ret && hasMorePages) {
			std::unique_ptr<CassFuture, void(&)(CassFuture*)> query{
				cass_session_execute(_session.get(), statement.get()),
				cass_future_free
			};
			std::unique_ptr<const CassResult, void(&)(const CassResult*)> result{
				cass_future_get_result(query.get()),
				cass_result_free
			};
			ret = false;
			if (result) {
				std::unique_ptr<CassIterator, void(&)(CassIterator*)> iterator{
					cass_iterator_from_result(result.get()),
					cass_iterator_free
				};
				while (cass_iterator_next(iterator.get())) {
					const CassRow* row = cass_iterator_get_row(iterator.get());
					rowHandler(row);
				}
				ret = true;

				hasMorePages = cass_result_has_more_pages(result.get());
				if (hasMorePages) {
					cass_statement_set_paging_state(statement.get(), result.get());
				}
			}
		}

		return ret;
	}
}
