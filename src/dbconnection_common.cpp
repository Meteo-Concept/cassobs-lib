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
#include <vector>
#include <cstring>

#include <cassandra.h>
#include <syslog.h>
#include <unistd.h>
#include <pqxx/strconv>

#include "dbconnection_common.h"

using namespace date;

void pqxx::string_traits<std::vector<int>, void>::from_string(const char str[], std::vector<int>& obj)
{
	if (str[0] != '{') {
		throw std::runtime_error("Array format error");
	}
	std::string s{str, ::strlen(str)};
	std::istringstream is{s};
	int next = '{';
	while (is && next != '}' && next != ',') {
		int n;
		is >> n;
		if (is) {
			obj.push_back(n);
			next = is.get();
		}
	}
	if (next != '}') {
		throw std::runtime_error("Array format error");
	}
}

std::string pqxx::string_traits<std::vector<int>, void>::to_string(std::vector<int> obj)
{
	std::ostringstream os;
	os << "{";
	auto it = obj.begin();
	if (it != obj.end()) {
		os << *it;
		++it;
	}
	while (it != obj.end()) {
		os << "," << (*it);
		++it;
	}
	os << "}";
	return os.str();
}

namespace meteodata {

namespace chrono = std::chrono;

DbConnectionCommon::DbConnectionCommon(const std::string& address, const std::string& user, const std::string& password) :
	_session{cass_session_new(), cass_session_free},
	_cluster{cass_cluster_new(), cass_cluster_free}
{
	cass_cluster_set_contact_points(_cluster.get(), address.c_str());
	if (!user.empty() && !password.empty())
		cass_cluster_set_credentials_n(_cluster.get(), user.c_str(), user.length(), password.c_str(), password.length());
	cass_cluster_set_prepare_on_all_hosts(_cluster.get(), cass_true);
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
	bool ret = performSelect(_selectAllStations.get(),
		[&](const CassRow* row) {
			CassUuid uuid;
			cass_value_get_uuid(cass_row_get_column(row, 0), &uuid);
			stations.push_back(uuid);
		}
	);

	if (!ret)
		return false;

	ret = performSelect(_selectAllStationsFr.get(),
		[&](const CassRow* row) {
			CassUuid uuid;
			cass_value_get_uuid(cass_row_get_column(row, 0), &uuid);
			stations.push_back(uuid);
		}
	);

	return ret;
}

bool DbConnectionCommon::getStationDetails(const CassUuid& uuid, std::string& name, int& pollPeriod, time_t& lastArchiveDownloadTime, bool* storeInsideMeasurements)
{
	return performSelect(_selectStationDetails.get(),
		[&](const CassRow* row) {
			const CassValue* v = cass_row_get_column(row, 0);
			if (cass_value_is_null(v))
				return;
			const char* stationName;
			size_t size;
			cass_value_get_string(v, &stationName, &size);

			v = cass_row_get_column(row, 1);
			if (cass_value_is_null(v))
				return;
			cass_value_get_int32(v, &pollPeriod);

			v = cass_row_get_column(row, 2);
			if (cass_value_is_null(v))
				return;
			cass_int64_t timeMillisec;
			cass_value_get_int64(v, &timeMillisec);
			lastArchiveDownloadTime = timeMillisec/1000;

			name.clear();
			name.insert(0, stationName, size);

			if (storeInsideMeasurements) {
				v = cass_row_get_column(row, 3);
				if (cass_value_is_null(v)) {
					*storeInsideMeasurements = false;
				} else {
					cass_bool_t store;
					cass_value_get_bool(v, &store);
					*storeInsideMeasurements = store == cass_true;
				}
			}
		},
		[&](CassStatement* stmt) {
			cass_statement_bind_uuid(stmt, 0, uuid);
		}
	);
}

bool DbConnectionCommon::getStationLocation(const CassUuid& uuid, float& latitude, float& longitude, int& elevation)
{
	return performSelect(_selectStationLocation.get(),
		[&](const CassRow* row) {
			cass_value_get_float(cass_row_get_column(row,0), &latitude);
			cass_value_get_float(cass_row_get_column(row,1), &longitude);
			cass_value_get_int32(cass_row_get_column(row,2), &elevation);
		},
		[&](CassStatement* stmt) {
			cass_statement_bind_uuid(stmt, 0, uuid);
		}
	);
}

bool DbConnectionCommon::getWindValues(const CassUuid& uuid, const date::sys_days& date, std::vector<std::pair<int,float>>& values)
{
	return performSelect(_selectWindValues.get(),
		[this, &values](const CassRow* row) {
			std::pair<bool, int> dir;
			std::pair<bool, float> speed;
			storeCassandraInt(row, 0, dir);
			storeCassandraFloat(row, 1, speed);
			if (dir.first && speed.first)
				values.emplace_back(dir.second, speed.second);
		},
		[&](CassStatement* stmt) {
			cass_statement_bind_uuid(stmt, 0, uuid);
			cass_statement_bind_uint32(stmt, 1, from_sysdays_to_CassandraDate(date));
		}
	);
}

bool DbConnectionCommon::performSelect(const CassPrepared* stmt,
	const std::function<void(const CassRow*)>& rowHandler,
	const std::function<void(CassStatement*)>& parameterBinder
	)
{
	std::unique_ptr<CassStatement, void(&)(CassStatement*)> statement{
		cass_prepared_bind(stmt),
		cass_statement_free
	};
	cass_statement_set_is_idempotent(statement.get(), cass_true);
	parameterBinder(statement.get());

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
				if (row)
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
