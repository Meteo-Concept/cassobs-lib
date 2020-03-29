/**
 * @file dbconnection_normals.cpp
 * @brief Implementation of the DbConnectionNormals class
 * @author Laurent Georget
 * @date 2020-03-27
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

#include <iostream>
#include <cstring>
#include <functional>
#include <tuple>
#include <memory>
#include <vector>
#include <utility>
#include <map>

#include <mysql.h>
#include <date.h>
#include <cassandra.h>

#include "dbconnection_normals.h"

namespace meteodata {

using Neighbor = DbConnectionNormals::Neighbor;
using Values = DbConnectionNormals::Values;

constexpr char DbConnectionNormals::GET_STATIONS_WITH_NORMALS_NEARBY[];
constexpr char DbConnectionNormals::GET_NORMALS[];

DbConnectionNormals::DbConnectionNormals(const std::string& host, const std::string& user, const std::string& password, const std::string& database) :
	_db{mysql_init(nullptr), &mysql_close},
	_getStationsWithNormalsNearbyStmt{nullptr, &mysql_stmt_close},
	_getNormalsStmt{nullptr, &mysql_stmt_close}
{
	if (!mysql_real_connect(_db.get(),
			host.empty() ? nullptr : host.c_str(),
			user.empty() ? nullptr : user.c_str(),
			password.empty() ? nullptr : password.c_str(),
			database.empty() ? nullptr : database.c_str(),
			0,
			"/var/run/mysqld/mysqld.sock",
			0)
	    ) {
		panic("Cannot connect to the database");
	}
	mysql_set_character_set(_db.get(), "utf8mb4");

	prepareStatements();
}

void DbConnectionNormals::panic(const std::string& msg)
{
	std::cerr << msg << ": " << mysql_error(_db.get());
	throw std::runtime_error("DB Fatal error");
}

void DbConnectionNormals::panic(MYSQL_STMT* stmt, const std::string& msg)
{
	std::cerr << msg << ": " << mysql_stmt_error(stmt) << " (" << mysql_stmt_errno(stmt) << ")";
	throw std::runtime_error("DB Fatal error");
}

void DbConnectionNormals::prepareStatements()
{
	_getStationsWithNormalsNearbyStmt.reset(mysql_stmt_init(_db.get()));
	if (mysql_stmt_prepare(_getStationsWithNormalsNearbyStmt.get(), GET_STATIONS_WITH_NORMALS_NEARBY, sizeof(GET_STATIONS_WITH_NORMALS_NEARBY)))
		panic("Could not prepare statement \"getStationsWithNormalsNearby\"");

	_getNormalsStmt.reset(mysql_stmt_init(_db.get()));
	if (mysql_stmt_prepare(_getNormalsStmt.get(), GET_NORMALS, sizeof(GET_NORMALS)))
		panic("Could not prepare statement \"getNormals\"");
}

std::vector<Neighbor> DbConnectionNormals::getStationsWithNormalsNearby(const CassUuid& uuid)
{
	MYSQL_STMT* stmt = _getStationsWithNormalsNearbyStmt.get();

	MYSQL_BIND params[2];
	std::memset(params, 0, 2 * sizeof(MYSQL_BIND));
	int limit = 1;
	char uuidStr[CASS_UUID_STRING_LENGTH];
	cass_uuid_string(uuid, uuidStr);
	params[0].buffer_type = MYSQL_TYPE_VAR_STRING;
	params[0].buffer = uuidStr;
	params[0].buffer_length = CASS_UUID_STRING_LENGTH;
	params[1].buffer_type = MYSQL_TYPE_LONG;
	params[1].buffer = &limit;
	params[1].is_unsigned = 0;

	if (mysql_stmt_bind_param(stmt, params))
		panic(stmt, "Failed to bind params in statement \"getStationsWithNormalsNearby\"");
	if (mysql_stmt_execute(stmt))
		panic(stmt, "Failed to execute statement \"getStationsWithNormalsNearby\"");

	constexpr int NB_PARAMS = 5;
	int neighborId;
	char name[STRING_SIZE];
	double latitude;
	double longitude;
	double distance;
	my_bool isNull[NB_PARAMS];
	size_t length[NB_PARAMS];
	my_bool error[NB_PARAMS];

	MYSQL_BIND result[NB_PARAMS];
	std::memset(result, 0, NB_PARAMS * sizeof(MYSQL_BIND));
	// id (INT)
	result[0].buffer_type = MYSQL_TYPE_LONG;
	result[0].buffer = &neighborId;
	result[0].is_null = &isNull[0];
	result[0].is_unsigned = false;
	result[0].length = &length[0];
	result[0].error = &error[0];
	// name (VARCHAR(STRING_SIZE))
	result[1].buffer_type = MYSQL_TYPE_VAR_STRING;
	result[1].buffer = name;
	result[1].buffer_length = STRING_SIZE;
	result[1].is_null = &isNull[1];
	result[1].length = &length[1];
	result[1].error = &error[1];
	// latitude (DOUBLE)
	result[2].buffer_type = MYSQL_TYPE_DOUBLE;
	result[2].buffer = &latitude;
	result[2].is_null = &isNull[2];
	result[2].length = &length[2];
	result[2].error = &error[2];
	// longitude (DOUBLE)
	result[3].buffer_type = MYSQL_TYPE_DOUBLE;
	result[3].buffer = &longitude;
	result[3].is_null = &isNull[3];
	result[3].length = &length[3];
	result[3].error = &error[3];
	// distance (DOUBLE)
	result[4].buffer_type = MYSQL_TYPE_DOUBLE;
	result[4].buffer = &distance;
	result[4].is_null = &isNull[4];
	result[4].length = &length[4];
	result[4].error = &error[4];

	if (mysql_stmt_bind_result(stmt, result))
		panic(stmt, "Cannot bind result in statement \"getStationsWithNormalsNearby\"");
	if (mysql_stmt_store_result(stmt))
		panic(stmt, "Cannot fetch the result\n");

	std::vector<Neighbor> neighbors;
	for (;;) {
		auto status = mysql_stmt_fetch(stmt);

		if (status == MYSQL_NO_DATA) {
			break;
		} else if (status == MYSQL_DATA_TRUNCATED) {
			// XXX swallowed
		} else if (status != 0) {
			panic(stmt, "Fetching the next row failed");
		}

		Neighbor st;
		if (isNull[0] || error[0])
			continue;
		st.id = neighborId;
		if (!isNull[1] && length[1])
			st.name = std::string(name, length[1]);
		st.latitude = latitude;
		st.longitude = longitude;
		st.distance = distance;

		neighbors.push_back(std::move(st));
	}
	std::cerr << "Found " << neighbors.size() << " neighbours" << std::endl;

	if (mysql_stmt_free_result(stmt))
		panic("Cannot free result of statement \"getStationsWithNormalsNearby\"");

	return neighbors;
}

void DbConnectionNormals::getMonthNormals(int id, Values& normals, date::month month)
{
	doGetNormals(id, normals, unsigned(month));
}

void DbConnectionNormals::getYearNormals(int id, Values& normals)
{
	doGetNormals(id, normals, 0);
}

void DbConnectionNormals::doGetNormals(int id, Values& normals, unsigned period)
{
	MYSQL_STMT* stmt = _getNormalsStmt.get();

	MYSQL_BIND params[2];
	std::memset(params, 0, 2 * sizeof(MYSQL_BIND));
	params[0].buffer_type = MYSQL_TYPE_LONG;
	params[0].buffer = &id;
	params[0].is_unsigned = 0;
	params[1].buffer_type = MYSQL_TYPE_LONG;
	params[1].buffer = &period;
	params[1].is_unsigned = 1;

	if (mysql_stmt_bind_param(stmt, params))
		panic(stmt, "Failed to bind params in statement \"getNormals\"");
	if (mysql_stmt_execute(stmt))
		panic(stmt, "Failed to execute statement \"getNormals\"");

	constexpr int NB_PARAMS = 27;
	double nbDaysWithSnow;
	double nbDaysWithHail;
	double nbDaysWithStorm;
	double nbDaysWithFog;
	double nbDaysGustOver28;
	double nbDaysGustOver16;
	double windSpeed;
	double etp;
	double nbDaysInsolationTimeOver80;
	double nbDaysInsolationTimeUnder20;
	double nbDaysInsolationTimeAt0;
	double insolationTime;
	double globalIrradiance;
	double dju;
	double nbDaysRainfallOver10;
	double nbDaysRainfallOver5;
	double nbDaysRainfallOver1;
	double rainfall;
	double nbDaysTnUnderMinus10;
	double nbDaysTnUnderMinus5;
	double nbDaysTnUnder0;
	double nbDaysTxUnder0;
	double nbDaysTxOver25;
	double nbDaysTxOver30;
	double tn;
	double tm;
	double tx;
	my_bool isNull[NB_PARAMS];
	size_t length[NB_PARAMS];
	my_bool error[NB_PARAMS];

	MYSQL_BIND result[NB_PARAMS];
	std::memset(result, 0, NB_PARAMS * sizeof(MYSQL_BIND));
	// nbDaysWithSnow
	result[0].buffer_type = MYSQL_TYPE_DOUBLE;
	result[0].buffer = &nbDaysWithSnow;
	result[0].is_null = &isNull[0];
	result[0].length = &length[0];
	result[0].error = &error[0];
	// nbDaysWithHail
	result[1].buffer_type = MYSQL_TYPE_DOUBLE;
	result[1].buffer = &nbDaysWithHail;
	result[1].is_null = &isNull[1];
	result[1].length = &length[1];
	result[1].error = &error[1];
	// nbDaysWithStorm
	result[2].buffer_type = MYSQL_TYPE_DOUBLE;
	result[2].buffer = &nbDaysWithStorm;
	result[2].is_null = &isNull[2];
	result[2].length = &length[2];
	result[2].error = &error[2];
	// nbDaysWithFog
	result[3].buffer_type = MYSQL_TYPE_DOUBLE;
	result[3].buffer = &nbDaysWithFog;
	result[3].is_null = &isNull[3];
	result[3].length = &length[3];
	result[3].error = &error[3];
	// nbDaysGustOver28
	result[4].buffer_type = MYSQL_TYPE_DOUBLE;
	result[4].buffer = &nbDaysGustOver28;
	result[4].is_null = &isNull[4];
	result[4].length = &length[4];
	result[4].error = &error[4];
	// nbDaysGustOver16
	result[5].buffer_type = MYSQL_TYPE_DOUBLE;
	result[5].buffer = &nbDaysGustOver16;
	result[5].is_null = &isNull[5];
	result[5].length = &length[5];
	result[5].error = &error[5];
	// windSpeed
	result[6].buffer_type = MYSQL_TYPE_DOUBLE;
	result[6].buffer = &windSpeed;
	result[6].is_null = &isNull[6];
	result[6].length = &length[6];
	result[6].error = &error[6];
	// etp
	result[7].buffer_type = MYSQL_TYPE_DOUBLE;
	result[7].buffer = &etp;
	result[7].is_null = &isNull[7];
	result[7].length = &length[7];
	result[7].error = &error[7];
	// nbDaysInsolationTimeOver80
	result[8].buffer_type = MYSQL_TYPE_DOUBLE;
	result[8].buffer = &nbDaysInsolationTimeOver80;
	result[8].is_null = &isNull[8];
	result[8].length = &length[8];
	result[8].error = &error[8];
	// nbDaysInsolationTimeUnder20
	result[9].buffer_type = MYSQL_TYPE_DOUBLE;
	result[9].buffer = &nbDaysInsolationTimeUnder20;
	result[9].is_null = &isNull[9];
	result[9].length = &length[9];
	result[9].error = &error[9];
	// nbDaysInsolationTimeAt0
	result[10].buffer_type = MYSQL_TYPE_DOUBLE;
	result[10].buffer = &nbDaysInsolationTimeAt0;
	result[10].is_null = &isNull[10];
	result[10].length = &length[10];
	result[10].error = &error[10];
	// insolationTime
	result[11].buffer_type = MYSQL_TYPE_DOUBLE;
	result[11].buffer = &insolationTime;
	result[11].is_null = &isNull[11];
	result[11].length = &length[11];
	result[11].error = &error[11];
	// globalIrradiance
	result[12].buffer_type = MYSQL_TYPE_DOUBLE;
	result[12].buffer = &globalIrradiance;
	result[12].is_null = &isNull[12];
	result[12].length = &length[12];
	result[12].error = &error[12];
	// dju
	result[13].buffer_type = MYSQL_TYPE_DOUBLE;
	result[13].buffer = &dju;
	result[13].is_null = &isNull[13];
	result[13].length = &length[13];
	result[13].error = &error[13];
	// nbDaysRainfallOver10
	result[14].buffer_type = MYSQL_TYPE_DOUBLE;
	result[14].buffer = &nbDaysRainfallOver10;
	result[14].is_null = &isNull[14];
	result[14].length = &length[14];
	result[14].error = &error[14];
	// nbDaysRainfallOver5
	result[15].buffer_type = MYSQL_TYPE_DOUBLE;
	result[15].buffer = &nbDaysRainfallOver5;
	result[15].is_null = &isNull[15];
	result[15].length = &length[15];
	result[15].error = &error[15];
	// nbDaysRainfallOver1
	result[16].buffer_type = MYSQL_TYPE_DOUBLE;
	result[16].buffer = &nbDaysRainfallOver1;
	result[16].is_null = &isNull[16];
	result[16].length = &length[16];
	result[16].error = &error[16];
	// rainfall
	result[17].buffer_type = MYSQL_TYPE_DOUBLE;
	result[17].buffer = &rainfall;
	result[17].is_null = &isNull[17];
	result[17].length = &length[17];
	result[17].error = &error[17];
	// nbDaysTnUnderMinus10
	result[18].buffer_type = MYSQL_TYPE_DOUBLE;
	result[18].buffer = &nbDaysTnUnderMinus10;
	result[18].is_null = &isNull[18];
	result[18].length = &length[18];
	result[18].error = &error[18];
	// nbDaysTnUnderMinus5
	result[19].buffer_type = MYSQL_TYPE_DOUBLE;
	result[19].buffer = &nbDaysTnUnderMinus5;
	result[19].is_null = &isNull[19];
	result[19].length = &length[19];
	result[19].error = &error[19];
	// nbDaysTnUnder0
	result[20].buffer_type = MYSQL_TYPE_DOUBLE;
	result[20].buffer = &nbDaysTnUnder0;
	result[20].is_null = &isNull[20];
	result[20].length = &length[20];
	result[20].error = &error[20];
	// nbDaysTxUnder0
	result[21].buffer_type = MYSQL_TYPE_DOUBLE;
	result[21].buffer = &nbDaysTxUnder0;
	result[21].is_null = &isNull[21];
	result[21].length = &length[21];
	result[21].error = &error[21];
	// nbDaysTxOver25
	result[22].buffer_type = MYSQL_TYPE_DOUBLE;
	result[22].buffer = &nbDaysTxOver25;
	result[22].is_null = &isNull[22];
	result[22].length = &length[22];
	result[22].error = &error[22];
	// nbDaysTxOver30
	result[23].buffer_type = MYSQL_TYPE_DOUBLE;
	result[23].buffer = &nbDaysTxOver30;
	result[23].is_null = &isNull[23];
	result[23].length = &length[23];
	result[23].error = &error[23];
	// tn
	result[24].buffer_type = MYSQL_TYPE_DOUBLE;
	result[24].buffer = &tn;
	result[24].is_null = &isNull[24];
	result[24].length = &length[24];
	result[24].error = &error[24];
	// tm
	result[25].buffer_type = MYSQL_TYPE_DOUBLE;
	result[25].buffer = &tm;
	result[25].is_null = &isNull[25];
	result[25].length = &length[25];
	result[25].error = &error[25];
	// tx
	result[26].buffer_type = MYSQL_TYPE_DOUBLE;
	result[26].buffer = &tx;
	result[26].is_null = &isNull[26];
	result[26].length = &length[26];
	result[26].error = &error[26];

	if (mysql_stmt_bind_result(stmt, result))
		panic(stmt, "Cannot bind result in statement \"getNormals\"");
	if (mysql_stmt_store_result(stmt))
		panic(stmt, "Cannot fetch the result\n");

	auto status = mysql_stmt_fetch(stmt);

	if (status == MYSQL_NO_DATA) {
		return;
	} else if (status == MYSQL_DATA_TRUNCATED) {
		// XXX swallowed
	} else if (status != 0) {
		panic(stmt, "Fetching the next row failed");
	}

	if (!isNull[0])
		normals.nbDaysWithSnow = { true, nbDaysWithSnow };
	if (!isNull[1])
		normals.nbDaysWithHail = { true, nbDaysWithHail };
	if (!isNull[2])
		normals.nbDaysWithStorm = { true, nbDaysWithStorm };
	if (!isNull[3])
		normals.nbDaysWithFog = { true, nbDaysWithFog };
	if (!isNull[4])
		normals.nbDaysGustOver28 = { true, nbDaysGustOver28 };
	if (!isNull[5])
		normals.nbDaysGustOver16 = { true, nbDaysGustOver16 };
	if (!isNull[6])
		normals.windSpeed = { true, windSpeed };
	if (!isNull[7])
		normals.etp = { true, etp };
	if (!isNull[8])
		normals.nbDaysInsolationTimeOver80 = { true, nbDaysInsolationTimeOver80 };
	if (!isNull[9])
		normals.nbDaysInsolationTimeUnder20 = { true, nbDaysInsolationTimeUnder20 };
	if (!isNull[10])
		normals.nbDaysInsolationTimeAt0 = { true, nbDaysInsolationTimeAt0 };
	if (!isNull[11])
		normals.insolationTime = { true, insolationTime };
	if (!isNull[12])
		normals.globalIrradiance = { true, globalIrradiance };
	if (!isNull[13])
		normals.dju = { true, dju };
	if (!isNull[14])
		normals.nbDaysRainfallOver10 = { true, nbDaysRainfallOver10 };
	if (!isNull[15])
		normals.nbDaysRainfallOver5 = { true, nbDaysRainfallOver5 };
	if (!isNull[16])
		normals.nbDaysRainfallOver1 = { true, nbDaysRainfallOver1 };
	if (!isNull[17])
		normals.rainfall = { true, rainfall };
	if (!isNull[18])
		normals.nbDaysTnUnderMinus10 = { true, nbDaysTnUnderMinus10 };
	if (!isNull[19])
		normals.nbDaysTnUnderMinus5 = { true, nbDaysTnUnderMinus5 };
	if (!isNull[20])
		normals.nbDaysTnUnder0 = { true, nbDaysTnUnder0 };
	if (!isNull[21])
		normals.nbDaysTxUnder0 = { true, nbDaysTxUnder0 };
	if (!isNull[22])
		normals.nbDaysTxOver25 = { true, nbDaysTxOver25 };
	if (!isNull[23])
		normals.nbDaysTxOver30 = { true, nbDaysTxOver30 };
	if (!isNull[24])
		normals.tn = { true, tn };
	if (!isNull[25])
		normals.tm = { true, tm };
	if (!isNull[26])
		normals.tx = { true, tx };

	if (mysql_stmt_free_result(stmt))
		panic("Cannot free result of statement \"getNormals\"");
}

}
