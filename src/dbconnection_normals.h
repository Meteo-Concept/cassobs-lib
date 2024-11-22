/**
 * @file dbconnection_normals.h
 * @brief Definition of the DbConnectionNormals class
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

#ifndef DBCONNECTION_NORMALS_H
#define DBCONNECTION_NORMALS_H

#include <functional>
#include <tuple>
#include <memory>
#include <vector>
#include <utility>
#include <map>

#include <mysql.h>
#include <date/date.h>

namespace meteodata {

/**
 * @brief A handle to the database to insert meteorological records
 *
 * An instance of this class is to be used by the periodic script
 * that maintains the list of meteorological records by station.
 */
class DbConnectionNormals
{
public:
	/**
	 * @brief Construct a connection to the database
	 *
	 * @param address the host of the database
	 * @param user the username to use
	 * @param password the password corresponding to the username
	 * @param database the password corresponding to the username
	 */
	DbConnectionNormals(const std::string& address = "127.0.0.1", const std::string& user = "", const std::string& password = "", const std::string& database = "observations2020");
	/**
	 * @brief Close the connection and destroy the database handle
	 */
	virtual ~DbConnectionNormals() = default;

	struct Values
	{
		std::pair<bool, float> nbDaysWithSnow = { false, .0f };
		std::pair<bool, float> nbDaysWithHail = { false, .0f };
		std::pair<bool, float> nbDaysWithStorm = { false, .0f };
		std::pair<bool, float> nbDaysWithFog = { false, .0f };

		std::pair<bool, float> nbDaysGustOver28 = { false, .0f };
		std::pair<bool, float> nbDaysGustOver16 = { false, .0f };

		std::pair<bool, float> windSpeed = { false, .0f };

		std::pair<bool, float> etp = { false, .0f };

		std::pair<bool, float> nbDaysInsolationTimeOver80 = { false, .0f };
		std::pair<bool, float> nbDaysInsolationTimeUnder20 = { false, .0f };
		std::pair<bool, float> nbDaysInsolationTimeAt0 = { false, .0f };
		std::pair<bool, float> insolationTime = { false, .0f };
		std::pair<bool, float> globalIrradiance = { false, .0f };

		std::pair<bool, float> dju = { false, .0f };

		std::pair<bool, float> nbDaysRainfallOver10 = { false, .0f };
		std::pair<bool, float> nbDaysRainfallOver5 = { false, .0f };
		std::pair<bool, float> nbDaysRainfallOver1 = { false, .0f };
		std::pair<bool, float> rainfall = { false, .0f };

		std::pair<bool, float> nbDaysTnUnderMinus10 = { false, .0f };
		std::pair<bool, float> nbDaysTnUnderMinus5 = { false, .0f };
		std::pair<bool, float> nbDaysTnUnder0 = { false, .0f };
		std::pair<bool, float> nbDaysTxUnder0 = { false, .0f };
		std::pair<bool, float> nbDaysTxOver25 = { false, .0f };
		std::pair<bool, float> nbDaysTxOver30 = { false, .0f };

		std::pair<bool, float> tn = { false, .0f };
		std::pair<bool, float> tm = { false, .0f };
		std::pair<bool, float> tx = { false, .0f };
	};

	struct Neighbor {
		int id;
		std::string name;
		double latitude;
		double longitude;
		double distance;
	};

	//bool hasStationWithNormalsNearby(const CassUuid& station, float& distance);

	std::vector<Neighbor> getStationsWithNormalsNearby(const CassUuid& uuid);
	void getMonthNormals(int id, Values& normals, date::month month);
	void getYearNormals(int id, Values& normals);

private:
	void doGetNormals(int id, Values& normals, unsigned period);

	std::unique_ptr<MYSQL, decltype(&mysql_close)> _db;

	static constexpr char GET_STATIONS_WITH_NORMALS_NEARBY[] =
		"SELECT s2.id,s2.name,s2.latitude,s2.longitude,"
			"SQRT(POW((s1.latitude - s2.latitude) * 110, 2) + POW(((s1.longitude - s2.longitude) * 110) * COS(s1.latitude * 3.14159 / 180.0), 2)) AS distance"
			" FROM stations AS s1,stations_with_normals AS s2 "
			" WHERE s1.uuid = ? AND "
				" s2.longitude > s1.longitude - 2 AND "
				" s2.longitude < s1.longitude + 2 AND "
				" s2.latitude > s1.latitude - 2 AND "
				" s2.latitude < s1.latitude + 2 "
			" ORDER BY distance LIMIT ?";
	std::unique_ptr<MYSQL_STMT, decltype(&mysql_stmt_close)> _getStationsWithNormalsNearbyStmt;

	static constexpr char GET_NORMALS[] =
		"SELECT "
			"nb_days_with_snow,"
			"nb_days_with_hail,"
			"nb_days_with_storm,"
			"nb_days_with_fog,"
			"nb_days_gust_over28,"
			"nb_days_gust_over16,"
			"wind_speed,"
			"etp,"
			"nb_days_insolation_over80,"
			"nb_days_insolation_under20,"
			"nb_days_insolation_at0,"
			"insolation_time,"
			"global_irradiance,"
			"dju,"
			"nb_days_rr_over10,"
			"nb_days_rr_over5,"
			"nb_days_rr_over1,"
			"total_rainfall,"
			"nb_days_tn_under_minus10,"
			"nb_days_tn_under_minus5,"
			"nb_days_tn_under0,"
			"nb_days_tx_under0,"
			"nb_days_tx_over25,"
			"nb_days_tx_over30,"
			"tn,"
			"tm,"
			"tx "
		" FROM monthly_normals "
		" WHERE station_id = ? AND month = ?";
	std::unique_ptr<MYSQL_STMT, decltype(&mysql_stmt_close)> _getNormalsStmt;

	void prepareStatements();
	void panic(const std::string& msg);
	void panic(MYSQL_STMT* stmt, const std::string& msg);

	static constexpr size_t STRING_SIZE=191;
};

}

#endif
