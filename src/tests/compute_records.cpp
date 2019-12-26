/*
 * Copyright (C) 2019  SAS JD Environnement <contact@meteo-concept.fr>
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

#include <algorithm>
#include <iostream>
#include <iterator>
#include <fstream>
#include <array>
#include <set>

#include <date.h>

#include "../monthly_records.h"


using namespace std::chrono;
using namespace meteodata;
using namespace date;

struct RawValues {
	date::sys_days day;
	float outsideTemp_max;
	float outsideTemp_min;
	float outsideTemp_avg;
	float dayrain;
	float windSpeed_avg;
	float windGust_max;
	int insolationTime;
};

std::array<RawValues, 30> raw = {
	RawValues{ 2019_y/11/1,18.4f,13.1f,15.8f,3.2f,18.8f,50.4f,36 },
	RawValues{ 2019_y/11/2,15.3f,10.4f,12.9f,20.0f,22.1f,72.0f,144 },
	RawValues{ 2019_y/11/3,14.8f,9.9f,12.4f,1.6f,21.6f,64.8f,234 },
	RawValues{ 2019_y/11/4,13.4f,9.3f,11.4f,1.2f,19.6f,82.8f,294 },
	RawValues{ 2019_y/11/5,12.7f,7.3f,10.0f,0.4f,16.5f,54.0f,108 },
	RawValues{ 2019_y/11/6,13.5f,8.5f,11.0f,6.6f,13.0f,32.4f,144 },
	RawValues{ 2019_y/11/7,10.8f,5.4f,8.1f,9.1f,17.0f,57.6f,294 },
	RawValues{ 2019_y/11/8,12.0f,5.9f,8.9f,0.2f,14.3f,39.6f,288 },
	RawValues{ 2019_y/11/9,9.1f,2.5f,5.8f,15.9f,12.3f,50.4f,96 },
	RawValues{ 2019_y/11/10,11.5f,5.7f,8.6f,1.8f,12.2f,36.0f,114 },
	RawValues{ 2019_y/11/11,11.8f,4.6f,8.2f,4.2f,16.4f,72.0f,324 },
	RawValues{ 2019_y/11/12,11.1f,5.0f,8.1f,1.2f,16.8f,50.4f,336 },
	RawValues{ 2019_y/11/13,10.7f,2.9f,6.8f,7.9f,12.8f,64.8f,252 },
	RawValues{ 2019_y/11/14,10.3f,0.4f,5.3f,0.0f,9.0f,25.2f,486 },
	RawValues{ 2019_y/11/15,9.3f,-2.2f,3.6f,16.5f,11.4f,50.4f,144 },
	RawValues{ 2019_y/11/16,9.3f,5.0f,7.2f,0.4f,8.6f,28.8f,90 },
	RawValues{ 2019_y/11/17,6.3f,-0.3f,3.0f,0.0f,4.3f,14.4f,54 },
	RawValues{ 2019_y/11/18,10.8f,2.6f,6.7f,0.0f,6.0f,21.6f,240 },
	RawValues{ 2019_y/11/19,8.4f,-1.3f,3.5f,0.0f,9.0f,36.0f,60 },
	RawValues{ 2019_y/11/20,6.1f,1.1f,3.6f,0.2f,13.7f,32.4f,246 },
	RawValues{ 2019_y/11/21,10.0f,2.7f,6.3f,6.8f,15.1f,43.2f,18 },
	RawValues{ 2019_y/11/22,10.6f,6.1f,8.4f,4.4f,21.3f,57.6f,114 },
	RawValues{ 2019_y/11/23,12.0f,8.0f,10.0f,0.0f,10.1f,43.2f,48 },
	RawValues{ 2019_y/11/24,10.9f,0.9f,5.9f,1.6f,9.6f,39.6f,90 },
	RawValues{ 2019_y/11/25,13.8f,9.1f,11.5f,3.2f,14.6f,43.2f,48 },
	RawValues{ 2019_y/11/26,14.9f,9.9f,12.4f,6.8f,21.0f,50.4f,12 },
	RawValues{ 2019_y/11/27,14.4f,9.0f,11.7f,0.8f,22.2f,64.8f,180 },
	RawValues{ 2019_y/11/28,13.6f,9.4f,11.5f,1.0f,16.0f,46.8f,126 },
	RawValues{ 2019_y/11/29,12.9f,5.4f,9.1f,0.0f,7.8f,21.6f,312 },
	RawValues{ 2019_y/11/30,9.0f,4.5f,6.8f,11.5f,18.4f,54.0f,0 }
};


template<typename T>
void displayRecord(MonthlyRecords& records, const std::string& name, const std::string& unit, T record)
{
	auto rec = records.getRecord(record);
	std::cout << "\n" << name << "\n------------\n"
		<< std::boolalpha
		<< "Record disponible : " << std::get<0>(rec) << "\n";
	if (std::get<0>(rec)) {
		std::cout << "Valeur : " << std::get<1>(rec) << unit << "\n";
		std::cout << "Date(s) : ";
		for (const auto& d : std::get<2>(rec))
			std::cout << d << " ";
		std::cout << std::endl;
	}
}

void setup(MonthlyRecords& records)
{
	records.setMonth(November);
	for (const RawValues& r : raw) {
		records.addDayValues({
				r.day,
				{ true, r.outsideTemp_max },
				{ true, r.outsideTemp_min },
				{ true, r.outsideTemp_avg },
				{ true, r.dayrain },
				{ true, r.windSpeed_avg },
				{ true, r.windGust_max },
				{ true, r.insolationTime }
			});
	}
}

void test1()
{
	MonthlyRecords rennesNov2019;
	setup(rennesNov2019);

	rennesNov2019.prepareRecords();

	std::array<std::tuple<MonthlyRecords::DayRecord, std::string, std::string>, 7> dayRecords{ {
		{ MonthlyRecords::DayRecord::OUTSIDE_TEMP_MAX_MAX, "txx", "°C" },
		{ MonthlyRecords::DayRecord::OUTSIDE_TEMP_MAX_MIN, "txn", "°C" },
		{ MonthlyRecords::DayRecord::OUTSIDE_TEMP_MIN_MAX, "tnx", "°C" },
		{ MonthlyRecords::DayRecord::OUTSIDE_TEMP_MIN_MIN, "tnn", "°C" },
		{ MonthlyRecords::DayRecord::OUTSIDE_TEMP_AMPL_MAX, "Amplitude thermique", "°C" },
		{ MonthlyRecords::DayRecord::DAYRAIN_MAX, "Pluviométrie max", "mm" },
		{ MonthlyRecords::DayRecord::GUST_MAX, "Rafales max", "km/h" }
	} };
	for (const auto& dayRecord : dayRecords)
		displayRecord(rennesNov2019, std::get<1>(dayRecord), std::get<2>(dayRecord), std::get<0>(dayRecord));

	std::array<std::tuple<MonthlyRecords::MonthRecord, std::string, std::string>, 24> monthRecords{ {
		{ MonthlyRecords::MonthRecord::OUTSIDE_TEMP_MAX_OVER_30, "Nb de jours Tx > 30°C", "" },
		{ MonthlyRecords::MonthRecord::OUTSIDE_TEMP_MAX_OVER_25, "Nb de jours Tx > 25°C", "" },
		{ MonthlyRecords::MonthRecord::OUTSIDE_TEMP_MAX_UNDER_0, "Nb de jours Tx < 0°C", "" },
		{ MonthlyRecords::MonthRecord::OUTSIDE_TEMP_MIN_UNDER_0, "Nb de jours Tn < 0°C", "" },
		{ MonthlyRecords::MonthRecord::OUTSIDE_TEMP_MIN_UNDER_MINUS_5, "Nb de jours Tn < -5°C", "" },
		{ MonthlyRecords::MonthRecord::OUTSIDE_TEMP_MIN_UNDER_MINUS_10, "Nb de jours Tn < -10°C", "" },
		{ MonthlyRecords::MonthRecord::OUTSIDE_TEMP_AVG_MAX, "Température moyenne max", "°C" },
		{ MonthlyRecords::MonthRecord::OUTSIDE_TEMP_AVG_MIN, "Température moyenne min", "°C" },
		{ MonthlyRecords::MonthRecord::OUTSIDE_TEMP_MAX_AVG_MAX, "Température maximale moyenne max", "°C" },
		{ MonthlyRecords::MonthRecord::OUTSIDE_TEMP_MAX_AVG_MIN, "Température maximale moyenne min", "°C" },
		{ MonthlyRecords::MonthRecord::OUTSIDE_TEMP_MIN_AVG_MAX, "Température minimale moyenne max", "°C" },
		{ MonthlyRecords::MonthRecord::OUTSIDE_TEMP_MIN_AVG_MIN, "Température minimale moyenne min", "°C" },
		{ MonthlyRecords::MonthRecord::MONTHRAIN_MAX, "Pluviométrie mensuelle max", "mm" },
		{ MonthlyRecords::MonthRecord::MONTHRAIN_MIN, "Pluviométrie mensuelle min", "mm" },
		{ MonthlyRecords::MonthRecord::DAYRAIN_OVER_1, "Nb de jours de pluie > 1mm", "" },
		{ MonthlyRecords::MonthRecord::DAYRAIN_OVER_5, "Nb de jours de pluie > 5mm", "" },
		{ MonthlyRecords::MonthRecord::DAYRAIN_OVER_10, "Nb de jours de pluie > 10mm", "" },
		{ MonthlyRecords::MonthRecord::MONTHINSOLATION_MAX, "Ensoleillement mensuel max", "h" },
		{ MonthlyRecords::MonthRecord::MONTHINSOLATION_MIN, "Ensoleillement mensuel min", "h" },
		{ MonthlyRecords::MonthRecord::DAYINSOLATION_OVER_1, "Nb de jours d'ensoleillement > 1h", "" },
		{ MonthlyRecords::MonthRecord::DAYINSOLATION_OVER_5, "Nb de jours d'ensoleillement > 5h", "" },
		{ MonthlyRecords::MonthRecord::DAYINSOLATION_AT_0, "Nb de jours d'ensoleillement = 0h", "" },
		{ MonthlyRecords::MonthRecord::WINDSPEED_AVG_MAX, "Vent moyen max", "km/h" },
		{ MonthlyRecords::MonthRecord::WINDSPEED_AVG_MIN, "Vent moyen min", "km/h" }
	} };
	for (const auto& monthRecord : monthRecords)
		displayRecord(rennesNov2019, std::get<1>(monthRecord), std::get<2>(monthRecord), std::get<0>(monthRecord));
}

int test2()
{
	MonthlyRecords testRecords;
	setup(testRecords);

	testRecords.setRecord(MonthlyRecords::DayRecord::OUTSIDE_TEMP_MAX_MAX, 18.3f, { 1985_y/November/3, 1994_y/November/13 });
	testRecords.prepareRecords();
	displayRecord(testRecords, "txx", "°C", MonthlyRecords::DayRecord::OUTSIDE_TEMP_MAX_MAX);
	auto rec = testRecords.getRecord(MonthlyRecords::DayRecord::OUTSIDE_TEMP_MAX_MAX);
	if (rec != std::make_tuple(true, 18.4f, std::set<sys_days>{ 2019_y/November/1 }))
		return 1;

	return 0;
}

int test3()
{
	MonthlyRecords testRecords;
	setup(testRecords);

	testRecords.setRecord(MonthlyRecords::DayRecord::OUTSIDE_TEMP_MAX_MAX, 18.4f, { 1985_y/November/3, 1994_y/November/13 });
	testRecords.prepareRecords();
	displayRecord(testRecords, "txx", "°C", MonthlyRecords::DayRecord::OUTSIDE_TEMP_MAX_MAX);
	auto rec = testRecords.getRecord(MonthlyRecords::DayRecord::OUTSIDE_TEMP_MAX_MAX);
	if (rec != std::make_tuple(true, 18.4f, std::set<sys_days>{ 1985_y/November/3, 1994_y/November/13, 2019_y/November/1 }))
		return 2;

	return 0;
}

int test4()
{
	MonthlyRecords testRecords;
	setup(testRecords);

	testRecords.setRecord(MonthlyRecords::DayRecord::OUTSIDE_TEMP_MAX_MAX, 18.5f, { 1985_y/November/3, 1994_y/November/13 });
	testRecords.prepareRecords();
	displayRecord(testRecords, "txx", "°C", MonthlyRecords::DayRecord::OUTSIDE_TEMP_MAX_MAX);
	auto rec = testRecords.getRecord(MonthlyRecords::DayRecord::OUTSIDE_TEMP_MAX_MAX);
	if (rec != std::make_tuple(true, 18.5f, std::set<sys_days>{ 1985_y/November/3, 1994_y/November/13 }))
		return 3;

	return 0;
}

int main()
{
	test1();
	return test2() || test3() || test4();
}
