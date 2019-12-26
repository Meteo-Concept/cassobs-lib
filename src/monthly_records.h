/**
 * @file monthly_records.h
 * @brief Definition of the MonthlyRecords class
 * @author Laurent Georget
 * @date 2019-12-20
 */
/*
 * Copyright (C) 2019  SAS Météo Concept <contact@meteo-concept.fr>
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

#ifndef MONTHLY_RECORDS_H
#define MONTHLY_RECORDS_H

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
 * @brief A datastructure used to compute the meteorological records for one month
 */
class MonthlyRecords
{
public:
	struct DayValues {
		date::sys_days day;
		std::pair<bool, float> outsideTemp_max;
		std::pair<bool, float> outsideTemp_min;
		std::pair<bool, float> outsideTemp_avg;
		std::pair<bool, float> dayrain;
		std::pair<bool, float> windSpeed_avg;
		std::pair<bool, float> windGust_max;
		std::pair<bool, int> insolationTime;
	};

	enum class DayRecord {
		OUTSIDE_TEMP_MAX_MAX,
		OUTSIDE_TEMP_MAX_MIN,
		OUTSIDE_TEMP_MIN_MAX,
		OUTSIDE_TEMP_MIN_MIN,
		OUTSIDE_TEMP_AMPL_MAX,
		DAYRAIN_MAX,
		GUST_MAX,
	};

	enum class MonthRecord {
		OUTSIDE_TEMP_MAX_OVER_30,
		OUTSIDE_TEMP_MAX_OVER_25,
		OUTSIDE_TEMP_MAX_UNDER_0,
		OUTSIDE_TEMP_MIN_UNDER_0,
		OUTSIDE_TEMP_MIN_UNDER_MINUS_5,
		OUTSIDE_TEMP_MIN_UNDER_MINUS_10,
		OUTSIDE_TEMP_AVG_MAX,
		OUTSIDE_TEMP_AVG_MIN,
		OUTSIDE_TEMP_MAX_AVG_MAX,
		OUTSIDE_TEMP_MAX_AVG_MIN,
		OUTSIDE_TEMP_MIN_AVG_MAX,
		OUTSIDE_TEMP_MIN_AVG_MIN,
		MONTHRAIN_MAX,
		MONTHRAIN_MIN,
		DAYRAIN_OVER_1,
		DAYRAIN_OVER_5,
		DAYRAIN_OVER_10,
		MONTHINSOLATION_MAX,
		MONTHINSOLATION_MIN,
		DAYINSOLATION_OVER_1,
		DAYINSOLATION_OVER_5,
		DAYINSOLATION_AT_0,
		WINDSPEED_AVG_MAX,
		WINDSPEED_AVG_MIN
	};

private:
	date::month _month;
	std::vector<DayValues> _rawValues;
	bool _recordsComputed = false;

	std::map<DayRecord, float> _dayValues;
	std::map<DayRecord, std::set<date::sys_days>> _dayDates;
	std::map<DayRecord, bool> _changedDayValues;

	std::map<MonthRecord, float> _monthValues;
	std::map<MonthRecord, std::set<int>> _monthDates;
	std::map<MonthRecord, bool> _changedMonthValues;

	template<typename T>
	void bindDayValue(DayRecord record, CassStatement* statement, int column);
	template<typename T>
	void bindMonthValue(MonthRecord record, CassStatement* statement, int column);

	void updateRecord(DayRecord record, std::function<bool(float,float)> replacement, float value, const std::set<date::sys_days>& date);
	void updateRecord(MonthRecord record, std::function<bool(float,float)> replacement, float value, int year);

	void prepareTemperatureRecords(date::year referenceYear, int referenceNbDays);
	void prepareWindRecords(date::year referenceYear, int referenceNbDays);
	void prepareRainRecords(date::year referenceYear, int referenceNbDays);
	void prepareSolarRecords(date::year referenceYear, int referenceNbDays);

public:
	inline void setMonth(date::month month) { _month = month; }
	inline void addDayValues(DayValues&& dayValues) {
		using namespace date;
		if (date::year_month_day(dayValues.day).month() == _month) {
			_recordsComputed = false;
			_rawValues.emplace_back(dayValues);
		}
	}
	inline void setRecord(DayRecord record, float value, std::set<date::sys_days>&& dates) {
		_recordsComputed = false;
		_dayValues.emplace(record, value);
		_dayDates.emplace(record, std::forward<std::set<date::sys_days>>(dates));
		_changedDayValues[record] = false;
	}
	inline void setRecord(MonthRecord record, float value, std::set<int>&& years) {
		_recordsComputed = false;
		_monthValues.emplace(record, value);
		_monthDates.emplace(record, std::forward<std::set<int>>(years));
		_changedMonthValues[record] = false;
	}

	void prepareRecords();
	void populateRecordInsertionQuery(CassStatement* statement, const CassUuid& station);
	inline std::tuple<bool, float, std::set<date::sys_days>> getRecord(DayRecord record) {
		if (!_recordsComputed)
			prepareRecords();

		auto pos = _dayValues.find(record);
		if (pos == _dayValues.end())
			return { false, 0.f, {} };

		return { true, pos->second, _dayDates[record] };
	}
	inline std::tuple<bool, float, std::set<int>> getRecord(MonthRecord record) {
		if (!_recordsComputed)
			prepareRecords();

		auto pos = _monthValues.find(record);
		if (pos == _monthValues.end())
			return { false, 0.f, {} };

		return { true, pos->second, _monthDates[record] };
	}
};

}

#endif
