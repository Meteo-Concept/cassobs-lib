/**
 * @file monthly_records.cpp
 * @brief Implementation of the MonthlyRecords class
 * @author Laurent Georget
 * @date 2019-10-22
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

#include <exception>
#include <stdexcept>
#include <memory>
#include <functional>
#include <algorithm>
#include <numeric>
#include <limits>
#include <chrono>
#include <vector>
#include <set>

#include <date.h>
#include <cassandra.h>

#include "monthly_records.h"

using namespace date;

namespace meteodata {

namespace chrono = std::chrono;
using DayRecord = MonthlyRecords::DayRecord;
using MonthRecord = MonthlyRecords::MonthRecord;


template<>
void MonthlyRecords::bindDayValue<int>(DayRecord record, CassStatement* statement, int column)
{
	auto pos = _changedDayValues.find(record);
	if (pos != _changedDayValues.end() && pos->second) {
		std::unique_ptr<CassCollection, void(*)(CassCollection*)> dateCollection{cass_collection_new(CASS_COLLECTION_TYPE_LIST, _dayDates[record].size()), &cass_collection_free};
		for (const auto& date : _dayDates[record])
			cass_collection_append_uint32(dateCollection.get(), cass_date_from_epoch(chrono::system_clock::to_time_t(date)));
		std::unique_ptr<CassTuple, void(*)(CassTuple*)> t{cass_tuple_new(2), &cass_tuple_free};
		cass_tuple_set_int32(t.get(), 0, int(_dayValues[record]));
		cass_tuple_set_collection(t.get(), 1, dateCollection.get());
		cass_statement_bind_tuple(statement, column, t.get());
	}
}

template<>
void MonthlyRecords::bindDayValue<float>(DayRecord record, CassStatement* statement, int column)
{
	auto pos = _changedDayValues.find(record);
	if (pos != _changedDayValues.end() && pos->second) {
		std::unique_ptr<CassCollection, void(*)(CassCollection*)> dateCollection{cass_collection_new(CASS_COLLECTION_TYPE_LIST, _dayDates[record].size()), &cass_collection_free};
		for (const auto& date : _dayDates[record])
			cass_collection_append_uint32(dateCollection.get(), cass_date_from_epoch(chrono::system_clock::to_time_t(date)));
		std::unique_ptr<CassTuple, void(*)(CassTuple*)> t{cass_tuple_new(2), &cass_tuple_free};
		cass_tuple_set_float(t.get(), 0, _dayValues[record]);
		cass_tuple_set_collection(t.get(), 1, dateCollection.get());
		cass_statement_bind_tuple(statement, column, t.get());
	}
}

template<>
void MonthlyRecords::bindMonthValue<int>(MonthRecord record, CassStatement* statement, int column)
{
	auto pos = _changedMonthValues.find(record);
	if (pos != _changedMonthValues.end() && pos->second) {
		std::unique_ptr<CassCollection, void(*)(CassCollection*)> dateCollection{cass_collection_new(CASS_COLLECTION_TYPE_LIST, _monthDates[record].size()), &cass_collection_free};
		for (const auto& year : _monthDates[record])
			cass_collection_append_int32(dateCollection.get(), year);
		std::unique_ptr<CassTuple, void(*)(CassTuple*)> t{cass_tuple_new(2), &cass_tuple_free};
		cass_tuple_set_int32(t.get(), 0, int(_monthValues[record]));
		cass_tuple_set_collection(t.get(), 1, dateCollection.get());
		cass_statement_bind_tuple(statement, column, t.get());
	}
}

template<>
void MonthlyRecords::bindMonthValue<float>(MonthRecord record, CassStatement* statement, int column)
{
	auto pos = _changedMonthValues.find(record);
	if (pos != _changedMonthValues.end() && pos->second) {
		std::unique_ptr<CassCollection, void(*)(CassCollection*)> dateCollection{cass_collection_new(CASS_COLLECTION_TYPE_LIST, _monthDates[record].size()), &cass_collection_free};
		for (const auto& year : _monthDates[record])
			cass_collection_append_int32(dateCollection.get(), year);
		std::unique_ptr<CassTuple, void(*)(CassTuple*)> t{cass_tuple_new(2), &cass_tuple_free};
		cass_tuple_set_float(t.get(), 0, _monthValues[record]);
		cass_tuple_set_collection(t.get(), 1, dateCollection.get());
		cass_statement_bind_tuple(statement, column, t.get());
	}
}

void MonthlyRecords::populateRecordInsertionQuery(CassStatement* statement, const CassUuid& station)
{
	if (!_recordsComputed)
		prepareRecords();

	// _rawValues is already checked for non-emptiness in the records preparation
	date::month referenceMonth = date::year_month_day(_rawValues.front().day).month();

	int param = 0;
	cass_statement_bind_uuid(statement,  param++, station);
	cass_statement_bind_int32(statement,  param++, unsigned(referenceMonth)); // safe because 1 <= month <= 12
	bindDayValue<float>(DayRecord::OUTSIDE_TEMP_MAX_MAX, statement, param++);
	bindDayValue<float>(DayRecord::OUTSIDE_TEMP_MIN_MIN, statement, param++);
	bindDayValue<float>(DayRecord::OUTSIDE_TEMP_MAX_MIN, statement, param++);
	bindDayValue<float>(DayRecord::OUTSIDE_TEMP_MIN_MAX, statement, param++);
	bindDayValue<float>(DayRecord::OUTSIDE_TEMP_AMPL_MAX, statement, param++);
	bindMonthValue<int>(MonthRecord::OUTSIDE_TEMP_MAX_OVER_30, statement, param++);
	bindMonthValue<int>(MonthRecord::OUTSIDE_TEMP_MAX_OVER_25, statement, param++);
	bindMonthValue<int>(MonthRecord::OUTSIDE_TEMP_MAX_UNDER_0, statement, param++);
	bindMonthValue<int>(MonthRecord::OUTSIDE_TEMP_MIN_UNDER_0, statement, param++);
	bindMonthValue<int>(MonthRecord::OUTSIDE_TEMP_MIN_UNDER_MINUS_5, statement, param++);
	bindMonthValue<int>(MonthRecord::OUTSIDE_TEMP_MIN_UNDER_MINUS_10, statement, param++);
	bindMonthValue<float>(MonthRecord::OUTSIDE_TEMP_AVG_MAX, statement, param++);
	bindMonthValue<float>(MonthRecord::OUTSIDE_TEMP_AVG_MIN, statement, param++);
	bindMonthValue<float>(MonthRecord::OUTSIDE_TEMP_MAX_AVG_MAX, statement, param++);
	bindMonthValue<float>(MonthRecord::OUTSIDE_TEMP_MAX_AVG_MIN, statement, param++);
	bindMonthValue<float>(MonthRecord::OUTSIDE_TEMP_MIN_AVG_MAX, statement, param++);
	bindMonthValue<float>(MonthRecord::OUTSIDE_TEMP_MIN_AVG_MIN, statement, param++);
	bindDayValue<float>(DayRecord::DAYRAIN_MAX, statement, param++);
	bindMonthValue<float>(MonthRecord::MONTHRAIN_MAX, statement, param++);
	bindMonthValue<float>(MonthRecord::MONTHRAIN_MIN, statement, param++);
	bindMonthValue<int>(MonthRecord::DAYRAIN_OVER_1, statement, param++);
	bindMonthValue<int>(MonthRecord::DAYRAIN_OVER_5, statement, param++);
	bindMonthValue<int>(MonthRecord::DAYRAIN_OVER_10, statement, param++);
	bindMonthValue<float>(MonthRecord::MONTHINSOLATION_MAX, statement, param++);
	bindMonthValue<float>(MonthRecord::MONTHINSOLATION_MIN, statement, param++);
	bindMonthValue<int>(MonthRecord::DAYINSOLATION_OVER_1, statement, param++);
	bindMonthValue<int>(MonthRecord::DAYINSOLATION_OVER_5, statement, param++);
	bindMonthValue<int>(MonthRecord::DAYINSOLATION_AT_0, statement, param++);
	bindDayValue<float>(DayRecord::GUST_MAX, statement, param++);
	bindMonthValue<float>(MonthRecord::WINDSPEED_AVG_MAX, statement, param++);
	bindMonthValue<float>(MonthRecord::WINDSPEED_AVG_MIN, statement, param++);
}

void MonthlyRecords::updateRecord(DayRecord record, std::function<bool(float,float)> replacement, float value, const std::set<date::sys_days>& dates) {
	auto dayPos = _dayValues.find(record);
	if (dayPos != _dayValues.end()) {
		if (int((dayPos->second + 0.05) * 10) == int((value + 0.05) * 10)) {
			// compare for equality up to the first decimal, safely (because of
			// floating-point values)
			std::copy(dates.cbegin(), dates.cend(), std::inserter(_dayDates[record], _dayDates[record].end()));
			_changedDayValues[record] = true;
		} else if (replacement(value, dayPos->second)) {
			dayPos->second = value;
			_dayValues[record] = value;
			_dayDates[record] = dates;
			_changedDayValues[record] = true;
		}
	} else {
		_dayValues[record] = value;
		_dayDates[record] = dates;
		_changedDayValues[record] = true;
	}
}

void MonthlyRecords::updateRecord(MonthRecord record, std::function<bool(float,float)> replacement, float value, int year) {
	auto monthPos = _monthValues.find(record);
	if (monthPos != _monthValues.end()) {
		if (int((monthPos->second + 0.05) * 10) == int((value + 0.05) * 10)) {
			_monthDates[record].insert(year);
			_changedMonthValues[record] = true;
		} else if (replacement(value, monthPos->second)) {
			_monthValues[record] = value;
			_monthDates[record] = { year };
			_changedMonthValues[record] = true;
		}
	} else {
		_monthValues[record] = value;
		_monthDates[record] = { year };
		_changedMonthValues[record] = true;
	}
}

void MonthlyRecords::prepareRecords()
{
	// sanity tests
	if (_rawValues.empty()) // don't make me waste my time
		throw std::invalid_argument("Empty dataset");

	auto referenceMonth = date::year_month_day(_rawValues.front().day).month();
	if (referenceMonth != _month) //hmmm...
		// TODO throw the relevant exception
		throw std::invalid_argument("Incorrect dataset");

	auto referenceYear = date::year_month_day(_rawValues.front().day).year();
	if (!std::all_of(_rawValues.cbegin(), _rawValues.cend(),
			[referenceYear, referenceMonth](const DayValues& v) {
				auto ymd = date::year_month_day(v.day);
				return ymd.month() == referenceMonth && ymd.year() == referenceYear;
			}))
		throw std::invalid_argument("Not all days in the dataset are in the same month and year");

	int nbDays = _rawValues.size();
	int referenceNbDays = ((referenceYear/referenceMonth/last).day() - day{0}).count();
	if (referenceNbDays - nbDays > 3)
		throw std::invalid_argument("Too many days are missing");

	prepareTemperatureRecords(referenceYear, referenceNbDays);
	prepareWindRecords(referenceYear, referenceNbDays);
	prepareRainRecords(referenceYear, referenceNbDays);
	prepareSolarRecords(referenceYear, referenceNbDays);
	_recordsComputed = true;
}

void MonthlyRecords::prepareTemperatureRecords(date::year referenceYear, int referenceNbDays) {
	// outside_temp_max and outside_temp_min
	struct {
		int countMax = 0;
		int countMin = 0;
		int countAvg = 0;
		float minmin = std::numeric_limits<float>::max();
		std::set<date::sys_days> minminDates;
		float maxmin = std::numeric_limits<float>::max();
		std::set<date::sys_days> maxminDates;
		float minmax = std::numeric_limits<float>::min();
		std::set<date::sys_days> minmaxDates;
		float maxmax = std::numeric_limits<float>::min();
		std::set<date::sys_days> maxmaxDates;
		float ampl = std::numeric_limits<float>::min();
		std::set<date::sys_days> amplDates;
		int maxOver30 = 0;
		int maxOver25 = 0;
		int maxUnder0 = 0;
		int minUnder0 = 0;
		int minUnderMinus5 = 0;
		int minUnderMinus10 = 0;
		float maxSum = 0;
		float minSum = 0;
		float avgSum = 0;
	} carry;
	auto outsideTempDerived =
		std::accumulate(_rawValues.cbegin(), _rawValues.cend(),
				carry,
				[](auto&& carry, const auto& v) {
					if (v.outsideTemp_max.first) {
						carry.countMax++;
						if (v.outsideTemp_max.second > carry.maxmax) {
							carry.maxmax = v.outsideTemp_max.second;
							carry.maxmaxDates = { v.day };
						} else if (v.outsideTemp_max.second == carry.maxmax) {
							carry.maxmaxDates.insert(v.day);
						}
						if (v.outsideTemp_max.second < carry.maxmin) {
							carry.maxmin = v.outsideTemp_max.second;
							carry.maxminDates = { v.day };
						} else if (v.outsideTemp_max.second == carry.minmin) {
							carry.minminDates.insert(v.day);
						}
						carry.maxOver30 += int(v.outsideTemp_max.second > 30);
						carry.maxOver25 += int(v.outsideTemp_max.second > 25);
						carry.maxUnder0 += int(v.outsideTemp_max.second < 0);
						carry.maxSum += v.outsideTemp_max.second;
					}
					if (v.outsideTemp_min.first) {
						carry.countMin++;
						if (v.outsideTemp_min.second < carry.minmin) {
							carry.minmin = v.outsideTemp_min.second;
							carry.minminDates = { v.day };
						} else if (v.outsideTemp_min.second == carry.minmin) {
							carry.minminDates.insert(v.day);
						}
						if (v.outsideTemp_min.second > carry.minmax) {
							carry.minmax = v.outsideTemp_min.second;
							carry.minmaxDates = { v.day };
						} else if (v.outsideTemp_min.second == carry.minmax) {
							carry.minmaxDates.insert(v.day);
						}
						carry.minUnder0 += int(v.outsideTemp_min.second < 0);
						carry.minUnderMinus5 += int(v.outsideTemp_min.second < -5);
						carry.minUnderMinus10 += int(v.outsideTemp_min.second < -10);
						carry.minSum += v.outsideTemp_min.second;
					}
					if (v.outsideTemp_max.first && v.outsideTemp_min.first) {
						float ampl = v.outsideTemp_max.second - v.outsideTemp_min.second;
						if (ampl > carry.ampl) {
							carry.ampl = ampl;
							carry.amplDates = { v.day };
						} else if (ampl == carry.ampl) {
							carry.amplDates.insert(v.day);
						}
					}
					if (v.outsideTemp_avg.first) {
						carry.countAvg++;
						carry.avgSum += v.outsideTemp_avg.second;
					}

					return carry;
				});

	updateRecord(DayRecord::OUTSIDE_TEMP_MAX_MIN, std::less<>(),
		outsideTempDerived.maxmin, outsideTempDerived.maxminDates
	);
	updateRecord(DayRecord::OUTSIDE_TEMP_MAX_MAX, std::greater<>(),
		outsideTempDerived.maxmax, outsideTempDerived.maxmaxDates
	);

	updateRecord(MonthRecord::OUTSIDE_TEMP_MAX_OVER_30, std::greater<>(),
		outsideTempDerived.maxOver30, int(referenceYear)
	);
	updateRecord(MonthRecord::OUTSIDE_TEMP_MAX_OVER_25, std::greater<>(),
		outsideTempDerived.maxOver25, int(referenceYear)
	);
	updateRecord(MonthRecord::OUTSIDE_TEMP_MAX_UNDER_0, std::greater<>(),
		outsideTempDerived.maxUnder0, int(referenceYear)
	);

	if (referenceNbDays - outsideTempDerived.countMax <= 3) {
		updateRecord(MonthRecord::OUTSIDE_TEMP_MAX_AVG_MAX, std::greater<>(),
			outsideTempDerived.maxSum / outsideTempDerived.countMax, int(referenceYear)
		);
		updateRecord(MonthRecord::OUTSIDE_TEMP_MAX_AVG_MIN, std::less<>(),
			outsideTempDerived.maxSum / outsideTempDerived.countMax, int(referenceYear)
		);
	}

	updateRecord(DayRecord::OUTSIDE_TEMP_MIN_MIN, std::less<>(),
		outsideTempDerived.minmin, outsideTempDerived.minminDates
	);
	updateRecord(DayRecord::OUTSIDE_TEMP_MIN_MAX, std::greater<>(),
		outsideTempDerived.minmax, outsideTempDerived.minmaxDates
	);

	updateRecord(MonthRecord::OUTSIDE_TEMP_MIN_UNDER_0, std::greater<>(),
		outsideTempDerived.minUnder0, int(referenceYear)
	);
	updateRecord(MonthRecord::OUTSIDE_TEMP_MIN_UNDER_MINUS_5, std::greater<>(),
		outsideTempDerived.minUnderMinus5, int(referenceYear)
	);
	updateRecord(MonthRecord::OUTSIDE_TEMP_MIN_UNDER_MINUS_10, std::greater<>(),
		outsideTempDerived.minUnderMinus10, int(referenceYear)
	);

	if (referenceNbDays - outsideTempDerived.countMin <= 3) {
		updateRecord(MonthRecord::OUTSIDE_TEMP_MIN_AVG_MAX, std::greater<>(),
			outsideTempDerived.minSum / outsideTempDerived.countMin, int(referenceYear)
		);
		updateRecord(MonthRecord::OUTSIDE_TEMP_MIN_AVG_MIN, std::less<>(),
			outsideTempDerived.minSum / outsideTempDerived.countMin, int(referenceYear)
		);
	}

	updateRecord(DayRecord::OUTSIDE_TEMP_AMPL_MAX, std::greater<>(),
		outsideTempDerived.ampl, outsideTempDerived.amplDates
	);

	if (referenceNbDays - outsideTempDerived.countAvg <= 3) {
		updateRecord(MonthRecord::OUTSIDE_TEMP_AVG_MAX, std::greater<>(),
			outsideTempDerived.avgSum / outsideTempDerived.countAvg, int(referenceYear)
		);
		updateRecord(MonthRecord::OUTSIDE_TEMP_AVG_MIN, std::less<>(),
			outsideTempDerived.avgSum / outsideTempDerived.countAvg, int(referenceYear)
		);
	}
}

void MonthlyRecords::prepareWindRecords(date::year referenceYear, int referenceNbDays) {
	struct {
		int countGust = 0;
		int countSpeed = 0;
		float sum = 0.f;
		float gust = std::numeric_limits<float>::min();
		std::set<date::sys_days> gustDates;
	} carry;
	auto windDerived =
		std::accumulate(_rawValues.cbegin(), _rawValues.cend(),
				carry,
				[](auto&& carry, const auto& v) {
					if (v.windSpeed_avg.first) {
						carry.countSpeed++;
						carry.sum += v.windSpeed_avg.second;
					}
					if (v.windGust_max.first) {
						carry.countGust++;
						if (v.windGust_max.second > carry.gust) {
							carry.gust = v.windGust_max.second;
							carry.gustDates = { v.day };
						} else if (v.windGust_max.second == carry.gust) {
							carry.gustDates.insert(v.day);
						}
					}

					return carry;
				});

	updateRecord(DayRecord::GUST_MAX, std::greater<>(),
		windDerived.gust, windDerived.gustDates
	);

	if (referenceNbDays - windDerived.countSpeed <= 3) {
		updateRecord(MonthRecord::WINDSPEED_AVG_MAX, std::greater<>(),
			windDerived.sum / windDerived.countSpeed, int(referenceYear)
		);
		updateRecord(MonthRecord::WINDSPEED_AVG_MIN, std::less<>(),
			windDerived.sum / windDerived.countSpeed, int(referenceYear)
		);
	}
}

void MonthlyRecords::prepareRainRecords(date::year referenceYear, int referenceNbDays) {
	struct {
		int countRain = 0;
		int over1 = 0;
		int over5 = 0;
		int over10 = 0;
		float sum = 0;
		float max = std::numeric_limits<float>::min();
		std::set<date::sys_days> maxDates;
	} carry;
	auto rainDerived =
		std::accumulate(_rawValues.cbegin(), _rawValues.cend(),
				carry,
				[](auto&& carry, const auto& v) {
					if (v.dayrain.first) {
						carry.countRain++;
						if (v.dayrain.second > 1)
							carry.over1++;
						if (v.dayrain.second > 5)
							carry.over5++;
						if (v.dayrain.second > 10)
							carry.over10++;
						carry.sum += v.dayrain.second;
						if (v.dayrain.second > carry.max) {
							carry.max = v.dayrain.second;
							carry.maxDates = { v.day };
						} else if (v.dayrain.second == carry.max) {
							carry.maxDates.insert(v.day);
						}
					}

					return carry;
				});

	updateRecord(DayRecord::DAYRAIN_MAX, std::greater<>(),
		rainDerived.max, rainDerived.maxDates
	);

	updateRecord(MonthRecord::DAYRAIN_OVER_1, std::greater<>(),
		rainDerived.over1, int(referenceYear)
	);
	updateRecord(MonthRecord::DAYRAIN_OVER_5, std::greater<>(),
		rainDerived.over5, int(referenceYear)
	);
	updateRecord(MonthRecord::DAYRAIN_OVER_10, std::greater<>(),
		rainDerived.over10, int(referenceYear)
	);

	updateRecord(MonthRecord::MONTHRAIN_MAX, std::greater<>(),
		rainDerived.sum, int(referenceYear)
	);

	if (referenceNbDays == rainDerived.countRain)
		updateRecord(MonthRecord::MONTHRAIN_MIN, std::less<>(),
			rainDerived.sum, int(referenceYear)
		);
}

void MonthlyRecords::prepareSolarRecords(date::year referenceYear, int referenceNbDays) {
	struct {
		int countSolar = 0;
		int over1 = 0;
		int over5 = 0;
		int at0 = 0;
		float sum = 0;
	} carry;
	auto solarDerived =
		std::accumulate(_rawValues.cbegin(), _rawValues.cend(),
				carry,
				[](auto&& carry, const auto& v) {
					if (v.insolationTime.first) {
						carry.countSolar++;
						if (v.insolationTime.second > 60)
							carry.over1++;
						if (v.insolationTime.second > 5 * 60)
							carry.over5++;
						if (v.insolationTime.second == 0)
							carry.at0++;
						carry.sum += (v.insolationTime.second / 60);
					}

					return carry;
				});

	updateRecord(MonthRecord::DAYINSOLATION_OVER_1, std::greater<>(),
		solarDerived.over1, int(referenceYear)
	);
	updateRecord(MonthRecord::DAYINSOLATION_OVER_5, std::greater<>(),
		solarDerived.over5, int(referenceYear)
	);
	updateRecord(MonthRecord::DAYINSOLATION_AT_0, std::greater<>(),
		solarDerived.at0, int(referenceYear)
	);

	updateRecord(MonthRecord::MONTHINSOLATION_MAX, std::greater<>(),
		solarDerived.sum, int(referenceYear)
	);

	if (referenceNbDays == solarDerived.countSolar)
		updateRecord(MonthRecord::MONTHINSOLATION_MIN, std::less<>(),
			solarDerived.sum, int(referenceYear)
		);
}

}
