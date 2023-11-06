//
// Created by lgeorget on 11/11/2021.
//

#ifndef BUILD_FILTER_H
#define BUILD_FILTER_H


/**
 * This is very crude for the moment, we could use the information from the
 * database instead but for now, we can use constants to make it easy
 */
struct Filter {
    static constexpr float MIN_AIR_TEMPERATURE = -20;
    static constexpr float MAX_AIR_TEMPERATURE = 60;
    static constexpr float MIN_SOIL_TEMPERATURE = -30;
    static constexpr float MAX_SOIL_TEMPERATURE = 30;
    static constexpr int MIN_HUMIDITY = 0;
    static constexpr int MAX_HUMIDITY = 100;
    static constexpr float MIN_WIND_SPEED = 0;
    static constexpr float MAX_WIND_SPEED = 160;
    static constexpr float MIN_WINDGUST_SPEED = 0;
    static constexpr float MAX_WINDGUST_SPEED = 250;
    static constexpr int MIN_WINDDIR = 0;
    static constexpr int MAX_WINDDIR = 360;
    static constexpr float MIN_RAINFALL = 0;
    static constexpr float MAX_RAINFALL = 300;
    static constexpr float MIN_RAINRATE = 0;
    static constexpr float MAX_RAINRATE = 1500;
    static constexpr int MIN_SOLARRAD = 0;
    static constexpr int MAX_SOLARRAD = 1500;
    static constexpr float MIN_BAROMETER = 900;
    static constexpr float MAX_BAROMETER = 1100;
    static constexpr int MIN_LEAFWETNESS = 0;
    static constexpr int MAX_LEAFWETNESS = 15;
    static constexpr float MIN_ET = 0;
    static constexpr float MAX_ET = 10;
    static constexpr int MIN_SOILMOISTURE = 0;
    static constexpr int MAX_SOILMOISTURE = 200;
    static constexpr float MIN_UV = 0;
    static constexpr float MAX_UV = 140;
    static constexpr float MIN_PERCENTAGE_SOIL_MOISTURE = 0;
    static constexpr float MAX_PERCENTAGE_SOIL_MOISTURE = 100;
};


#endif //BUILD_FILTER_H
