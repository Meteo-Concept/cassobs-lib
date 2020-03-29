lib_LTLIBRARIES = libcassobs.la
include_HEADERS = \
		dbconnection_observations.h\
		dbconnection_common.h\
		dbconnection_minmax.h\
		dbconnection_month_minmax.h\
		dbconnection_normals.h\
		observation.h \
		message.h\
		cassandra_stmt_ptr.h\
		monthly_records.h\
		dbconnection_records.h

libcassobs_la_SOURCES = \
		    dbconnection_common.cpp\
		    dbconnection_common.h\
		    dbconnection_observations.cpp\
		    dbconnection_observations.h\
		    dbconnection_minmax.cpp\
		    dbconnection_minmax.h\
		    dbconnection_month_minmax.cpp\
		    dbconnection_month_minmax.h\
		    dbconnection_records.cpp\
		    dbconnection_records.h\
		    dbconnection_normals.cpp\
		    dbconnection_normals.h\
		    monthly_records.cpp\
		    monthly_records.h\
		    cassandra_stmt_ptr.h\
		    observation.h \
		    observation.cpp \
		    message.h

libcassobs_la_CPPFLAGS = $(PTHREAD_CFLAGS) $(CASSANDRA_CPPFLAGS) $(DATE_CPPFLAGS) $(MYSQL_CPPFLAGS)
libcassobs_la_CXXFLAGS = -std=c++14 -Wall -Wextra -pedantic
libcassobs_la_LIBADD = $(PTHREAD_CFLAGS) $(CASSANDRA_LIBS) $(MYSQL_LIBS)
libcassobs_la_LDFLAGS = -version-info 0:0:0

check_PROGRAMS=get_last_data get_mqtt_stations get_rainfall compute_records get_wlv2_stations get_normals
TESTS=$(check_PROGRAMS)

get_last_data_SOURCES = \
			tests/get_last_data.cpp
get_last_data_CPPFLAGS = $(libcassobs_la_CPPFLAGS)
get_last_data_CXXFLAGS = $(libcassobs_la_CXXFLAGS)
get_last_data_LDADD = libcassobs.la $(libcassobs_la_LIBADD)

get_mqtt_stations_SOURCES = \
			tests/get_mqtt_stations.cpp
get_mqtt_stations_CPPFLAGS = $(libcassobs_la_CPPFLAGS)
get_mqtt_stations_CXXFLAGS = $(libcassobs_la_CXXFLAGS)
get_mqtt_stations_LDADD = libcassobs.la $(libcassobs_la_LIBADD)

get_wlv2_stations_SOURCES = \
			tests/get_wlv2_stations.cpp
get_wlv2_stations_CPPFLAGS = $(libcassobs_la_CPPFLAGS)
get_wlv2_stations_CXXFLAGS = $(libcassobs_la_CXXFLAGS)
get_wlv2_stations_LDADD = libcassobs.la $(libcassobs_la_LIBADD)

get_rainfall_SOURCES = \
			tests/get_rainfall.cpp
get_rainfall_CPPFLAGS = $(libcassobs_la_CPPFLAGS)
get_rainfall_CXXFLAGS = $(libcassobs_la_CXXFLAGS)
get_rainfall_LDADD = libcassobs.la $(libcassobs_la_LIBADD)

get_normals_SOURCES = \
			tests/get_normals.cpp
get_normals_CPPFLAGS = $(libcassobs_la_CPPFLAGS)
get_normals_CXXFLAGS = $(libcassobs_la_CXXFLAGS)
get_normals_LDADD = libcassobs.la $(libcassobs_la_LIBADD)

compute_records_SOURCES = \
		tests/compute_records.cpp
compute_records_CPPFLAGS = $(libcassobs_la_CPPFLAGS)
compute_records_CXXFLAGS = $(libcassobs_la_CXXFLAGS)
compute_records_LDADD = libcassobs.la $(libcassobs_la_LIBADD)
