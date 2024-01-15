lib_LTLIBRARIES = libcassobs2.la
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
		dbconnection_records.h\
		dbconnection_jobs.h

libcassobs2_la_SOURCES = \
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
		    dbconnection_jobs.cpp\
		    dbconnection_jobs.h\
		    monthly_records.cpp\
		    monthly_records.h\
		    cassandra_stmt_ptr.h\
		    observation.h \
		    observation.cpp \
		    message.h

libcassobs2_la_CPPFLAGS = $(PTHREAD_CFLAGS) $(CASSANDRA_CFLAGS) $(DATE_CFLAGS) $(MYSQL_CFLAGS)
libcassobs2_la_CXXFLAGS =
libcassobs2_la_LIBADD = $(PTHREAD_CFLAGS) $(CASSANDRA_LIBS) $(DATE_LIBS) $(MYSQL_LIBS)
libcassobs2_la_LDFLAGS = -version-info 9:1:0

check_PROGRAMS=get_last_data get_mqtt_stations get_rainfall compute_records get_wlv2_stations get_fieldclimate_stations get_normals get_objenious_stations get_liveobjects_stations get_cimel_stations compute_minmax get_jobs
TESTS=$(check_PROGRAMS)

get_last_data_SOURCES = \
			tests/get_last_data.cpp
get_last_data_CPPFLAGS = $(libcassobs2_la_CPPFLAGS)
get_last_data_CXXFLAGS = $(libcassobs2_la_CXXFLAGS)
get_last_data_LDADD = libcassobs2.la $(libcassobs2_la_LIBADD)

get_mqtt_stations_SOURCES = \
			tests/get_mqtt_stations.cpp
get_mqtt_stations_CPPFLAGS = $(libcassobs2_la_CPPFLAGS)
get_mqtt_stations_CXXFLAGS = $(libcassobs2_la_CXXFLAGS)
get_mqtt_stations_LDADD = libcassobs2.la $(libcassobs2_la_LIBADD)

get_wlv2_stations_SOURCES = \
			tests/get_wlv2_stations.cpp
get_wlv2_stations_CPPFLAGS = $(libcassobs2_la_CPPFLAGS)
get_wlv2_stations_CXXFLAGS = $(libcassobs2_la_CXXFLAGS)
get_wlv2_stations_LDADD = libcassobs2.la $(libcassobs2_la_LIBADD)

get_fieldclimate_stations_SOURCES = \
			tests/get_fieldclimate_stations.cpp
get_fieldclimate_stations_CPPFLAGS = $(libcassobs2_la_CPPFLAGS)
get_fieldclimate_stations_CXXFLAGS = $(libcassobs2_la_CXXFLAGS)
get_fieldclimate_stations_LDADD = libcassobs2.la $(libcassobs2_la_LIBADD)

get_rainfall_SOURCES = \
			tests/get_rainfall.cpp
get_rainfall_CPPFLAGS = $(libcassobs2_la_CPPFLAGS)
get_rainfall_CXXFLAGS = $(libcassobs2_la_CXXFLAGS)
get_rainfall_LDADD = libcassobs2.la $(libcassobs2_la_LIBADD)

get_normals_SOURCES = \
			tests/get_normals.cpp
get_normals_CPPFLAGS = $(libcassobs2_la_CPPFLAGS)
get_normals_CXXFLAGS = $(libcassobs2_la_CXXFLAGS)
get_normals_LDADD = libcassobs2.la $(libcassobs2_la_LIBADD)

compute_records_SOURCES = \
		tests/compute_records.cpp
compute_records_CPPFLAGS = $(libcassobs2_la_CPPFLAGS)
compute_records_CXXFLAGS = $(libcassobs2_la_CXXFLAGS)
compute_records_LDADD = libcassobs2.la $(libcassobs2_la_LIBADD)

get_objenious_stations_SOURCES = \
			tests/get_objenious_stations.cpp
get_objenious_stations_CPPFLAGS = $(libcassobs2_la_CPPFLAGS)
get_objenious_stations_CXXFLAGS = $(libcassobs2_la_CXXFLAGS)
get_objenious_stations_LDADD = libcassobs2.la $(libcassobs2_la_LIBADD)

get_liveobjects_stations_SOURCES = \
			tests/get_liveobjects_stations.cpp
get_liveobjects_stations_CPPFLAGS = $(libcassobs2_la_CPPFLAGS)
get_liveobjects_stations_CXXFLAGS = $(libcassobs2_la_CXXFLAGS)
get_liveobjects_stations_LDADD = libcassobs2.la $(libcassobs2_la_LIBADD)

get_cimel_stations_SOURCES = \
			tests/get_cimel_stations.cpp
get_cimel_stations_CPPFLAGS = $(libcassobs2_la_CPPFLAGS)
get_cimel_stations_CXXFLAGS = $(libcassobs2_la_CXXFLAGS)
get_cimel_stations_LDADD = libcassobs2.la $(libcassobs2_la_LIBADD)

compute_minmax_SOURCES = \
			tests/compute_minmax.cpp
compute_minmax_CPPFLAGS = $(libcassobs2_la_CPPFLAGS)
compute_minmax_CXXFLAGS = $(libcassobs2_la_CXXFLAGS)
compute_minmax_LDADD = libcassobs2.la $(libcassobs2_la_LIBADD)

get_jobs_SOURCES = \
	    tests/get_jobs.cpp
get_jobs_CPPFLAGS = $(libcassobs2_la_CPPFLAGS)
get_jobs_CXXFLAGS = $(libcassobs2_la_CXXFLAGS)
get_jobs_LDADD = libcassobs2.la $(libcassobs2_la_LIBADD)
