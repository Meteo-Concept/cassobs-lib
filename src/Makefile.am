lib_LTLIBRARIES = libcassobs2.la
pkginclude_HEADERS = \
		dbconnection_observations.h\
		dbconnection_common.h\
		dbconnection_minmax.h\
		dbconnection_month_minmax.h\
		dbconnection_normals.h\
		observation.h \
		map_observation.h \
		message.h\
		cassandra_stmt_ptr.h\
		monthly_records.h\
		dbconnection_records.h\
		dbconnection_jobs.h\
		virtual_station.h\
		nbiot_station.h\
		modem_station_configuration.h\
		download.h

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
		    map_observation.h \
		    message.h\
		    virtual_station.h\
		    nbiot_station.h\
		    modem_station_configuration.h\
		    download.h

libcassobs2_la_CPPFLAGS = $(PTHREAD_CFLAGS) $(CASSANDRA_CFLAGS) $(DATE_CFLAGS) $(MYSQL_CFLAGS) $(POSTGRES_CFLAGS)
libcassobs2_la_CXXFLAGS =
libcassobs2_la_LIBADD = $(PTHREAD_LIBS) $(CASSANDRA_LIBS) $(DATE_LIBS) $(MYSQL_LIBS) $(POSTGRES_LIBS)
libcassobs2_la_LDFLAGS = -version-info 22:0:0

check_PROGRAMS=get_last_data get_mqtt_stations get_rainfall compute_records get_wlv2_stations get_fieldclimate_stations get_normals get_objenious_stations get_liveobjects_stations get_cimel_stations get_meteofrance_stations compute_minmax compute_month_minmax get_jobs get_map_obs get_virtual_stations get_nbiot_stations get_config insert_timescaledb insert_download
TESTS=$(check_PROGRAMS)

get_last_data_SOURCES = tests/get_last_data.cpp
get_last_data_CPPFLAGS = $(libcassobs2_la_CPPFLAGS)
get_last_data_CXXFLAGS = $(libcassobs2_la_CXXFLAGS)
get_last_data_LDADD = libcassobs2.la $(libcassobs2_la_LIBADD)

get_mqtt_stations_SOURCES = tests/get_mqtt_stations.cpp
get_mqtt_stations_CPPFLAGS = $(libcassobs2_la_CPPFLAGS)
get_mqtt_stations_CXXFLAGS = $(libcassobs2_la_CXXFLAGS)
get_mqtt_stations_LDADD = libcassobs2.la $(libcassobs2_la_LIBADD)

get_wlv2_stations_SOURCES = tests/get_wlv2_stations.cpp
get_wlv2_stations_CPPFLAGS = $(libcassobs2_la_CPPFLAGS)
get_wlv2_stations_CXXFLAGS = $(libcassobs2_la_CXXFLAGS)
get_wlv2_stations_LDADD = libcassobs2.la $(libcassobs2_la_LIBADD)

get_fieldclimate_stations_SOURCES = tests/get_fieldclimate_stations.cpp
get_fieldclimate_stations_CPPFLAGS = $(libcassobs2_la_CPPFLAGS)
get_fieldclimate_stations_CXXFLAGS = $(libcassobs2_la_CXXFLAGS)
get_fieldclimate_stations_LDADD = libcassobs2.la $(libcassobs2_la_LIBADD)

get_rainfall_SOURCES = tests/get_rainfall.cpp
get_rainfall_CPPFLAGS = $(libcassobs2_la_CPPFLAGS)
get_rainfall_CXXFLAGS = $(libcassobs2_la_CXXFLAGS)
get_rainfall_LDADD = libcassobs2.la $(libcassobs2_la_LIBADD)

get_normals_SOURCES = tests/get_normals.cpp
get_normals_CPPFLAGS = $(libcassobs2_la_CPPFLAGS)
get_normals_CXXFLAGS = $(libcassobs2_la_CXXFLAGS)
get_normals_LDADD = libcassobs2.la $(libcassobs2_la_LIBADD)

compute_records_SOURCES = tests/compute_records.cpp
compute_records_CPPFLAGS = $(libcassobs2_la_CPPFLAGS)
compute_records_CXXFLAGS = $(libcassobs2_la_CXXFLAGS)
compute_records_LDADD = libcassobs2.la $(libcassobs2_la_LIBADD)

get_objenious_stations_SOURCES = tests/get_objenious_stations.cpp
get_objenious_stations_CPPFLAGS = $(libcassobs2_la_CPPFLAGS)
get_objenious_stations_CXXFLAGS = $(libcassobs2_la_CXXFLAGS)
get_objenious_stations_LDADD = libcassobs2.la $(libcassobs2_la_LIBADD)

get_liveobjects_stations_SOURCES = tests/get_liveobjects_stations.cpp
get_liveobjects_stations_CPPFLAGS = $(libcassobs2_la_CPPFLAGS)
get_liveobjects_stations_CXXFLAGS = $(libcassobs2_la_CXXFLAGS)
get_liveobjects_stations_LDADD = libcassobs2.la $(libcassobs2_la_LIBADD)

get_cimel_stations_SOURCES = tests/get_cimel_stations.cpp
get_cimel_stations_CPPFLAGS = $(libcassobs2_la_CPPFLAGS)
get_cimel_stations_CXXFLAGS = $(libcassobs2_la_CXXFLAGS)
get_cimel_stations_LDADD = libcassobs2.la $(libcassobs2_la_LIBADD)

get_meteofrance_stations_SOURCES = tests/get_meteofrance_stations.cpp
get_meteofrance_stations_CPPFLAGS = $(libcassobs2_la_CPPFLAGS)
get_meteofrance_stations_CXXFLAGS = $(libcassobs2_la_CXXFLAGS)
get_meteofrance_stations_LDADD = libcassobs2.la $(libcassobs2_la_LIBADD)

compute_minmax_SOURCES = tests/compute_minmax.cpp
compute_minmax_CPPFLAGS = $(libcassobs2_la_CPPFLAGS)
compute_minmax_CXXFLAGS = $(libcassobs2_la_CXXFLAGS)
compute_minmax_LDADD = libcassobs2.la $(libcassobs2_la_LIBADD)

compute_month_minmax_SOURCES = tests/compute_month_minmax.cpp
compute_month_minmax_CPPFLAGS = $(libcassobs2_la_CPPFLAGS)
compute_month_minmax_CXXFLAGS = $(libcassobs2_la_CXXFLAGS)
compute_month_minmax_LDADD = libcassobs2.la $(libcassobs2_la_LIBADD)

get_jobs_SOURCES = tests/get_jobs.cpp
get_jobs_CPPFLAGS = $(libcassobs2_la_CPPFLAGS)
get_jobs_CXXFLAGS = $(libcassobs2_la_CXXFLAGS)
get_jobs_LDADD = libcassobs2.la $(libcassobs2_la_LIBADD)

get_map_obs_SOURCES = tests/get_map_obs.cpp
get_map_obs_CPPFLAGS = $(libcassobs2_la_CPPFLAGS)
get_map_obs_CXXFLAGS = $(libcassobs2_la_CXXFLAGS)
get_map_obs_LDADD = libcassobs2.la $(libcassobs2_la_LIBADD)

get_virtual_stations_SOURCES = tests/get_virtual_stations.cpp
get_virtual_stations_CPPFLAGS = $(libcassobs2_la_CPPFLAGS)
get_virtual_stations_CXXFLAGS = $(libcassobs2_la_CXXFLAGS)
get_virtual_stations_LDADD = libcassobs2.la $(libcassobs2_la_LIBADD)

get_nbiot_stations_SOURCES = tests/get_nbiot_stations.cpp
get_nbiot_stations_CPPFLAGS = $(libcassobs2_la_CPPFLAGS)
get_nbiot_stations_CXXFLAGS = $(libcassobs2_la_CXXFLAGS)
get_nbiot_stations_LDADD = libcassobs2.la $(libcassobs2_la_LIBADD)

get_config_SOURCES = tests/get_config.cpp
get_config_CPPFLAGS = $(libcassobs2_la_CPPFLAGS)
get_config_CXXFLAGS = $(libcassobs2_la_CXXFLAGS)
get_config_LDADD = libcassobs2.la $(libcassobs2_la_LIBADD)

insert_timescaledb_SOURCES = tests/insert_timescaledb.cpp
insert_timescaledb_CPPFLAGS = $(libcassobs2_la_CPPFLAGS)
insert_timescaledb_CXXFLAGS = $(libcassobs2_la_CXXFLAGS)
insert_timescaledb_LDADD = libcassobs2.la $(libcassobs2_la_LIBADD)

insert_download_SOURCES = tests/insert_download.cpp
insert_download_CPPFLAGS = $(libcassobs2_la_CPPFLAGS)
insert_download_CXXFLAGS = $(libcassobs2_la_CXXFLAGS)
insert_download_LDADD = libcassobs2.la $(libcassobs2_la_LIBADD)
