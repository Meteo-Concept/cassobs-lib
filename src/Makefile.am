lib_LTLIBRARIES = libcassobs.la
libcassobs_la_SOURCES = \
		    dbconnection.cpp\
		    dbconnection.h\
		    dbconnection_common.cpp\
		    dbconnection_common.h\
		    dbconnection_minmax.cpp\
		    dbconnection_minmax.h\
		    dbconnection_month_minmax.cpp\
		    dbconnection_month_minmax.h\
		    message.h

libcassobs_la_CPPFLAGS = $(PTHREAD_CFLAGS) $(CASSANDRA_CPPFLAGS) $(DATE_CPPFLAGS)
libcassobs_la_CXXFLAGS = -std=c++14 -Wall -Wextra -pedantic
libcassobs_la_LIBADD = $(PTHREAD_CFLAGS) $(CASSANDRA_LIBS)
libcassobs_la_LDFLAGS = -version-info 0:0:0
