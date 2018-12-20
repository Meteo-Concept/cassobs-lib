#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.69])
AC_INIT([CassObs], [0.1], [contact@meteo-concept.fr])
AC_CONFIG_SRCDIR([src/dbconnection_common.h])
AC_CONFIG_HEADERS([src/config.h])
AC_CONFIG_MACRO_DIRS([m4])
AM_INIT_AUTOMAKE([-Wall -Werror subdir-objects])

## Checks for programs.
# For compiling
AC_PROG_CXX
AX_CXX_COMPILE_STDCXX_14
# For libtooling
AM_PROG_AR
# For the documentation
AC_CHECK_PROGS([DOT], [dot])
if test -z "$DOT";
then AC_MSG_WARN([[Graphs will not be available in Doxygen documentation]])
fi
AC_CHECK_PROGS([DOXYGEN], [doxygen])
if test -z "$DOXYGEN";
   then AC_MSG_WARN([[Doxygen not found - continuing without Doxygen support]])
fi
AM_CONDITIONAL([HAVE_DOXYGEN],[test -n "$DOXYGEN"])
AM_COND_IF([HAVE_DOXYGEN], [AC_CONFIG_FILES([Doxyfile])])
AM_CONDITIONAL([HAVE_DOT],[test -n "$DOT"])
AM_COND_IF([HAVE_DOT], [AC_SUBST(DOT_IS_AVAILABLE,["YES"])], [AC_SUBST(DOT_IS_AVAILABLE,["NO"])])

# Checks for libraries.

# Checks for header files.
AC_CHECK_HEADERS([syslog.h])
AC_CHECK_HEADERS([cassandra.h])
AC_CHECK_HEADERS([date.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_TYPE_SIZE_T

# Checks for library functions.
AX_PTHREAD
AC_CHECK_LIB([cassandra],[cass_session_new])

# Output the configuration
AC_CONFIG_FILES([Makefile src/Makefile])
LT_INIT
AC_OUTPUT
