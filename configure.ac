dnl Process this file with autoconf to produce configure.

AC_INIT(SQLClient.h)
AC_CONFIG_HEADER(config.h)

AC_ARG_WITH(additional-include-dir,
  [  --with-additional-include-dir=PATH  specify additional include dirs to use],
  additional_include_dir="$withval", additional_include_dir="no")

if test "$additional_include_dir" != "no"; then
  CPPFLAGS="$CPPFLAGS $additional_include_dir"
  INCD="$INCD $additional_include_dir"
fi

echo "Postgres development support"
AC_ARG_WITH(postgres-dir,
  [  --with-postgres-dir=PATH  specify postgres installation dir],
  postgres_topdir="$withval", postgres_topdir="no")

if test "$postgres_topdir" != "no"; then
  CPPFLAGS="$CPPFLAGS -I$postgres_topdir/include -L$postgres_topdir/lib"
  INCD="$INCD -I$postgres_topdir/include"
  LIBD="$LIBD -L$postgres_topdir/lib"
fi

AC_CHECK_HEADERS(mysql/mysql.h)
if test "$ac_cv_header_mysql_mysql_h" = "yes"; then
  MYSQL=yes
else
  MYSQL=
  echo "*********************************************************"
  echo "Unable to locate mysqlclient headers (are they installed)"
  echo "*********************************************************"
fi
if test "$MYSQL" = "yes"; then
  AC_CHECK_LIB(mysqlclient,mysql_init)
  if test "$ac_cv_lib_mysqlclient_mysql_init" != "yes"; then
    MYSQL=
    echo "******************************************************"
    echo "Unable to locate mysqlclient library (is it installed)"
    echo "******************************************************"
  fi
fi
AC_SUBST(MYSQL)

AC_CHECK_HEADERS(libpq-fe.h)
if test "$ac_cv_header_libpq_fe_h" = "yes"; then
  POSTGRES=yes
else
  old="$CPPFLAGS"
  CPPFLAGS="$CPPFLAGS -I/usr/include/postgresql"
  AC_CHECK_HEADERS(/usr/include/postgresql/libpq-fe.h)
  CPPFLAGS="$old"
  if test "$ac_cv_header__usr_include_postgresql_libpq_fe_h" = "yes"; then
    INCD="$INCD -I/usr/include/postgresql"
    POSTGRES=yes
  else
    CPPFLAGS="$CPPFLAGS -I/usr/include/pgsql"
    AC_CHECK_HEADERS(/usr/include/pgsql/libpq-fe.h)
    CPPFLAGS="$old"
    if test "$ac_cv_header__usr_include_pgsql_libpq_fe_h" = "yes"; then
      INCD="$INCD -I/usr/include/pgsql"
      POSTGRES=yes
    else
      CPPFLAGS="$CPPFLAGS -I/usr/local/include/pgsql"
      AC_CHECK_HEADERS(/usr/local/include/pgsql/libpq-fe.h)
      CPPFLAGS="$old"
      if test "$ac_cv_header__usr_local_include_pgsql_libpq_fe_h" = "yes"; then
	INCD="$INCD -I/usr/local/include/pgsql"
	POSTGRES=yes
      else
	CPPFLAGS="$save_CPPFLAGS"
	POSTGRES=
	echo "**************************************************************"
	echo "Unable to locate libpq (postgres) headers (are they installed)"
	echo "**************************************************************"
      fi
    fi
  fi
fi

AC_CHECK_HEADERS(ecpglib.h)
if test "$ac_cv_header_ecpglib_h" = "yes"; then
  ECPG=yes
else
  old="$CPPFLAGS"
  CPPFLAGS="$CPPFLAGS -I/usr/include/postgresql"
  AC_CHECK_HEADERS(/usr/include/postgresql/ecpglib.h)
  CPPFLAGS="$old"
  if test "$ac_cv_header__usr_include_postgresql_ecpglib_h" = "yes"; then
    INCD="$INCD -I/usr/include/postgresql"
    ECPG=yes
  else
    CPPFLAGS="$CPPFLAGS -I/usr/include/pgsql"
    AC_CHECK_HEADERS(/usr/include/pgsql/ecpglib.h)
    CPPFLAGS="$old"
    if test "$ac_cv_header__usr_include_pgsql_ecpglib_h" = "yes"; then
      INCD="$INCD -I/usr/include/pgsql"
      ECPG=yes
    else
      CPPFLAGS="$CPPFLAGS -I/usr/local/include/pgsql"
      AC_CHECK_HEADERS(/usr/local/include/pgsql/ecpglib.h)
      CPPFLAGS="$old"
      if test "$ac_cv_header__usr_local_include_pgsql_ecpglib_h" = "yes"; then
	INCD="$INCD -I/usr/local/include/pgsql"
	ECPG=yes
      else
	CPPFLAGS="$save_CPPFLAGS"
	ECPG=
	echo "*************************************************************"
	echo "Unable to locate ecpg (postgres) headers (are they installed)"
	echo "*************************************************************"
      fi
    fi
  fi
fi
if test "$POSTGRES" = "yes"; then
  # NICOLA - hack
  if test -d /usr/lib/pgsql ; then
    CPPFLAGS="$CPPFLAGS -L/usr/lib/pgsql"
    LIBD="$LIBD -L/usr/lib/pgsql"
  else
    if test -d /usr/local/lib/pgsql ; then
      CPPFLAGS="$CPPFLAGS -L/usr/local/lib/pgsql"
      LIBD="$LIBD -L/usr/local/lib/pgsql"
    fi
  fi
 
  AC_CHECK_LIB(pq,PQfformat)
  if test "$ac_cv_lib_pq_PQfformat" != "yes"; then
      POSTGRES=
      AC_CHECK_LIB(pq,PQclear)
      if test "$ac_cv_lib_pq_PQclear" != "yes"; then
	echo "******************************************************"
	echo "Unable to locate postgres pq library (is it installed)"
	echo "******************************************************"
      else
	echo "******************************************************"
	echo "Located postgres pq library, but it is too old to use!"
	echo "******************************************************"
      fi
  fi
  AC_CHECK_LIB(ecpg,ECPGconnect)
  if test "$ac_cv_lib_ecpg_ECPGconnect" != "yes"; then
      ECPG=
      echo "********************************************************"
      echo "Unable to locate postgres ecpg library (is it installed)"
      echo "********************************************************"
  fi
fi

AC_SUBST(POSTGRES)
AC_SUBST(ECPG)

ORACLE_HOME=
AC_SUBST(ORACLE_HOME)

AC_SUBST(INCD)
AC_SUBST(LIBD)
AC_SUBST(LIBS)

if test "$MYSQL" = "yes"; then
  BUNDLE="The MySQL backend bundle will be built"
else
  BUNDLE="The MySQL backend bundle will NOT be built"
fi
AC_MSG_RESULT(${BUNDLE})

if test "$POSTGRES" = "yes"; then
  BUNDLE="The Postgres backend bundle will be built"
else
  BUNDLE="The Postgres backend bundle will NOT be built"
fi
AC_MSG_RESULT(${BUNDLE})

if test "$ECPG" = "yes"; then
  BUNDLE="The ECPG backend bundle will be built"
else
  BUNDLE="The ECPG backend bundle will NOT be built"
fi
AC_MSG_RESULT(${BUNDLE})

if test "$ORACLE" = "yes"; then
  BUNDLE="The Oracle backend bundle will be built"
else
  BUNDLE="The Oracle backend bundle will NOT be built"
fi
AC_MSG_RESULT(${BUNDLE})

AC_OUTPUT(config.make)

