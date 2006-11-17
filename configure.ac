dnl Process this file with autoconf to produce configure.

AC_INIT(SQLClient.h)
AC_CONFIG_HEADER(config.h)

if test -z "$GNUSTEP_MAKEFILES"; then
  AC_MSG_ERROR([You must have the gnustep-make package installed and set up the GNUSTEP_MAKEFILES environment variable to contain the path to the makefiles directory before configuring!])
else
  . $GNUSTEP_MAKEFILES/GNUstep.sh
fi

#--------------------------------------------------------------------
AC_ARG_WITH(additional-include,
  [  --with-additional-include=flags  specify additional include dirs to use],
  additional_include="$withval", additional_include="no")

if test "$additional_include" != "no"; then
  CPPFLAGS="$CPPFLAGS $additional_include"
  INCD="$INCD $additional_include"
fi

AC_ARG_WITH(additional-lib,
  [  --with-additional-lib=flags  specify additional lib dirs to use],
  additional_lib="$withval", additional_lib="no")

if test "$additional_lib" != "no"; then
  LDFLAGS="$LDFLAGS $additional_lib"
  LIBD="$LIBD $additional_lib"
fi

AC_ARG_WITH(postgres-dir,
  [  --with-postgres-dir=PATH  specify postgres installation dir],
  postgres_topdir="$withval", postgres_topdir="no")

if test "$postgres_topdir" != "no"; then
  CPPFLAGS="$CPPFLAGS -I$postgres_topdir/include -L$postgres_topdir/lib"
  INCD="$INCD -I$postgres_topdir/include"
  LIBD="$LIBD -L$postgres_topdir/lib"
fi


AC_MSG_CHECKING([if Jdbc support was manually disabled])
AC_ARG_ENABLE(jdbc-bundle, [
--disable-jdbc-bundle
   Disable creating the Jdbc bundle.
   Use this option to force the Jdbc bundle not to be built
   even if the Jdbc libraries look like being present.
], 
  ac_cv_jdbc_bundle=$enableval,
  ac_cv_jdbc_bundle="yes")

if test "$ac_cv_jdbc_bundle" = "no"; then
  AC_MSG_RESULT([yes: disabled from the command-line])
  JDBC=
else
  AC_MSG_RESULT([no: build if possible])

# Get likely subdirectory for system specific java include
case "$GNUSTEP_HOST_OS" in
  bsdi*)	_JNI_SUBDIR="bsdos";;
  linux*)	_JNI_SUBDIR="linux";;
  osf*)		_JNI_SUBDIR="alpha";;
  solaris*)	_JNI_SUBDIR="solaris";;
  mingw*)	_JNI_SUBDIR="win32";;
  cygwin*)	_JNI_SUBDIR="win32";;
  *)		_JNI_SUBDIR="genunix";;
esac

save_CFLAGS="$CFLAGS"
save_CPPFLAGS="$CPPFLAGS"
CPPFLAGS="$CPPFLAGS -I$JAVA_HOME/include -I$JAVA_HOME/include/$_JNI_SUBDIR"
AC_CHECK_HEADERS(jni.h)
if test "$ac_cv_header_jni_h" = "yes"; then
  JDBC_VM_LIBS="-ljvm"
  jre_lib="$JAVA_HOME/jre/lib"
  # If on a 32/64bit system and compiling for the 64bit model
  # adjust the cpu type to be the 64bit version
  case "$CFLAGS" in
    *-m64*)
      if test "$GNUSTEP_HOST_CPU" = "ix86"; then 
        _CPU="x86_64"
      else
        _CPU="$GNUSTEP_HOST_CPU"
      fi;;
    *) _CPU="$GNUSTEP_HOST_CPU";;
  esac
  case "$_CPU" in
    ix86)	JAVA_CPU=i386;;
    x86_64)	JAVA_CPU=amd64;;
    sparc)	JAVA_CPU=sparc;;
    *)		JAVA_CPU=i386;;
  esac
  jre_cpu="$jre_lib/$JAVA_CPU"
  JDBC_VM_LIBDIRS="-L$jre_cpu/server"
  CFLAGS="$CFLAGS $JDBC_VM_LIBDIRS"
  AC_CHECK_LIB(jvm,JNI_CreateJavaVM)
  if test "$ac_cv_lib_jvm_JNI_CreateJavaVM" = "yes"; then
    INCD="$INCD -I$JAVA_HOME/include -I$JAVA_HOME/include/$_JNI_SUBDIR"
    JDBC=yes
  else
    JDBC=
    JDBC_VM_LIBS=
    JDBC_VM_LIBDIRS=
    echo "**********************************************"
    echo "Unable to locate jvm library (is it installed)"
    echo "**********************************************"
  fi
else
  JDBC=
  JDBC_VM_LIBS=
  JDBC_VM_LIBDIRS=
  echo "*********************************************"
  echo "Unable to locate jni header (is it installed)"
  echo "*********************************************"
fi
AC_SUBST(JDBC)
AC_SUBST(JDBC_VM_LIBS)
AC_SUBST(JDBC_VM_LIBDIRS)
CFLAGS="$save_CFLAGS"
CPPFLAGS="$save_CPPFLAGS"

fi


AC_MSG_CHECKING([if Mysql support was manually disabled])
AC_ARG_ENABLE(mysql-bundle, [
--disable-mysql-bundle
   Disable creating the Mysql bundle.
   Use this option to force the Mysql bundle not to be built
   even if the Mysql libraries look like being present.
], 
  ac_cv_mysql_bundle=$enableval,
  ac_cv_mysql_bundle="yes")

if test "$ac_cv_mysql_bundle" = "no"; then
  AC_MSG_RESULT([yes: disabled from the command-line])
  MYSQL=
else
  AC_MSG_RESULT([no: build if possible])

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

fi


AC_MSG_CHECKING([if Sqllite support was manually disabled])
AC_ARG_ENABLE(sqllite-bundle, [
--disable-sqllite-bundle
   Disable creating the Sqllite bundle.
   Use this option to force the Sqllite bundle not to be built
   even if the Sqllite libraries look like being present.
], 
  ac_cv_sqllite_bundle=$enableval,
  ac_cv_sqllite_bundle="yes")

if test "$ac_cv_sqllite_bundle" = "no"; then
  AC_MSG_RESULT([yes: disabled from the command-line])
  SQLLITE=
else
  AC_MSG_RESULT([no: build if possible])

AC_CHECK_HEADERS(sqlite3.h)
if test "$ac_cv_header_sqlite3_h" = "yes"; then
  SQLITE=yes
else
  SQLITE=
  echo "*****************************************************"
  echo "Unable to locate sqlite3 headers (are they installed)"
  echo "*****************************************************"
fi
if test "$SQLITE" = "yes"; then
  AC_CHECK_LIB(sqlite3,sqlite3_open)
  if test "$ac_cv_lib_sqlite3_sqlite3_open" != "yes"; then
    SQLITE=
    echo "******************************************************"
    echo "Unable to locate sqlite3 library (is it installed)"
    echo "******************************************************"
  fi
fi
AC_SUBST(SQLITE)

fi

AC_MSG_CHECKING([if Postgres support was manually disabled])
AC_ARG_ENABLE(postgres-bundle, [
--disable-postgres-bundle
   Disable creating the Postgres bundle.
   Use this option to force the Postgres bundle not to be built
   even if the Postgres libraries look like being present.
], 
  ac_cv_postgres_bundle=$enableval,
  ac_cv_postgres_bundle="yes")

if test "$ac_cv_postgres_bundle" = "no"; then
  AC_MSG_RESULT([yes: disabled from the command-line])
  POSTGRES=
else
  AC_MSG_RESULT([no: build if possible])

# Start POSTGRES checks
POSTGRES=

if test "$POSTGRES" = ""; then
  AC_CHECK_HEADERS(libpq-fe.h)
  if test "$ac_cv_header_libpq_fe_h" = "yes"; then
    POSTGRES=yes
  fi
fi
if test "$ECPG" = ""; then
  AC_CHECK_HEADERS(ecpglib.h)
  if test "$ac_cv_header_ecpglib_h" = "yes"; then
    ECPG=yes
  fi
fi

if test "$POSTGRES" = ""; then
  AC_CHECK_HEADERS(/usr/include/postgresql/libpq-fe.h)
  CPPFLAGS="$save_CPPFLAGS"
  if test "$ac_cv_header__usr_include_postgresql_libpq_fe_h" = "yes"; then
    INCD="$INCD -I/usr/include/postgresql"
    POSTGRES=yes
  fi
fi
if test "$ECPG" = ""; then
  AC_CHECK_HEADERS(/usr/include/postgresql/ecpglib.h)
  if test "$ac_cv_header__usr_include_postgresql_libpq_fe_h" = "yes"; then
    INCD="$INCD -I/usr/include/postgresql"
    ECPG=yes
  fi
fi

if test "$POSTGRES" = ""; then
  AC_CHECK_HEADERS(/usr/include/postgresql/8.0/libpq-fe.h)
  CPPFLAGS="$save_CPPFLAGS"
  if test "$ac_cv_header__usr_include_postgresql_8_0_libpq_fe_h" = "yes"; then
    INCD="$INCD -I/usr/include/postgresql/8.0"
    POSTGRES=yes
  fi
fi
if test "$ECPG" = ""; then
  AC_CHECK_HEADERS(/usr/include/postgresql/8.0/ecpglib.h)
  if test "$ac_cv_header__usr_include_postgresql_8_0_libpq_fe_h" = "yes"; then
    INCD="$INCD -I/usr/include/postgresql/8.0"
    ECPG=yes
  fi
fi

if test "$POSTGRES" = ""; then
  AC_CHECK_HEADERS(/usr/include/pgsql/libpq-fe.h)
  CPPFLAGS="$save_CPPFLAGS"
  if test "$ac_cv_header__usr_include_pgsql_libpq_fe_h" = "yes"; then
    INCD="$INCD -I/usr/include/pgsql"
    POSTGRES=yes
  fi
fi
if test "$ECPG" = ""; then
  AC_CHECK_HEADERS(/usr/include/pgsql/ecpglib.h)
  if test "$ac_cv_header__usr_include_pgsql_libpq_fe_h" = "yes"; then
    INCD="$INCD -I/usr/include/pgsql"
    ECPG=yes
  fi
fi

if test "$POSTGRES" = ""; then
  AC_CHECK_HEADERS(/usr/local/include/pgsql/libpq-fe.h)
  CPPFLAGS="$save_CPPFLAGS"
  if test "$ac_cv_header__usr_local_include_pgsql_libpq_fe_h" = "yes"; then
    INCD="$INCD -I/usr/local/include/pgsql"
    POSTGRES=yes
  fi
fi
if test "$ECPG" = ""; then
  AC_CHECK_HEADERS(/usr/local/include/pgsql/ecpglib.h)
  if test "$ac_cv_header__usr_local_include_pgsql_libpq_fe_h" = "yes"; then
    INCD="$INCD -I/usr/local/include/pgsql"
    ECPG=yes
  fi
fi

if test "$POSTGRES" = ""; then
  echo "**************************************************************"
  echo "Unable to locate libpq (postgres) headers (are they installed)"
  echo "**************************************************************"
fi
if test "$ECPG" = ""; then
  echo "*************************************************************"
  echo "Unable to locate ecpg (postgres) headers (are they installed)"
  echo "*************************************************************"
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
      echo "******************************************************"
      if test "$ac_cv_lib_pq_PQclear" != "yes"; then
	echo "Unable to locate postgres pq library (is it installed)"
      else
	echo "Located postgres pq library, but it is too old to use!"
      fi
      echo "Perhaps you can try 'configure --with-postgres=dir=path'"
      echo "to point to the postgres version you wish to use."
      echo "******************************************************"
  else
    AC_CHECK_FUNCS(PQescapeStringConn)
  fi
  AC_CHECK_LIB(ecpg,ECPGconnect)
  if test "$ac_cv_lib_ecpg_ECPGconnect" != "yes"; then
      ECPG=
      echo "********************************************************"
      echo "Unable to locate postgres ecpg library (is it installed)"
      echo "Perhaps you can try 'configure --with-postgres=dir=path'"
      echo "to point to the postgres version you wish to use."
      echo "********************************************************"
  fi
fi

# End POSTGRES checks
fi

AC_SUBST(POSTGRES)
AC_SUBST(ECPG)

ORACLE_HOME=
AC_SUBST(ORACLE_HOME)

AC_SUBST(INCD)
AC_SUBST(LIBD)
AC_SUBST(LIBS)

if test "$JDBC" = "yes"; then
  BUNDLE="The JDBC backend bundle will be built"
else
  BUNDLE="The JDBC backend bundle will NOT be built"
fi
AC_MSG_RESULT(${BUNDLE})

if test "$MYSQL" = "yes"; then
  BUNDLE="The MySQL backend bundle will be built"
else
  BUNDLE="The MySQL backend bundle will NOT be built"
fi
AC_MSG_RESULT(${BUNDLE})

if test "$SQLITE" = "yes"; then
  BUNDLE="The SQLite backend bundle will be built"
else
  BUNDLE="The SQLite backend bundle will NOT be built"
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

