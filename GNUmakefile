include $(GNUSTEP_MAKEFILES)/common.make

-include config.make

PACKAGE_NAME = SQLClient
PACKAGE_VERSION = 1.4.0
CVS_MODULE_NAME = gnustep/dev-libs/SQLClient
CVS_TAG_NAME = SQLClient
SVN_BASE_URL=svn+ssh://svn.gna.org/svn/gnustep/libs
SVN_MODULE_NAME=sqlclient

TEST_TOOL_NAME=

LIBRARY_NAME=SQLClient
DOCUMENT_NAME=SQLClient

SQLClient_INTERFACE_VERSION=1.4

SQLClient_OBJC_FILES = SQLClient.m
SQLClient_LIBRARIES_DEPEND_UPON = -lPerformance
SQLClient_HEADER_FILES = SQLClient.h
SQLClient_AGSDOC_FILES = SQLClient.h

# Optional Java wrappers for the library
JAVA_WRAPPER_NAME = SQLClient

SQLClient_HEADER_FILES_INSTALL_DIR = SQLClient

BUNDLE_NAME=

BUNDLE_INSTALL_DIR=$(GNUSTEP_INSTALLATION_DIR)/Library/Bundles/SQLClient

# In some systems and situations the dynamic linker needs to haved the
# SQLClient, gnustep-base, and objc libraries explicityly linked into
# the bundle, but in others it requires them to not be linked.
# To handle that, we create two versions of each bundle, the seond version
# has _libs appended to the bundle name, and has the extra libraries linked.

ifeq ($(LINKSQLCLIENT),)
  LINKSQLCLIENT=0
  ifeq ($(APPLE),1)
    LINKSQLCLIENT=1
  endif
endif

ifneq ($(ECPG),)
ifeq ($(LINKSQLCLIENT),1)
BUNDLE_NAME += ECPG
ECPG_OBJC_FILES = ECPG.m
ECPG_LIB_DIRS = -L./$(GNUSTEP_OBJ_DIR)
ECPG_BUNDLE_LIBS += -lSQLClient -lecpg
ECPG_PRINCIPAL_CLASS = SQLClientECPG
else
BUNDLE_NAME += ECPG
ECPG_OBJC_FILES = ECPG.m
ECPG_LIB_DIRS = -L./$(GNUSTEP_OBJ_DIR)
ECPG_BUNDLE_LIBS += -lecpg
ECPG_PRINCIPAL_CLASS = SQLClientECPG
BUNDLE_NAME += ECPG_libs
ECPG_libs_OBJC_FILES = ECPG.m
ECPG_libs_LIB_DIRS = -L./$(GNUSTEP_OBJ_DIR)
ECPG_libs_BUNDLE_LIBS += -lSQLClient -lPerformance \
-lgnustep-base -lobjc -lecpg
ECPG_libs_PRINCIPAL_CLASS = SQLClientECPG_libs
endif
TEST_TOOL_NAME += testECPG
testECPG_OBJC_FILES = testECPG.m
testECPG_LIB_DIRS += -L./$(GNUSTEP_OBJ_DIR)
testECPG_TOOL_LIBS += -lSQLClient -lPerformance
endif

ifneq ($(POSTGRES),)
ifeq ($(LINKSQLCLIENT),1)
BUNDLE_NAME += Postgres
Postgres_OBJC_FILES = Postgres.m
Postgres_LIB_DIRS = -L./$(GNUSTEP_OBJ_DIR)
Postgres_BUNDLE_LIBS += -lSQLClient -lpq
Postgres_PRINCIPAL_CLASS = SQLClientPostgres
else
BUNDLE_NAME += Postgres
Postgres_OBJC_FILES = Postgres.m
Postgres_LIB_DIRS = -L./$(GNUSTEP_OBJ_DIR)
Postgres_BUNDLE_LIBS += -lpq
Postgres_PRINCIPAL_CLASS = SQLClientPostgres
BUNDLE_NAME += Postgres_libs
Postgres_libs_OBJC_FILES = Postgres.m
Postgres_libs_LIB_DIRS = -L./$(GNUSTEP_OBJ_DIR)
Postgres_libs_BUNDLE_LIBS += -lSQLClient -lPerformance \
-lgnustep-base -lobjc -lpq
Postgres_libs_PRINCIPAL_CLASS = SQLClientPostgres_libs
endif
TEST_TOOL_NAME += testPostgres
testPostgres_OBJC_FILES = testPostgres.m
testPostgres_LIB_DIRS += -L./$(GNUSTEP_OBJ_DIR)
testPostgres_TOOL_LIBS += -lSQLClient -lPerformance
endif

ifneq ($(JDBC),)
ifeq ($(LINKSQLCLIENT),1)
BUNDLE_NAME += JDBC
JDBC_OBJC_FILES = JDBC.m
JDBC_LIB_DIRS = -L./$(GNUSTEP_OBJ_DIR) $(JDBC_VM_LIBDIRS)
JDBC_BUNDLE_LIBS += -lSQLClient $(JDBC_VM_LIBS)
JDBC_PRINCIPAL_CLASS = SQLClientJDBC
else
BUNDLE_NAME += JDBC
JDBC_OBJC_FILES = JDBC.m
JDBC_LIB_DIRS = -L./$(GNUSTEP_OBJ_DIR) $(JDBC_VM_LIBDIRS)
JDBC_BUNDLE_LIBS += $(JDBC_VM_LIBS)
JDBC_PRINCIPAL_CLASS = SQLClientJDBC
BUNDLE_NAME += JDBC_libs
JDBC_libs_OBJC_FILES = JDBC.m
JDBC_libs_LIB_DIRS = -L./$(GNUSTEP_OBJ_DIR) $(JDBC_VM_LIBDIRS)
JDBC_libs_BUNDLE_LIBS += -lSQLClient -lPerformance \
-lgnustep-base -lobjc $(JDBC_VM_LIBS)
JDBC_libs_PRINCIPAL_CLASS = SQLClientJDBC_libs
endif
TEST_TOOL_NAME += testJDBC
testJDBC_OBJC_FILES = testJDBC.m
testJDBC_LIB_DIRS += -L./$(GNUSTEP_OBJ_DIR)
testJDBC_TOOL_LIBS += -lSQLClient -lPerformance
endif

ifneq ($(MYSQL),)
ifeq ($(LINKSQLCLIENT),1)
BUNDLE_NAME += MySQL
MySQL_OBJC_FILES = MySQL.m
MySQL_LIB_DIRS = -L./$(GNUSTEP_OBJ_DIR)
MySQL_BUNDLE_LIBS += -lSQLClient -lmysqlclient
MySQL_PRINCIPAL_CLASS = SQLClientMySQL
else
BUNDLE_NAME += MySQL
MySQL_OBJC_FILES = MySQL.m
MySQL_LIB_DIRS = -L./$(GNUSTEP_OBJ_DIR)
MySQL_BUNDLE_LIBS += -lmysqlclient
MySQL_PRINCIPAL_CLASS = SQLClientMySQL
BUNDLE_NAME += MySQL_libs
MySQL_libs_OBJC_FILES = MySQL.m
MySQL_libs_LIB_DIRS = -L./$(GNUSTEP_OBJ_DIR)
MySQL_libs_BUNDLE_LIBS += -lSQLClient -lPerformance \
-lgnustep-base -lobjc -lmysqlclient
MySQL_libs_PRINCIPAL_CLASS = SQLClientMySQL_libs
endif
TEST_TOOL_NAME += testMySQL
testMySQL_OBJC_FILES = testMySQL.m
testMySQL_LIB_DIRS += -L./$(GNUSTEP_OBJ_DIR)
testMySQL_TOOL_LIBS += -lSQLClient -lPerformance
endif

ifneq ($(SQLITE),)
ifeq ($(LINKSQLCLIENT),1)
BUNDLE_NAME += SQLite
SQLite_OBJC_FILES = SQLite.m
SQLite_LIB_DIRS = -L./$(GNUSTEP_OBJ_DIR)
SQLite_BUNDLE_LIBS += -lSQLClient -lsqlite3
SQLite_PRINCIPAL_CLASS = SQLClientSQLite
else
BUNDLE_NAME += SQLite
SQLite_OBJC_FILES = SQLite.m
SQLite_LIB_DIRS = -L./$(GNUSTEP_OBJ_DIR)
SQLite_BUNDLE_LIBS += -lsqlite3
SQLite_PRINCIPAL_CLASS = SQLClientSQLite
BUNDLE_NAME += SQLite_libs
SQLite_libs_OBJC_FILES = SQLite.m
SQLite_libs_LIB_DIRS = -L./$(GNUSTEP_OBJ_DIR)
SQLite_libs_BUNDLE_LIBS += -lSQLClient -lPerformance \
-lgnustep-base -lobjc -lsqlite3
SQLite_libs_PRINCIPAL_CLASS = SQLClientSQLite_libs
endif
TEST_TOOL_NAME += testSQLite
testSQLite_OBJC_FILES = testSQLite.m
testSQLite_LIB_DIRS += -L./$(GNUSTEP_OBJ_DIR)
testSQLite_TOOL_LIBS += -lSQLClient -lPerformance
endif

ifneq ($(ORACLE_HOME),)
BUNDLE_NAME += Oracle
Oracle_OBJC_FILES = Oracle.m
Oracle_LIB_DIRS = -L$(ORACLE_HOME)/lib -L./$(GNUSTEP_OBJ_DIR) \
			$(shell cat $(ORACLE_HOME)/lib/ldflags)
Oracle_BUNDLE_LIBS += -lclntsh \
                      $(shell cat $(ORACLE_HOME)/lib/sysliblist) \
                      -ldl -lm
Oracle_PRINCIPAL_CLASS = SQLClientOracle
BUNDLE_NAME += Oracle_libs
Oracle_libs_OBJC_FILES = Oracle.m
Oracle_libs_LIB_DIRS = -L$(ORACLE_HOME)/lib -L./$(GNUSTEP_OBJ_DIR) \
			$(shell cat $(ORACLE_HOME)/lib/ldflags)
Oracle_libs_BUNDLE_LIBS += -lclntsh \
		      -lSQLClient -lPerformance -lgnustep-base -lobjc \
                      $(shell cat $(ORACLE_HOME)/lib/sysliblist) \
                      -ldl -lm
Oracle_libs_PRINCIPAL_CLASS = SQLClientOracle_libs
endif

-include GNUmakefile.preamble

include $(GNUSTEP_MAKEFILES)/library.make
include $(GNUSTEP_MAKEFILES)/bundle.make
# If JIGS is installed, automatically generate Java wrappers as well.
# Because of the '-', should not complain if java-wrapper.make can't be
# found ... simply skip generation of java wrappers in that case.
-include $(GNUSTEP_MAKEFILES)/java-wrapper.make
include $(GNUSTEP_MAKEFILES)/test-tool.make
include $(GNUSTEP_MAKEFILES)/documentation.make

-include GNUmakefile.postamble
