include $(GNUSTEP_MAKEFILES)/common.make

-include config.make

PACKAGE_NAME = SQLClient
PACKAGE_VERSION = 1.1.0
CVS_MODULE_NAME = gnustep/dev-libs/SQLClient
CVS_TAG_NAME = SQLClient

TEST_TOOL_NAME=

LIBRARY_NAME=SQLClient
DOCUMENT_NAME=SQLClient

SQLClient_INTERFACE_VERSION=1.1

SQLClient_OBJC_FILES = SQLClient.m
SQLClient_LIBRARIES_DEPEND_UPON =
SQLClient_HEADER_FILES = SQLClient.h
SQLClient_AGSDOC_FILES = SQLClient.h

#
# Assume that the use of the gnu runtime means we have the gnustep
# base library and can use its extensions to build WebServer stuff.
#
ifeq ($(OBJC_RUNTIME_LIB), gnu)
SQLClient_OBJC_FILES += WebServer.m WebServerBundles.m
SQLClient_HEADER_FILES += WebServer.h
SQLClient_AGSDOC_FILES += WebServer.h
endif

SQLClient_HEADER_FILES_INSTALL_DIR = SQLClient

BUNDLE_NAME=

BUNDLE_INSTALL_DIR=$(GNUSTEP_INSTALLATION_DIR)/Library/Bundles/SQLClient

# In some systems and situations the dynamic linker needs to haved the
# SQLClient, gnustep-base, and objc libraries explicityly linked into
# the bundle, but in others it requires them to not be linked.
# To handle that, we create two versions of each bundle, the seond version
# has _libs appended to the bundle name, and has the extra libraries linked.

ifneq ($(ECPG),)
BUNDLE_NAME += ECPG
ECPG_OBJC_FILES = ECPG.m
ECPG_LIB_DIRS = -L./obj
ECPG_BUNDLE_LIBS += -lecpg
ECPG_PRINCIPAL_CLASS = SQLClientECPG
BUNDLE_NAME += ECPG_libs
ECPG_libs_OBJC_FILES = ECPG.m
ECPG_libs_LIB_DIRS = -L./obj
ECPG_libs_BUNDLE_LIBS += -lSQLClient -lgnustep-base -lobjc -lecpg
ECPG_libs_PRINCIPAL_CLASS = SQLClientECPG_libs
TEST_TOOL_NAME += testECPG
testECPG_OBJC_FILES = testECPG.m
testECPG_TOOL_LIBS += -lSQLClient
testECPG_LIB_DIRS += -L./obj
endif

ifneq ($(POSTGRES),)
BUNDLE_NAME += Postgres
Postgres_OBJC_FILES = Postgres.m
Postgres_LIB_DIRS = -L./obj
Postgres_BUNDLE_LIBS += -lpq
Postgres_PRINCIPAL_CLASS = SQLClientPostgres
BUNDLE_NAME += Postgres_libs
Postgres_libs_OBJC_FILES = Postgres.m
Postgres_libs_LIB_DIRS = -L./obj
Postgres_libs_BUNDLE_LIBS += -lSQLClient -lgnustep-base -lobjc -lpq
Postgres_libs_PRINCIPAL_CLASS = SQLClientPostgres_libs
TEST_TOOL_NAME += testPostgres
testPostgres_OBJC_FILES = testPostgres.m
testPostgres_LIB_DIRS += -L./obj
testPostgres_TOOL_LIBS += -lSQLClient
endif

ifneq ($(MYSQL),)
BUNDLE_NAME += MySQL
MySQL_OBJC_FILES = MySQL.m
MySQL_LIB_DIRS = -L./obj
MySQL_BUNDLE_LIBS += -lmysqlclient
MySQL_PRINCIPAL_CLASS = SQLClientMySQL
BUNDLE_NAME += MySQL_libs
MySQL_libs_OBJC_FILES = MySQL.m
MySQL_libs_LIB_DIRS = -L./obj
MySQL_libs_BUNDLE_LIBS += -lSQLClient -lgnustep-base -lobjc -lmysqlclient
MySQL_libs_PRINCIPAL_CLASS = SQLClientMySQL_libs
TEST_TOOL_NAME += testMySQL
testMySQL_OBJC_FILES = testMySQL.m
testMySQL_LIB_DIRS += -L./obj
testMySQL_TOOL_LIBS += -lSQLClient
endif

ifneq ($(ORACLE_HOME),)
BUNDLE_NAME += Oracle
Oracle_OBJC_FILES = Oracle.m
Oracle_LIB_DIRS = -L$(ORACLE_HOME)/lib -L./obj \
			$(shell cat $(ORACLE_HOME)/lib/ldflags)
Oracle_BUNDLE_LIBS += -lclntsh \
                      $(shell cat $(ORACLE_HOME)/lib/sysliblist) \
                      -ldl -lm
Oracle_PRINCIPAL_CLASS = SQLClientOracle
BUNDLE_NAME += Oracle_libs
Oracle_libs_OBJC_FILES = Oracle.m
Oracle_libs_LIB_DIRS = -L$(ORACLE_HOME)/lib -L./obj \
			$(shell cat $(ORACLE_HOME)/lib/ldflags)
Oracle_libs_BUNDLE_LIBS += -lclntsh \
		      -lSQLClient -lgnustep-base -lobjc \
                      $(shell cat $(ORACLE_HOME)/lib/sysliblist) \
                      -ldl -lm
Oracle_libs_PRINCIPAL_CLASS = SQLClientOracle_libs
endif

-include GNUmakefile.preamble

include $(GNUSTEP_MAKEFILES)/library.make
include $(GNUSTEP_MAKEFILES)/bundle.make
include $(GNUSTEP_MAKEFILES)/test-tool.make
include $(GNUSTEP_MAKEFILES)/documentation.make

-include GNUmakefile.postamble
