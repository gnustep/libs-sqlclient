include $(GNUSTEP_MAKEFILES)/common.make

-include config.make

PACKAGE_NAME = SQLClient

TEST_TOOL_NAME=

LIBRARY_NAME=SQLClient

SQLClient_INTERFACE_VERSION=1.1

SQLClient_OBJC_FILES = SQLClient.m WebServer.m WebServerBundles.m
SQLClient_LIBRARIES_DEPEND_UPON =
SQLClient_HEADER_FILES = SQLClient.h  WebServer.h


SQLClient_HEADER_FILES_INSTALL_DIR = SQLClient


DOCUMENT_NAME=SQLClient
SQLClient_AGSDOC_FILES = SQLClient.h WebServer.h


BUNDLE_NAME=

BUNDLE_INSTALL_DIR=$(GNUSTEP_INSTALLATION_DIR)/Library/Bundles/SQLClient

ifneq ($(ECPG),)
BUNDLE_NAME += ECPG
ECPG_OBJC_FILES = ECPG.m
ECPG_LIB_DIRS = -L./obj
ECPG_BUNDLE_LIBS += -lecpg
ECPG_PRINCIPAL_CLASS = SQLClientECPG
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
endif

-include GNUmakefile.preamble

include $(GNUSTEP_MAKEFILES)/library.make
include $(GNUSTEP_MAKEFILES)/bundle.make
include $(GNUSTEP_MAKEFILES)/test-tool.make
include $(GNUSTEP_MAKEFILES)/documentation.make

-include GNUmakefile.postamble
