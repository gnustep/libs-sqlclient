include $(GNUSTEP_MAKEFILES)/common.make

-include config.make

PACKAGE_NAME = SQLClient

TEST_TOOL_NAME = testPostgres testMySQL testECPG
testPostgres_OBJC_FILES = testPostgres.m
testPostgres_LIB_DIRS += -L./obj
testPostgres_TOOL_LIBS += -lSQLClient

testMySQL_OBJC_FILES = testMySQL.m
testMySQL_LIB_DIRS += -L./obj
testMySQL_TOOL_LIBS += -lSQLClient

testECPG_OBJC_FILES = testECPG.m
testECPG_TOOL_LIBS += -lSQLClient
testECPG_LIB_DIRS += -L./obj


LIBRARY_NAME=SQLClient

SQLClient_OBJC_FILES = SQLClient.m

SQLClient_HEADER_FILES = SQLClient.h 


SQLClient_HEADER_FILES_INSTALL_DIR = SQLClient


DOCUMENT_NAME=SQLClient
SQLClient_AGSDOC_FILES = SQLClient.h


BUNDLE_NAME=

BUNDLE_INSTALL_DIR=$(GNUSTEP_LOCAL_ROOT)/Library/Bundles/SQLClient

ifneq ($(ECPG),)
BUNDLE_NAME += ECPG
ECPG_OBJC_FILES = ECPG.m
ECPG_BUNDLE_LIBS += -lgnustep-base -lobjc -lecpg
ECPG_PRINCIPAL_CLASS = SQLClientECPG
endif

ifneq ($(POSTGRES),)
BUNDLE_NAME += Postgres
Postgres_OBJC_FILES = Postgres.m
Postgres_BUNDLE_LIBS += -lgnustep-base -lobjc -lpq
Postgres_PRINCIPAL_CLASS = SQLClientPostgres
endif

ifneq ($(MYSQL),)
BUNDLE_NAME += MySQL
MySQL_OBJC_FILES = MySQL.m
MySQL_BUNDLE_LIBS += -lgnustep-base -lobjc -lmysqlclient
MySQL_PRINCIPAL_CLASS = SQLClientMySQL
endif

ifneq ($(ORACLE_HOME),)
BUNDLE_NAME += Oracle
Oracle_OBJC_FILES = Oracle.m
Oracle_BUNDLE_LIBS += -L$(ORACLE_HOME)/lib \
                      -lclntsh \
		      -lgnustep-base -lobjc \
                      $(shell cat $(ORACLE_HOME)/lib/ldflags) \
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
