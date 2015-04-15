/** 
   Copyright (C) 2004 Free Software Foundation, Inc.
   
   Written by:  Richard Frith-Macdonald <rfm@gnu.org>
   Date:	April 2004
   
   This file is part of the SQLClient Library.

   This library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation; either
   version 3 of the License, or (at your option) any later version.
   
   This library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Lesser General Public License for more details.
   
   You should have received a copy of the GNU Lesser General Public
   License along with this library; if not, write to the Free
   Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111 USA.

<title>SQLClient documentation</title>
<chapter>
  <heading>The SQLClient library</heading>
  <section>
    <heading>What is the SQLClient library?</heading>
    <p>
      The SQLClient library is designed to provide a simple interface to SQL
      databases for GNUstep applications.  It does not attempt the sort of
      abstraction provided by the much more sophisticated GDL2 library, but
      rather allows applications to directly execute SQL queries and
      statements.
    </p>
    <p>
      SQLClient provides for the Objective-C programmer much the same thing
      that JDBC provides for the Java programmer (though SQLClient is a bit
      faster, easier to use, and easier to add new database backends for
      than JDBC).
    </p>
    <p>
      The major features of the SQLClient library are -
    </p>
    <list>
      <item>
        Simple API for executing queries and statements ... a variable
	length sequence of comma separated strings and other objects
	(NSNumber, NSDate, NSData) are concatenated into a single SQL
	statement and executed.
      </item>
      <item>
        Simple API ([SQLTransaction])for combining multiple SQL statements
	into a single transaction which can be used to minimise client-server
	interactions to get the best possible performance from your database.
      </item>
      <item>
        Supports multiple sumultaneous named connections to a database
	server in a thread-safe manner.<br />
      </item>
      <item>
	Supports multiple simultaneous connections to different database
	servers with backend driver bundles loaded for different database
	engines.  Clear, simple subclassing of the abstract base class to
	enable easy implementation of new backend bundles.
      </item>
      <item>
	Configuration for all connections held in one place and referenced
	by connection name for ease of configuration control.
	Changes via NSUserDefaults can even allow reconfiguration of
	client instances within a running application.
      </item>
      <item>
        Thread safe operation ... The base class supports locking such that
	a single instance can be shared between multiple threads.
      </item>
      <item>
        Support for standalone web applications ... eg to allow data to be
	added to the database by people posting web forms to the application.
      </item>
      <item>
        Supports notification of connection to and disconnection from the
        database server.
      </item>
    </list>
  </section>
  <section>
    <heading>What backend bundles are available?</heading>
    <p>
      Current backend bundles are -
    </p>
    <list>
      <item>
        ECPG - a bundle using the embedded SQL interface for postgres.<br />
	This is based on a similar code which was in production use
	for over eighteen months, so it should be reliable, but inefficient.
      </item>
      <item>
        Postgres - a bundle using the libpq native interface for postgres.<br />
	This is the preferred backend as it allows 'SELECT FOR UPDATE', which
	the ECPG backend cannot support due to limitations in the postgres
	implementation of cursors.  The code is now well tested and known
	to be efficient.
      </item>
      <item>
        MySQL - a bundle using the mysqlclient library for *recent* MySQL.<br />
	I don't use MySQL ... but the test program ran successfully with a
	vanilla install of the MySQL packages for recent Debian unstable.
      </item>
      <item>
	SQLite - a bundle using the sqlite3 library which supports an
	SQL-like API for direct access to a database file (rather than
	acting as a client of a database server process).<br />
	Not as functional as the other backends (doesn't support dates
	for instance), but good enough for many purposes and very
	'lightweight'.  See http://www.sqlite.org
      </item>
      <item>
        Oracle - a bundle using embedded SQL for Oracle.<br />
	Completely untested ... may even need some work to compile ... but
	this *is* based on code which was working about a year ago.<br />
	No support for BLOBs yet.
      </item>
    </list>
  </section>
  <section>
    <heading>Where can you get it? How can you install it?</heading>
    <p>
      The SQLClient library is currently only available via CVS from the
      GNUstep CVS repository.<br />
      See &lt;https://savannah.gnu.org/cvs/?group=gnustep&gt;<br />
      You need to check out <code>gnustep/dev-libs/SQLClient</code>
    </p>
    <p>
      To build this library you must have a basic GNUstep environment set up ...
    </p>
    <list>
      <item>
	The gnustep-make package must have been built and installed.
      </item>
      <item>
	The gnustep-base package must have been built and installed.
      </item>
      <item>
	The Performance library (from the dev-libs area in GNUstep CVS)
	must have been built and installed.
      </item>
      <item>
	If this environment is in place, all you should need to do is run 'make'
	to configure and build the library, 'make install' to install it.
      </item>
      <item>
	Then you can run the test programs.
      </item>
      <item>
	Your most likely problems are that the configure script may not
	detect the database libraries you want ...  Please figure out how
	to modify <code>configure.ac</code> so that it will detect the
	required headers and libraries on your system, and supply na patch.
      </item>
      <item>
        Once the library is installed, you can include the header file
	<code>&lt;SQLClient/SQLClient.h%gt;</code> and link your programs
       	with the <code>SQLClient</code> library to use it.
      </item>
    </list>
    <p>
      Bug reports, patches, and contributions (eg a backend bundle for a
      new database) should be entered on the GNUstep project page
      &lt;http://savannah.gnu.org/projects/gnustep&gt; and the bug
      reporting page &lt;http://savannah.gnu.org/bugs/?group=gnustep&gt;
    </p>
  </section>
</chapter>

   $Date$ $Revision$
   */ 

#ifndef	INCLUDED_SQLClient_H
#define	INCLUDED_SQLClient_H

#import	<Foundation/NSArray.h>
#import	<Foundation/NSObject.h>

@class	NSConditionLock;
@class	NSCountedSet;
@class	NSMapTable;
@class	NSMutableDictionary;
@class	NSMutableSet;
@class	NSString;
@class	NSThread;

@class	GSCache;
@class	SQLTransaction;

/**
 * Notification sent when an instance becomes connected to the database
 * server.  The notification object is the instance connected.
 */
extern NSString * const SQLClientDidConnectNotification;

/**
 * Notification sent when an instance becomes disconnected from the database
 * server.  The notification object is the instance disconnected.
 */
extern NSString * const SQLClientDidDisconnectNotification;

#if     !defined(SQLCLIENT_PRIVATE)
#define SQLCLIENT_PRIVATE       @private
#endif

/**
 * <p>An enhanced array to represent a record returned from a query.
 * You should <em>NOT</em> try to create instances of this class
 * except via the +newWithValues:keys:count: method.
 * </p>
 * <p>SQLRecord is the abstract base class of a class cluster.
 * If you wish to subclass it you must implement the primitive methods
 * +newWithValues:keys:count: -count -keyAtIndex: -objectAtIndex:
 * and -replaceObjectAtIndex:withObject:
 * </p>
 * <p>NB. You do not need to use SQLRecord (or a subclass of it), all you
 * actually need to supply is a class which responds to the
 * +newWithValues:keys:count: method that the system uses to create
 * new records ... none of the other methods of the SQLRecord class
 * are used internally by the SQLClient system.
 * </p>
 */
@interface SQLRecord : NSArray

/**
 * Create a new SQLRecord containing the specified fields.<br />
 * NB. The values and keys are <em>retained</em> by the record rather
 * than being copied.<br />
 * A nil value is represented by [NSNull null].<br />
 * Keys must be unique string values (case insensitive comparison).
 */
+ (id) newWithValues: (id*)v
		keys: (NSString**)k
	       count: (unsigned int)c;

/**
 * Returns an array containing the names of all the fields in the record.
 */
- (NSArray*) allKeys;

/**
 * Returns the number of items in the record.<br />
 * Subclasses must implement this method.
 */
- (NSUInteger) count;

/**
 * Return the record as a mutable dictionary with the keys as the
 * record field names standardised to be lowercase strings.
 */
- (NSMutableDictionary*) dictionary;

/**
 * Optimised mechanism for retrieving all keys in order.
 */
- (void) getKeys: (id*)buf;

/**
 * Optimised mechanism for retrieving all objects.
 */
- (void) getObjects: (id*)buf;

/** <override-subclass />
 * Returns the key at the specified indes.<br />
 */
- (NSString*) keyAtIndex: (NSUInteger)index;

/** <override-subclass />
 * Returns the object at the specified indes.<br />
 */
- (id) objectAtIndex: (NSUInteger)index;

/**
 * Returns the value of the named field.<br />
 * The field name is case insensitive.
 */
- (id) objectForKey: (NSString*)key;

/**
 * Replaces the value at the specified index.<br />
 * Subclasses must implement this method.
 */
- (void) replaceObjectAtIndex: (NSUInteger)index withObject: (id)anObject;

/**
 * Replaces the value of the named field.<br />
 * The field name is case insensitive.<br />
 * NB. You must be careful not to change the contents of a record which
 * has been cached (unless you are sure you really want to), as you will
 * be changing the contents of the cache, not just a private copy.
 */
- (void) setObject: (id)anObject forKey: (NSString*)aKey;

/**
 * Return approximate size of this record in bytes.<br />
 * The exclude set is used to specify objects to exclude from the
 * calculation (to prevent recursion etc).
 */
- (NSUInteger) sizeInBytes: (NSMutableSet*)exclude;
@end

extern NSString	*SQLException;
extern NSString	*SQLConnectionException;
extern NSString	*SQLEmptyException;
extern NSString	*SQLUniqueException;

/**
 * Returns the timestamp of the most recent call to SQLClientTimeNow().
 */
extern NSTimeInterval SQLClientTimeLast();

/**
 * Convenience function to provide timing information quickly.<br />
 * This returns the current date/time, and stores the value for use
 * by the SQLClientTimeLast() function.
 */
extern NSTimeInterval	SQLClientTimeNow();

/**
 * This returns the timestamp from which any of the SQLClient classes was
 * first used or SQLClientTimeNow() was first called (whichever came first).
 */
extern NSTimeInterval	SQLClientTimeStart();

/**
 * A convenience method to return the current clock 'tick' ... which is
 * the current second based on the time we started.  This does <em>not</em>
 * check the current time, but relies on SQLClientTimeLast() returning an
 * up to date value (so if you need an accurate tick, you should ensure
 * that SQLClientTimeNow() is called at least once a second).<br />
 * The returned value is always greater than zero, and is basically
 * calculated as (SQLClientTimeLast() - SQLClientTimeStart() + 1).<br />
 * In the event that the system clock is reset into the past, the value
 * of SQLClientTimeStart() is automatically adjusted to ensure that the
 * result of a call to SQLClientTimeTick() is never less than the result
 * of any earlier call to the function.
 */
extern unsigned	SQLClientTimeTick();

@class SQLClientPool;

/**
 * <p>The SQLClient class encapsulates dynamic SQL access to relational
 * database systems.  A shared instance of the class is used for
 * each database (as identified by the name of the database), and
 * the number of simultanous database connections is managed too.
 * </p>
 * <p>SQLClient is an abstract base class ... when you create an instance
 * of it, you are actually creating an instance of a concrete subclass
 * whose implementation is loaded from a bundle.
 * </p>
 */
@interface	SQLClient : NSObject
{
SQLCLIENT_PRIVATE
  void			*extra;		/** For subclass specific data */
  NSRecursiveLock	*lock;		/** Maintain thread-safety */
  /**
   * A flag indicating whether this instance is currently connected to
   * the backend database server.  This variable must <em>only</em> be
   * set by the -backendConnect or -backendDisconnect methods.
   */
  BOOL			connected;
  /**
   * A flag indicating whether this instance is currently within a
   * transaction. This variable must <em>only</em> be
   * set by the -begin, -commit or -rollback methods.
   */
  BOOL			_inTransaction;	/** Are we inside a transaction? */
  /**
   * A flag indicating whether leading and trailing white space in values
   * read from the database should automatically be removed.<br />
   * This should only be modified by the -setShouldTrim: method.
   */
  BOOL                  _shouldTrim;    /** Should whitespace be trimmed? */
  NSString		*_name;		/** Unique identifier for instance */
  NSString		*_client;	/** Identifier within backend */
  NSString		*_database;	/** The configured database name/host */
  NSString		*_password;	/** The configured password */
  NSString		*_user;		/** The configured user */
  NSMutableArray	*_statements;	/** Uncommitted statements */
  /**
   * Timestamp of completion of last operation.<br />
   * Maintained by -simpleExecute: -simpleQuery:recordType:listType:
   * and -cache:simpleQuery:recordType:listType:
   * Also set for a failed connection attempt, but not reported by the
   * -lastOperation method in that case.
   */
  NSTimeInterval	_lastOperation;	
  NSTimeInterval	_lastStart;	/** Last op start or connect */
  NSTimeInterval	_duration;      /** Duration logging threshold */
  unsigned int		_debugging;	/** The current debugging level */
  GSCache		*_cache;	/** The cache for query results */
  NSThread		*_cacheThread;	/** Thread for cache queries */
  unsigned int		_connectFails;	/** The count of connection failures */
  NSMapTable            *_observers;    /** Observations of async events */
  NSCountedSet          *_names;        /** Track notification names */
  SQLClientPool         *_pool;         /** The pool of the client (or nil) */
  /** Allow for extensions by allocating memory and pointing to it from
   * the _extra ivar.  That way we can avoid binary incompatibility between
   * minor releases.
   */
  void			*_extra;
}

/**
 * Returns an array containing all the SQLClient instances .
 */
+ (NSArray*) allClients;

/**
 * Return an existing SQLClient instance (using +existingClient:) if possible,
 * or creates one, initialises it using -initWithConfiguration:name:, and
 * returns the new instance (autoreleased).<br />
 * Returns nil on failure.
 */
+ (SQLClient*) clientWithConfiguration: (NSDictionary*)config
				  name: (NSString*)reference;

/**
 * Return an existing SQLClient instance for the specified name
 * if one exists, otherwise returns nil.
 */
+ (SQLClient*) existingClient: (NSString*)reference;

/**
 * Returns the maximum number of simultaneous database connections
 * permitted (set by +setMaxConnections: and defaults to 8) for
 * connections outside of SQLClientPool instances.
 */
+ (unsigned int) maxConnections;

/**
 * <p>Use this method to reduce the number of database connections
 * currently active so that it is less than the limit set by the
 * +setMaxConnections: method.  This mechanism is used internally
 * by the class to ensure that, when it is about to open a new
 * connection, the limit is not exceeded.
 * </p>
 * <p>If since is not nil, then any connection which has not been
 * used more recently than that date is disconnected anyway.<br />
 * You can (and probably should) use this periodically to purge
 * idle connections, but you can also pass a date in the future to
 * close all connections.
 * </p>
 * <p>Purging does not apply to connections made by SQLClientPool
 * instances.
 * </p>
 */
+ (void) purgeConnections: (NSDate*)since;

/**
 * <p>Set the maximum number of simultaneous database connections
 * permitted (defaults to 8 and may not be set less than 1).
 * </p>
 * <p>This value is used by the +purgeConnections: method to determine how
 * many connections should be disconnected when it is called.
 * </p>
 * <p>Connections used by SQLClientPool instances are not considered by
 * this maximum.
 * </p>
 */
+ (void) setMaxConnections: (unsigned int)c;

/**
 * Start a transaction for this database client.<br />
 * You <strong>must</strong> match this with either a -commit
 * or a -rollback.<br />
 * <p>Normally, if you execute an SQL statement without using this
 * method first, the <em>autocommit</em> feature is employed, and
 * the statement takes effect immediately.  Use of this method
 * permits you to execute several statements in sequence, and
 * only have them take effect (as a single operation) when you
 * call the -commit method.
 * </p>
 * <p>NB. You must <strong>not</strong> execute an SQL statement
 * which would start a transaction directly ... use only this
 * method.
 * </p>
 * <p>Where possible, consider using the [SQLTransaction] class rather
 * than calling -begin -commit or -rollback yourself.
 * </p>
 */
- (void) begin;

/** This grabs the receiver for use by the current thread.<br />
 * If limit is nil or in the past, makes a single immediate attempt.<br />
 * Returns NO if it fails to obtain a lock by the specified date.<br /> 
 * Must be matched by an -unlock if it succeeds.
 */
- (BOOL) lockBeforeDate: (NSDate*)limit;

/**
 * <p>Build an sql query string using the supplied arguments.
 * </p>
 * <p>This method has at least one argument, the string starting the
 * query to be executed (which must have the prefix 'select ').
 * </p>
 * <p>Additional arguments are a nil terminated list which also be strings,
 * and these are appended to the statement.<br />
 * Any string arguments are assumed to have been quoted appropriately
 * already, but non-string arguments are automatically quoted using the
 * -quote: method.
 * </p>
 * <example>
 *   sql = [db buildQuery: @"SELECT Name FROM ", table, nil];
 * </example>
 * <p>Upon error, an exception is raised.
 * </p>
 * <p>The method returns a string containing sql suitable for passing to
 * the -simpleQuery:recordType:listType:
 * or -cache:simpleQuery:recordType:listType: methods.
 * </p>
 */
- (NSString*) buildQuery: (NSString*)stmt,...;

/**
 * Takes the query statement and substitutes in values from
 * the dictionary where markup of the format {key} is found.<br />
 * Returns the resulting query string.
 * <example>
 *   sql = [db buildQuery: @"SELECT Name FROM {Table} WHERE ID = {ID}"
 *                   with: values];
 * </example>
 * <p>Any non-string values in the dictionary will be replaced by
 * the results of the -quote: method.<br />
 * The markup format may also be {key?default} where <em>default</em>
 * is a string to be used if there is no value for the <em>key</em>
 * in the dictionary.
 * </p>
 * <p>The method returns a string containing sql suitable for passing to
 * the -simpleQuery:recordType:listType:
 * or -cache:simpleQuery:recordType:listType: methods.
 * </p>
 */
- (NSString*) buildQuery: (NSString*)stmt with: (NSDictionary*)values;

/**
 * Return the client name for this instance.<br />
 * Normally this is useful only for debugging/reporting purposes, but
 * if you are using multiple instances of this class in your application,
 * and you are using embedded SQL, you will need to use this
 * method to fetch the client/connection name and store its C-string
 * representation in a variable 'connectionName' declared to the sql
 * preprocessor, so you can then have statements of the form -
 * 'exec sql at :connectionName ...'.
 */
- (NSString*) clientName;

/**
 * Complete a transaction for this database client.<br />
 * This <strong>must</strong> match an earlier -begin.
 * <p>NB. You must <strong>not</strong> execute an SQL statement
 * which would commit or rollback a transaction directly ... use
 * only this method or the -rollback method.
 * </p>
 * <p>Where possible, consider using the [SQLTransaction] class rather
 * than calling -begin -commit or -rollback yourself.
 * </p>
 */
- (void) commit;

/**
 * If the <em>connected</em> instance variable is NO, this method
 * calls -backendConnect to ensure that there is a connection to the
 * database server established. Returns the result.<br />
 * Performs any necessary locking for thread safety.<br />
 * This method also counts the number of consecutive failed connection
 * attempts.  A delay is enforced between each connection attempt, with
 * the length of the delay growing with each failure.  This ensures
 * that applications which fail to deal with connection failures, and
 * just keep trying to reconnect, will not overload the system/server.<br />
 * The maximum delay is 30 seconds, so when the database server is restarted,
 * the application can reconnect reasonably quickly.
 */
- (BOOL) connect;

/**
 * Return a flag to say whether a connection to the database server is
 * currently live (the value of the 'connected' instance variable).<br />
 * This is mostly useful for debug/reporting.
 */
- (BOOL) connected;

/**
 * Return the database name for this instance (or nil).
 */
- (NSString*) database;

/**
 * If the <em>connected</em> instance variable is YES, this method
 * calls -backendDisconnect to ensure that the connection to the
 * database server is dropped.<br />
 * Performs any necessary locking for thread safety.
 */
- (void) disconnect;

/**
 * Perform arbitrary operation <em>which does not return any value.</em><br />
 * This arguments to this method are a nil terminated list which are
 * concatenated in the manner of the -query:,... method.<br />
 * Any string arguments are assumed to have been quoted appropriately
 * already, but non-string arguments are automatically quoted using the
 * -quote: method.
 * <example>
 *   [db execute: @"UPDATE ", table, @" SET Name = ",
 *     myName, " WHERE ID = ", myId, nil];
 * </example>
 * Where the database backend support it, this method returns the count of
 * the number of rows to which the operation applied.  Otherwise this
 * returns -1.
 */
- (NSInteger) execute: (NSString*)stmt,...;

/**
 * Takes the statement and substitutes in values from
 * the dictionary where markup of the format {key} is found.<br />
 * Passes the result to the -execute:,... method.
 * <example>
 *   [db execute: @"UPDATE {Table} SET Name = {Name} WHERE ID = {ID}"
 *          with: values];
 * </example>
 * Any non-string values in the dictionary will be replaced by
 * the results of the -quote: method.<br />
 * The markup format may also be {key?default} where <em>default</em>
 * is a string to be used if there is no value for the <em>key</em>
 * in the dictionary.<br />
 * Where the database backend support it, this method returns the count of
 * the number of rows to which the operation applied.  Otherwise this
 * returns -1.
 */
- (NSInteger) execute: (NSString*)stmt with: (NSDictionary*)values;

/**
 * Calls -initWithConfiguration:name: passing a nil reference name.
 */
- (id) initWithConfiguration: (NSDictionary*)config;

/**
 * Calls -initWithConfiguration:name:pool: passing NO to say the client is
 * not in a pool.
 */
- (id) initWithConfiguration: (NSDictionary*)config
			name: (NSString*)reference;

/**
 * Initialise using the supplied configuration, or if that is nil, try to
 * use values from NSUserDefaults (and automatically update when the
 * defaults change).<br />
 * Uses the reference name to determine configuration information ... and if
 * a nil name is supplied, defaults to the value of SQLClientName in the
 * configuration dictionary (or in the standard user defaults).  If there is
 * no value for SQLClientName, uses the string 'Database'.<br />
 * If pool is nil and a SQLClient instance already exists with the
 * name used for this instance, the receiver is deallocated and the existing
 * instance is retained and returned ... there may only ever be one instance
 * for a particular reference name which is not in a pool.<br />
 * <br />
 * The config argument (or the SQLClientReferences user default)
 * is a dictionary with names as keys and dictionaries
 * as its values.  Configuration entries from the dictionary corresponding
 * to the database client are used if possible, general entries are used
 * otherwise.<br />
 * Database ... is the name of the database to use, if it is missing
 * then 'Database' may be used instead.<br />
 * User ... is the name of the database user to use, if it is missing
 * then 'User' may be used instead.<br />
 * Password ... is the name of the database user password, if it is
 * missing then 'Password' may be used instead.<br />
 * ServerType ... is the name of the backend server to be used ... by
 * convention the name of a bundle containing the interface to that backend.
 * If this is missing then 'Postgres' is used.<br />
 * The database name may be of the format 'name@host:port' when you wish to
 * connect to a database on a different host over the network.
 */
- (id) initWithConfiguration: (NSDictionary*)config
			name: (NSString*)reference
                        pool: (SQLClientPool*)pool;

/** Two clients are considered equal if they refer to the same database
 * and are logged in as the same database user using the same protocol.
 * These are the general criteria for transactions to be compatoible so
 * that an SQLTransaction object generated by one client can be used by
 * the other.
 */
- (BOOL) isEqual: (id)other;

/**
 * Return the state of the flag indicating whether the library thinks
 * a transaction is in progress.  This flag is normally maintained by
 * -begin, -commit, and -rollback.
 */
- (BOOL) isInTransaction;

/**
 * Returns the date/time stamp of the last database operation performed
 * by the receiver, or nil if no operation has ever been done by it.<br />
 * Simply connecting to or disconnecting from the databsse does not
 * count as an operation.
 */
- (NSDate*) lastOperation;

/** Compares the receiver with the other client to see which one has been
 * inactive but connected for longest (if they are connected) and returns
 * that instance.<br />
 * If neither is idle but connected, the method returns nil.<br />
 * In a tie, the method returns the other instance.
 */
- (SQLClient*) longestIdle: (SQLClient*)other;

/**
 * Return the database reference name for this instance (or nil).
 */
- (NSString*) name;

/**
 * Return the database password for this instance (or nil).
 */
- (NSString*) password;

/**
 * This is the method used to convert a query or statement to a standard
 * form used internally by other methods.<br />
 * This works to build an sql string by quoting any non-string objects
 * and concatenating the resulting strings in a nil terminated list.<br />
 * Returns an array containing the statement as the first object and
 * any NSData objects following.  The NSData objects appear in the
 * statement strings as the marker sequence - <code>'?'''?'</code><br />
 * If the returned array contains a single object, that object is a
 * simple SQL query/statement.
 */
- (NSMutableArray*) prepare: (NSString*)stmt args: (va_list)args;

/**
 * <p>Perform arbitrary query <em>which returns values.</em>
 * </p>
 * <p>This method handles its arguments in the same way as the -buildQuery:,...
 * method and returns the result of the query.
 * </p>
 * <example>
 *   result = [db query: @"SELECT Name FROM ", table, nil];
 * </example>
 * <p>Upon error, an exception is raised.
 * </p>
 * <p>The query returns an array of records (each of which is represented
 * by an SQLRecord object).
 * </p>
 * <p>Each SQLRecord object contains one or more fields, in the order in
 * which they occurred in the query.  Fields may also be retrieved by name.
 * </p>
 * <p>NULL field items are returned as NSNull objects.
 * </p>
 * <p>Most other field items are returned as NSString objects.
 * </p>
 * <p>Date and timestamp field items are returned as NSDate objects.
 * </p>
 */
- (NSMutableArray*) query: (NSString*)stmt,...;

/**
 * Takes the query statement and substitutes in values from
 * the dictionary (in the same manner as the -buildQuery:with: method)
 * then executes the query and returns the response.<br />
 * <example>
 *   result = [db query: @"SELECT Name FROM {Table} WHERE ID = {ID}"
 *                 with: values];
 * </example>
 * Any non-string values in the dictionary will be replaced by
 * the results of the -quote: method.<br />
 * The markup format may also be {key?default} where <em>default</em>
 * is a string to be used if there is no value for the <em>key</em>
 * in the dictionary.
 */
- (NSMutableArray*) query: (NSString*)stmt with: (NSDictionary*)values;

/**
 * Convert an object to a string suitable for use in an SQL query.<br />
 * Normally the -execute:,..., and -query:,... methods will call this
 * method automatically for everything apart from string objects.<br />
 * Strings have to be handled specially, because they are used both for
 * parts of the SQL command, and as values (where they need to be quoted).
 * So where you need to pass a string value which needs quoting,
 * you must call this method explicitly.<br />
 * Subclasses may override this method to provide appropriate quoting for
 * types of object which need database backend specific quoting conventions.
 * However, the defalt implementation should be OK for most cases.<br />
 * This method makes use of -quoteString: to quote literal strings.<br />
 * The base class implementation formats NSDate objects as<br />
 * YYYY-MM-DD hh:mm:ss.mmm ?ZZZZ<br />
 * NSData objects are not quoted ... they must not appear in queries, and
 * where used for insert/update operations, they need to be passed to the
 * -backendExecute: method unchanged.<br />
 * NSArray and NSSet objects are quoted as sets containing the quoted
 * elements from the array/set.  If you want to use SQL arrays (and your
 * database backend supports it) you must explicitly use the
 * -quoteArray:toString:quotingString: to convert an NSArray to a literal
 * database array representation.
 */
- (NSString*) quote: (id)obj;

/**
 * Produce a quoted string from the supplied arguments (printf style).
 */
- (NSString*) quotef: (NSString*)fmt, ...;

/* Produce a quoted string from an array on databases where arrays are
 * supported (currently only Postgres).<br />
 * If the s argument is not nil, the quoted array is appended to it rather
 * than being produced in a new string (this method uses that feature to
 * recursively quote nested arrays).<br />
 * The q argument determines whether string values found in the array
 * are quoted or added literally.
 */
- (NSMutableString*) quoteArray: (NSArray *)a
                       toString: (NSMutableString *)s
                 quotingStrings: (BOOL)q;

/**
 * Convert a big (64 bit) integer to a string suitable for use in an SQL query.
 */
- (NSString*) quoteBigInteger: (int64_t)i;

/**
 * Convert a 'C' string to a string suitable for use in an SQL query
 * by using -quoteString: to convert it to a literal string format.<br />
 * NB. a null pointer is treated as an empty string.
 */
- (NSString*) quoteCString: (const char *)s;

/**
 * Convert a single character to a string suitable for use in an SQL query
 * by using -quoteString: to convert it to a literal string format.<br />
 * NB. a nul character is not allowed and will cause an exception.
 */
- (NSString*) quoteChar: (char)c;

/**
 * Convert a float to a string suitable for use in an SQL query.
 */
- (NSString*) quoteFloat: (float)f;

/**
 * Convert an integer to a string suitable for use in an SQL query.
 */
- (NSString*) quoteInteger: (int)i;

/**
 * Convert a string to a form suitable for use as a string
 * literal in an SQL query.<br />
 * Subclasses may override this for non-standard literal string
 * quoting conventions.
 */
- (NSString*) quoteString: (NSString *)s;

/**
 * Revert a transaction for this database client.<br />
 * If there is no transaction in progress, this method does nothing.<br />
 * <p>NB. You must <strong>not</strong> execute an SQL statement
 * which would commit or rollback a transaction directly ... use
 * only this method or the -rollback method.
 * </p>
 * <p>Where possible, consider using the [SQLTransaction] class rather
 * than calling -begin -commit or -rollback yourself.
 * </p>
 */
- (void) rollback;

/**
 * Set the database host/name for this object.<br />
 * This is called automatically to configure the connection ...
 * you normally shouldn't need to call it yourself.
 */
- (void) setDatabase: (NSString*)s;

/**
 * Set the database reference name for this object.  This is used to
 * differentiate between multiple connections to the database.<br />
 * This is called automatically to configure the connection ...
 * you normally shouldn't need to call it yourself.<br />
 * NB. attempts to change the name of an instance to that of an existing
 * instance are ignored.
 */
- (void) setName: (NSString*)s;

/**
 * Set the database password for this object.<br />
 * This is called automatically to configure the connection ...
 * you normally shouldn't need to call it yourself.
 */
- (void) setPassword: (NSString*)s;

/** Sets an internal flag to indicate whether leading and trailing white
 * space characters should be removed from values retrieved from the
 * database by the receiver.
 */
- (void) setShouldTrim: (BOOL)aFlag;

/**
 * Set the database user for this object.<br />
 * This is called automatically to configure the connection ...
 * you normally shouldn't need to call it yourself.
 */
- (void) setUser: (NSString*)s;

/**
 * Calls -backendExecute: in a safe manner.<br />
 * Handles locking.<br />
 * Maintains -lastOperation date.<br />
 * Returns the result of -backendExecute:
 */
- (NSInteger) simpleExecute: (NSArray*)info;

/**
 * Calls -simpleQuery:recordType:listType: with the default record class
 * and default array class.
 */
- (NSMutableArray*) simpleQuery: (NSString*)stmt;

/**
 * Calls -backendQuery:recordType:listType: in a safe manner.<br />
 * Handles locking.<br />
 * Maintains -lastOperation date.<br />
 * The value of rtype must respond to the
 * [SQLRecord+newWithValues:keys:count:] method.<br />
 * If rtype is nil then the [SQLRecord] class is used.<br />
 * The value of ltype must respond to the [NSObject+alloc] method to produce
 * a container which must repond to the [NSMutableArray-initWithCapacity:]
 * method to initialise itsself and the [NSMutableArray-addObject:] method
 * to add records to the list.<br />
 * If ltype is nil then the [NSMutableArray] class is used.<br />
 * This library provides a few helper classes to provide alternative
 * values for rtype and ltype.
 */
- (NSMutableArray*) simpleQuery: (NSString*)stmt
		     recordType: (id)rtype
		       listType: (id)ltype;

/** Releases a lock previously obtained using -lockbeforeDate:
 */
- (void) unlock;

/**
 * Return the database user for this instance (or nil).
 */
- (NSString*) user;
@end

/**
 * This category contains the methods which a subclass <em>must</em>
 * override to provide a working instance, and helper methods for the
 * backend implementations.<br />
 * Application programmers should <em>not</em> call the backend
 * methods directly.<br />
 * <p>When subclassing to produce a backend driver bundle, please be
 * aware that the subclass must <em>NOT</em> introduce additional
 * instance variables.  Instead the <em>extra</em> instance variable
 * is provided for use as a pointer to subclass specific data.
 * </p>
 */
@interface	SQLClient(Subclass)

/** <override-subclass />
 * Attempts to establish a connection to the database server.<br />
 * Returns a flag to indicate whether the connection has been established.<br />
 * If a connection was already established, returns YES and does nothing.<br />
 * You should not need to use this method normally, as it is called for you
 * automatically when necessary.<br />
 * <p>Subclasses <strong>must</strong> implement this method to establish a
 * connection to the database server process (and initialise the
 * <em>extra</em> instance variable if necessary), setting the
 * <em>connected</em> instance variable to indicate the state of the object.
 * </p>
 * <p>This method must call +purgeConnections: to ensure that there is a
 * free slot for the new connection.
 * </p>
 * <p>Application code must <em>not</em> call this method directly, it is
 * for internal use only.  The -connect method calls this method if the
 * <em>connected</em> instance variable is NO.
 * </p>
 */
- (BOOL) backendConnect;

/** <override-subclass />
 * Disconnect from the database unless already disconnected.<br />
 * <p>This method is called automatically when the receiver is deallocated
 * or reconfigured, and may also be called automatically when there are
 * too many database connections active.
 * </p>
 * <p>If the receiver is an instance of a subclass which uses the
 * <em>extra</em> instance variable, it <strong>must</strong> clear that
 * variable in the -backendDisconnect method, because a reconfiguration
 * may cause the class of the receiver to change.
 * </p>
 * <p>This method must set the <em>connected</em> instance variable to NO.
 * </p>
 * <p>Application code must <em>not</em> call this method directly, it is
 * for internal use only.  The -disconnect method calls this method if the
 * <em>connected</em> instance variable is YES.
 * </p>
 */
- (void) backendDisconnect;

/** <override-subclass />
 * Perform arbitrary operation <em>which does not return any value.</em><br />
 * This method has a single argument, an array containing the string
 * representing the statement to be executed as its first object, and an
 * optional sequence of data objects following it.<br />
 * <example>
 *   [db backendExecute: [NSArray arrayWithObject:
 *     @"UPDATE MyTable SET Name = 'The name' WHERE ID = 123"]];
 * </example>
 * <p>The backend implementation is required to perform the SQL statement
 * using the supplied NSData objects at the points in the statement
 * marked by the <code>'?'''?'</code> sequence.  The marker saequences are
 * inserted into the statement at an earlier stage by the -execute:,...
 * and -execute:with: methods.
 * </p>
 * <p>Callers should lock the instance using the <em>lock</em>
 * instance variable for the duration of the operation, and unlock
 * it afterwards.
 * </p>
 * <p>NB. callers (other than the -begin, -commit, and -rollback methods)
 * should not pass any statement to this method which would cause a
 * transaction to begin or end.
 * </p>
 * <p>Application code must <em>not</em> call this method directly, it is
 * for internal use only.
 * </p>
 * <p>Where the database backend support it, this method returns the count of
 * the number of rows to which the operation applied.  Otherwise this
 * returns -1.
 * </p>
 */
- (NSInteger) backendExecute: (NSArray*)info;

/** <override-subclass />
 * <p>Perform arbitrary query <em>which returns values.</em>
 * </p>
 * <example>
 *   result = [db backendQuery: @"SELECT Name FROM Table"
 *                  recordType: [SQLRecord class]]
 *                    listType: [NSMutableArray class]];
 * </example>
 * <p>Upon error, an exception is raised.
 * </p>
 * <p>The query returns an array of records (each of which is represented
 * by an SQLRecord object).
 * </p>
 * <p>Each SQLRecord object contains one or more fields, in the order in
 * which they occurred in the query.  Fields may also be retrieved by name.
 * </p>
 * <p>NULL field items are returned as NSNull objects.
 * </p>
 * <p>Callers should lock the instance using the <em>lock</em>
 * instance variable for the duration of the operation, and unlock
 * it afterwards.
 * </p>
 * <p>Application code must <em>not</em> call this method directly, it is
 * for internal use only.
 * </p>
 * <p>The rtype argument specifies an object to be used to
 * create the records produced by the query.<br />
 * This is provided as a performance optimisation when you want to store
 * data directly into a special class of your own.<br />
 * The object must respond to the [SQLRecord +newWithValues:keys:count:]
 * method to produce a new record initialised with the supplied data.
 * </p>
 * <p>The ltype argument specifies an object to be used to create objects to
 * store the records produced by the query.<br />
 * The should be a subclass of NSMutableArray.  It must at least
 * implement the [NSObject+alloc] method to create an instnce to store
 * records.  The instance must implement [NSMutableArray-initWithCapacity:]
 * to initialise itsself and [NSMutableArray-addObject:] to allow the
 * backend to add records to it.<br />
 * For caching to work, it must be possible to make a mutable copy of the
 * instance using the mutableCopy method.
 * </p>
 */
- (NSMutableArray*) backendQuery: (NSString*)stmt
		      recordType: (id)rtype
		        listType: (id)ltype;

/**
 * Calls -backendQuery:recordType:listType: with the default record class
 * and array class.
 */
- (NSMutableArray*) backendQuery: (NSString*)stmt;

/** <override-subclass />
 * Called to enable asynchronous notification of database events using the
 * specified name (which must be a valid identifier consisting of ascii
 * letters, digits, and underscore characters, starting with a letter).
 * Names are not case sensitive (so AAA is the same as aaa).<br />
 * Repeated calls to list on the same name should be treated as a single
 * call.<br />
 * The backend is responsible for implicitly unlistening when a connection
 * is closed.<br />
 * There is a default implementation which does nothing ... for backends
 * which don't support asynchronous notifications.<br />
 * If a backend <em>does</em> support asynchronous notifications,
 * it should do so by posting NSNotification instances to the main thread
 * [NSNotificationQueue defaultQueue] with the posting style NSPostASAP
 * (to post asynchronously) and using the SQLClient instance as
 * the notification object and supplying any payload as a string using
 * the 'Payload' key in the NSNotification userInfo dictionary.
 * The userInfo dictionary should also contain a boolean (NSNumber) value,
 * using the 'Local' key, to indicate whether the notification was sent by
 * the current SQLClient instance or by some other client/
 */
- (void) backendListen: (NSString*)name;

/** <override-subclass />
 * The backend should implement this to send asynchronous notifications
 * to anything listening for them. The name of the notification is an
 * SQL identifier used for listening for the asynchronous data.<br />
 * The payload string may be nil if no additional information is
 * needed in the notification.
 */
- (void) backendNotify: (NSString*)name payload: (NSString*)more;

/** <override-subclass />
 * Called to disable asynchronous notification of database events using the
 * specified name.  This has no effect if the name has not been used in an
 * earlier call to -backendListen:, or if the name has already been
 * unlistened since the last call to listen. on it.<br />
 * There is a default implementation which does nothing ... for backends
 * which don't support asynchronous notifications.
 */
- (void) backendUnlisten: (NSString*)name;

/** <override-subclass />
 * This method is <em>only</em> for the use of the
 * -insertBLOBs:intoStatement:length:withMarker:length:giving:
 * method.<br />
 * Subclasses which need to insert binary data into a statement
 * must implement this method to copy the escaped data into place
 * and return the number of bytes actually copied.
 */
- (unsigned) copyEscapedBLOB: (NSData*)blob into: (void*)buf;

/**
 * <p>This method is a convenience method provided for subclasses which need
 * to insert escaped binary data into an SQL statement before sending the
 * statement to a backend server process.  This method makes use of the
 * -copyEscapedBLOB:into: and -lengthOfEscapedBLOB: methods, which
 *  <em>must</em> be implemented by the subclass.
 * </p>
 * <p>The blobs array is an array containing the original SQL statement
 * string (unused by this method) followed by the data items to be inserted.
 * </p>
 * <p>The statement and sLength arguments specify the datastream to be
 * copied and into which the BLOBs are to be inserted.
 * </p>
 * <p>The marker and mLength arguments specify the sequence of marker bytes
 * in the statement which indicate a position for insertion of an escaped BLOB.
 * </p>
 * <p>The method returns either the original statement or a copy containing
 * the escaped BLOBs.  The length of the returned data is stored in result.
 * </p>
 */
- (const void*) insertBLOBs: (NSArray*)blobs
	      intoStatement: (const void*)statement
		     length: (unsigned)sLength
		 withMarker: (const void*)marker
		     length: (unsigned)mLength
		     giving: (unsigned*)result;

/** <override-subclass />
 * This method is <em>only</em> for the use of the
 * -insertBLOBs:intoStatement:length:withMarker:length:giving:
 * method.<br />
 * Subclasses which need to insert binary data into a statement
 * must implement this method to return the length of the escaped
 * bytestream which will be inserted.
 */
- (unsigned) lengthOfEscapedBLOB: (NSData*)blob;

@end

/**
 * This category contains methods for asynchronous notification of 
 * events via the database (for those database backends which support
 * it: currently only PostgreSQL).
 */
@interface	SQLClient (Notifications)
/** Adds anObserver to receive notifications when the backend database
 * server sends an asynchronous event identified by the specified name
 * (which must be a valid database identifier).<br />
 * When a notification (NSNotification instance) is received by the method
 * specified by aSelector, its <em>object</em> will be the SQLClient
 * instance to which anObserver was added and its userInfo dictionary
 * will contain the key 'Local' and possibly the key 'Payload'.<br />
 * If the 'Local' value is the boolean YES, the notification originated
 * as an action by this SQLClient instance.<br />
 * If the 'Payload' value is not nil, then it is a string providing extra
 * information about the notification.<br />
 * Notifications are posted asynchronously using the default notification
 * queue for the current thread, so they should be delivered to the
 * observer after the database statement in which they were detected
 * has completed.  However, delivery of the notification could still
 * occur inside a transaction is the -begin and -commit statements
 * are used.  For this reason, observing code may want to use the
 * -lockBeforeDate: -isInTransaction and -unlock methods to ensure
 * that they don't interfere with ongoing transactions.
 */
- (void) addObserver: (id)anObserver
            selector: (SEL)aSelector
                name: (NSString*)name;

/** Posts a notification via the database.  The name is an SQL identifier
 * (for which observers may have registered) and the extra payload
 * information may be nil if not required.
 */
- (void) postNotificationName: (NSString*)name payload: (NSString*)more;

/** Removes anObserver as an observer for asynchronous notifications from
 * the database server.  If name is omitted, the observer will be removed
 * for all names.
 */
- (void) removeObserver: (id)anObserver name: (NSString*)name;
@end

/**
 * This category contains convenience methods including those for
 * frequently performed database operations ... message logging etc.
 */
@interface	SQLClient (Convenience)

/**
 * Convenience method to deal with the results of a query converting the
 * normal array of records into an array of record columns.  Each column
 * in the array is an array containing all the values from that column.
 */
+ (NSMutableArray*) columns: (NSMutableArray*)records;

/**
 * Convenience method to deal with the results of a query where each
 * record contains a single field ... it converts the array of records
 * returned by the query to an array containing the fields.<br />
 * NB. This does not check that the contents of the records array are
 * actually instances of [SQLRecord], so you must ensure you don't
 * call it more than once on the same array (something that may happen
 * if you retrieve the array using a cache based query).
 */
+ (void) singletons: (NSMutableArray*)records;

/**
 * Returns a transaction object configured to handle batching and
 * execute part of a batch of statements if execution of the whole
 * using the [SQLTransaction-executeBatch] method fails.<br />
 * If stopOnFailure is YES than execution of the transaction will
 * stop with the first statement to fail, otherwise it will execute
 * all the statements it can, skipping any failed statements.
 */
- (SQLTransaction*) batch: (BOOL)stopOnFailure;

/** The same as the [SQLClient+columns:] method.
 */
- (NSMutableArray*) columns: (NSMutableArray*)records;

/**
 * Executes a query (like the -query:,... method) and checks the result
 * (raising an exception if the query did not contain a single record)
 * and returns the resulting record.
 */
- (SQLRecord*) queryRecord: (NSString*)stmt,...;

/**
 * Executes a query (like the -query:,... method) and checks the result.<br />
 * Raises an exception if the query did not contain a single record, or
 * if the record did not contain a single field.<br />
 * Returns the resulting field as a <em>string</em>.
 */
- (NSString*) queryString: (NSString*)stmt,...;

/** The same as the [SQLClient+singletons:] method.
 */
- (void) singletons: (NSMutableArray*)records;

/**
 * Creates and returns an autoreleased SQLTransaction instance  which will
 * use the receiver as the database connection to perform transactions.
 */
- (SQLTransaction*) transaction;

@end



/**
 * This category porovides basic methods for logging debug information.
 */
@interface      SQLClient (Logging)
/**
 * Return the class-wide debugging level, which is inherited by all
 * newly created instances.
 */
+ (unsigned int) debugging;

/**
 * Return the class-wide duration logging threshold, which is inherited by all
 * newly created instances.
 */
+ (NSTimeInterval) durationLogging;

/**
 * Set the debugging level to be inherited by all new instances.<br />
 * See [SQLClient(Logging)-setDebugging:]
 * for controlling an individual instance of the class.
 */
+ (void) setDebugging: (unsigned int)level;

/**
 * Set the duration logging threshold to be inherited by new instances.<br />
 * See [SQLClient(Logging)-setDurationLogging:]
 * for controlling an individual instance of the class.
 */
+ (void) setDurationLogging: (NSTimeInterval)threshold;

/**
 * The default implementation calls NSLogv to log a debug message.<br />
 * Override this in a category to provide more sophisticated logging.<br />
 * Do NOT override with code which can be slow or which calls (directly
 * or indirectly) any SQLCLient methods, since this method will be used
 * inside locked regions of the SQLClient code and you could cause
 * deadlocks or long delays to other threads using the class.
 */
- (void) debug: (NSString*)fmt, ...;

/**
 * Return the current debugging level.<br />
 * A level of zero (default) means that no debug output is produced,
 * except for that concerned with logging the database transactions
 * taking over a certain amount of time (see the -setDurationLogging: method).
 */
- (unsigned int) debugging;

/**
 * Returns the threshold above which queries and statements taking a long
 * time to execute are logged.  A negative value (default) indicates that
 * this logging is disabled.  A value of zero means that all statements
 * are logged.
 */
- (NSTimeInterval) durationLogging;

/**
 * Set the debugging level of this instance ... overrides the default
 * level inherited from the class.
 */
- (void) setDebugging: (unsigned int)level;

/**
 * Set a threshold above which queries and statements taking a long
 * time to execute are logged.  A negative value (default) disables
 * this logging.  A value of zero logs all statements.
 */
- (void) setDurationLogging: (NSTimeInterval)threshold;
@end

/**
 * This category provides methods for caching the results of queries
 * in order to reduce the number of client-server trips and the database
 * load produced by an application which needs update its information
 * from the database frequently.
 */
@interface      SQLClient (Caching)

/**
 * Returns the cache used by the receiver for storing the results of
 * requests made through it.  Creates a new cache if necessary.
 */
- (GSCache*) cache;

/** Returns an autoreleased mutable copy of the cached object corresponding
 * to the supplied query/statement (or nil if no such object is cached).
 */
- (NSMutableArray*) cacheCheckSimpleQuery: (NSString*)stmt;

/**
 * Calls -cache:simpleQuery:recordType:listType: with the default
 * record class, array class, and with a query string formed from
 * stmt and the following values (if any).
 */
- (NSMutableArray*) cache: (int)seconds
		    query: (NSString*)stmt,...;

/**
 * Calls -cache:simpleQuery:recordType:listType: with the default
 * record class array class and with a query string formed from stmt
 * and values.
 */
- (NSMutableArray*) cache: (int)seconds
		    query: (NSString*)stmt
		     with: (NSDictionary*)values;

/**
 * Calls -cache:simpleQuery:recordType:listType: with the default
 * record class and array class.
 */
- (NSMutableArray*) cache: (int)seconds simpleQuery: (NSString*)stmt;

/**
 * If the result of the query is already cached and has not expired,
 * return an autoreleased mutable copy. Otherwise, perform the query
 * and cache the result giving it the specified lifetime in seconds.<br />
 * If seconds is negative, the query is performed irrespective of
 * whether it is already cached, and its absolute value is used to
 * set the lifetime of the results.<br />
 * If seconds is zero, the cache for this query is emptied.<br />
 * Handles locking.<br />
 * Maintains -lastOperation date.<br />
 * The value of rtype must respond to the
 * [SQLRecord+newWithValues:keys:count:] method.<br />
 * If rtype is nil then the [SQLRecord] class is used.<br />
 * The value of ltype must respond to the [NSObject+alloc] method to produce
 * a container which must repond to the [NSMutableArray-initWithCapacity:]
 * method to initialise itsself and the [NSMutableArray-addObject:] method
 * to add records to the list.<br />
 * If ltype is nil then the [NSMutableArray] class is used.<br />
 * The list produced by this argument is used as the return value of
 * this method.<br /> 
 * NB. cache lookups for the instance created from ltype will be provided
 * by sending -mutableCopy and -autorelease messages to the original
 * instance.<br />
 * If a cache thread has been set using the -setCacheThread: method, and the
 * -cache:simpleQuery:recordType:listType: method is called from a
 * thread other than the cache thread, then any query to retrieve
 * uncached data will be performed in the cache thread, and for cached
 * (but expired) data, the old (expired) results may be returned ...
 * in which case an asynchronous query to update the cache will be
 * executed as soon as possible in the cache thread.
 */
- (NSMutableArray*) cache: (int)seconds
	      simpleQuery: (NSString*)stmt
	       recordType: (id)rtype
	         listType: (id)ltype;

/**
 * Sets the cache to be used by the receiver for storing the results of
 * requests made through it.<br />
 * If aCache is nil, the current cache is released, and a new cache will
 * be automatically created as soon as there is a need to cache anything.
 */
- (void) setCache: (GSCache*)aCache;

/** Sets the thread to be used to retrieve data to populate the cache.<br />
 * All cached queries will be performed in this thread (if non-nil).<br />
 * The setting of a thread for the cache also implies that expired items in
 * the cache may not be removed when they are queried from another thread,
 * rather they can be kept (if they are not <em>too</em> old) and an
 * asynchronous query to update them will be run on the cache thread.<br />
 * The rule is that, if the item's age is more than twice its nominal
 * lifetime, it will be retrieved immediately, otherwise it will be
 * retrieved asynchronously.<br />
 * Currently this may only be the main thread or nil.  Any attempt to set
 * another thread will use the main thread instead.
 */
- (void) setCacheThread: (NSThread*)aThread;
@end

/** <p>An SQLClientPool instance may be used to create/control a pool of
 * client objects.  Code may obtain autoreleased proxies to the clients
 * from the pool and use them safe in the knowledge that they won't be
 * used anywhere else ... as soon as the client would be deallocated, it
 * is returned to the pool.
 * </p>
 * <p>All clients in the pool share the same cache object, so query results
 * cached by one client will be available to other clients in the pool.
 * </p>
 * <p>As a convenience, an SQLClientPool instance acts as a proxy for the
 * clients it contains, so you may (where it makes sense) send the same
 * messages to a pool that you would send to an individual client, and the
 * pool will temporarily allocate one of its clients to handle it.<br />
 * In this case the client will be returned to the pool immediately
 * after the message has been handled (and subsequent messages may go
 * to a different client), so you can't change settings or send any other
 * method which would be required to be followed up by another message to
 * the same client.
 * </p>
 */
@interface	SQLClientPool : NSObject
{
  NSConditionLock       *lock;  /** Controls access to the pool contents */
  SQLClient             **c;    /** The clients of the pool. */
  SQLClient             *q;     /** The clients used for quoting. */
  BOOL                  *u;     /** Whether the client is in use. */
  int                   max;    /** Maximum connection count */
  int                   min;    /** Minimum connection count */
  NSDictionary          *_config;       /** The pool configuration object */
  NSString              *_name;         /** The pool configuration name */
  NSTimeInterval	_duration;      /** Duration logging threshold */
  unsigned int		_debugging;	/** The current debugging level */
  uint64_t              _immediate;     /** Immediate client provisions */
  uint64_t              _delayed;       /** Count of delayed provisions */
  uint64_t              _failed;        /** Count of timed out provisions */
  NSTimeInterval        _longest;       /** Count of longest delay */
  NSTimeInterval        _delayWaits;    /** Time waiting for provisions */
  NSTimeInterval        _failWaits;     /** Time waiting for timewouts */
}

/** Returns the count of currently available connections in the pool.
 */
- (int) availableConnections;

/**
 * Returns the cache used by clients in the pool for storing the results of
 * requests made through them.  Creates a new cache if necessary.
 */
- (GSCache*) cache;

/**
 * Creates a pool of clients using a single client configuration.<br />
 * Calls -initWithConfiguration:name:pool: (passing NO to say the client
 * is not in a pool) to create each client.<br />
 * The value of maxConnections is the size of the pool (ie the number of
 * clients created) and thus the maximum number of concurrent connections
 * to the database server.<br />
 * The value of minConnections is the minimum number of connected clients
 * normally expected to be in the pool.  The pool tries to ensure that it
 * doesn't contain more than this number of idle connected clients.
 */
- (id) initWithConfiguration: (NSDictionary*)config
			name: (NSString*)reference
                         max: (int)maxConnections
                         min: (int)minConnections;

/** Return the maximum number of database connections in the pool.
 */
- (int) maxConnections;

/** Return the minimum number of database connections in the pool.
 */
- (int) minConnections;

/** Fetches an (autoreleased) client from the pool.<br />
 * This method blocks indefinitely waiting for a client to become
 * available in the pool.
 */
- (SQLClient*) provideClient;

/** Fetches an (autoreleased) client from the pool.<br />
 * If no client is or becomes available before the specified date then
 * the method returns nil.<br />
 * If when is nil then a date in the distant future is used so that
 * the method will effectively wait forever to get a client.
 */
- (SQLClient*) provideClientBeforeDate: (NSDate*)when;

/**
 * Sets the cache for all the clients in the pool.
 */
- (void) setCache: (GSCache*)aCache;

/**
 * Sets the cache thread for all the clients in the pool.
 */
- (void) setCacheThread: (NSThread*)aThread;

/** Set the debugging level for all clients in the pool.
 */
- (void) setDebugging: (unsigned int)level;

/** Set the duration logging threshold for all clients in the pool.
 */
- (void) setDurationLogging: (NSTimeInterval)threshold;

/** Sets the pool size limits (number of connections we try to maintain).<br />
 * The value of maxConnections is the size of the pool (ie the number of
 * clients created) and thus the maximum number of concurrent connections
 * to the database server.<br />
 * The value of minConnections is the minimum number of connected clients
 * normally expected to be in the pool.  The pool tries to ensure that it
 * contains at least this number of connected clients.<br />
 * The value of maxConnections must be greater than or equal to that of
 * minConnections and may not be greater than 100.
 * The value of minConnections must be less than or equal to that of
 * maxConnections and may not be less than 1.
 */
- (void) setMax: (int)maxConnections min: (int)minConnections;

/** Returns a string describing the usage of the pool.
 */
- (NSString*) statistics;

/** Puts the client back in the pool.  This happens automatically
 * when a client from a pool would normally be deallocated so you don't
 * generally need to do it.<br />
 * Returns YES if the supplied client was from the pool, NO otherwise.
 */
- (BOOL) swallowClient: (SQLClient*)client;

@end

/** This category lists the convenience methods provided by a pool instance
 * for proxying messages to a one-off client instance in the pool.<br />
 * The behavior of each method is, of course, as documentf for instances
 * of the [SQLClient] class.
 */
@interface      SQLClientPool (Convenience)
- (NSString*) buildQuery: (NSString*)stmt,...;
- (NSString*) buildQuery: (NSString*)stmt with: (NSDictionary*)values;
- (NSMutableArray*) cache: (int)seconds
		    query: (NSString*)stmt,...;
- (NSMutableArray*) cache: (int)seconds
		    query: (NSString*)stmt
		     with: (NSDictionary*)values;
- (NSMutableArray*) cache: (int)seconds simpleQuery: (NSString*)stmt;
- (NSMutableArray*) cache: (int)seconds
	      simpleQuery: (NSString*)stmt
	       recordType: (id)rtype
	         listType: (id)ltype;
- (NSMutableArray*) columns: (NSMutableArray*)records;
- (NSInteger) execute: (NSString*)stmt,...;
- (NSInteger) execute: (NSString*)stmt with: (NSDictionary*)values;
- (NSMutableArray*) query: (NSString*)stmt,...;
- (NSMutableArray*) query: (NSString*)stmt with: (NSDictionary*)values;
- (SQLRecord*) queryRecord: (NSString*)stmt,...;
- (NSString*) queryString: (NSString*)stmt,...;
- (NSString*) quote: (id)obj;
- (NSMutableString*) quoteArray: (NSArray *)a
                       toString: (NSMutableString *)s
                 quotingStrings: (BOOL)_q;
- (NSString*) quotef: (NSString*)fmt, ...;
- (NSString*) quoteBigInteger: (int64_t)i;
- (NSString*) quoteCString: (const char *)s;
- (NSString*) quoteChar: (char)c;
- (NSString*) quoteFloat: (float)f;
- (NSString*) quoteInteger: (int)i;
- (NSString*) quoteString: (NSString *)s;
- (NSInteger) simpleExecute: (NSArray*)info;
- (void) singletons: (NSMutableArray*)records;
- (NSMutableArray*) simpleQuery: (NSString*)stmt;
- (NSMutableArray*) simpleQuery: (NSString*)stmt
		     recordType: (id)rtype
		       listType: (id)ltype;
@end

/**
 * The SQLTransaction transaction class provides a convenient mechanism
 * for grouping together a series of SQL statements to be executed as a
 * single transaction.  It avoids the need for handling begin/commit,
 * and should be as efficient as reasonably possible.<br />
 * You obtain an instance by calling [SQLClient-transaction], add SQL
 * statements to it using the -add:,... and/or -add:with: methods, and
 * then use the -execute method to perform all the statements as a
 * single operation.<br />
 * Any exception is caught and re-raised in the -execute method after any
 * tidying up to leave the database in a consistent state.<br />
 * NB. This class is not in itsself thread-safe, though the underlying
 * database operations should be.   If you have multiple threads, you
 * should create multiple SQLTransaction instances, at least one per thread.
 */
@interface	SQLTransaction : NSObject <NSCopying>
{
SQLCLIENT_PRIVATE
  SQLClient		*_db;
  NSMutableArray	*_info;
  unsigned		_count;
  BOOL                  _batch;
  BOOL                  _stop;
  uint8_t               _merge;
}

/**
 * Adds an SQL statement to the transaction.  This is similar to
 * [SQLClient-execute:,...] but does not cause any database operation
 * until -execute is called, so it will not raise a database exception.
 */
- (void) add: (NSString*)stmt,...;

/**
 * Adds an SQL statement to the transaction.  This is similar to
 * [SQLClient-execute:with:] but does not cause any database operation
 * until -execute is called, so it will not raise a database exception.
 */
- (void) add: (NSString*)stmt with: (NSDictionary*)values;

/** Adds a prepared statement.
 */
- (void) addPrepared: (NSArray*)statement;

/**
 * Appends a copy of the other transaction to the receiver.<br />
 * This provides a convenient way of merging transactions which have been
 * built by different code modules, in order to have them all executed
 * together in a single operation (for efficiency etc).<br />
 * This does not alter the other transaction, so if the execution of
 * a group of merged transactions fails, it is then possible to attempt
 * to commit the individual transactions separately.<br />
 * NB. All transactions appended must be using the same database
 * connection (SQLClient instance).
 */
- (void) append: (SQLTransaction*)other;

/**
 * Make a copy of the receiver.
 */
- (id) copyWithZone: (NSZone*)z;

/**
 * Returns the number of individual statements and/or subsidiary transactions
 * which have been added to the receiver.  For a count of the total number
 * of statements, use the -totalCount method.
 */
- (NSUInteger) count;

/**
 * Returns the database client with which this instance operates.<br />
 * This client is retained by the transaction.
 */
- (SQLClient*) db;

/**
 * <p>Performs any statements added to the transaction as a single operation.
 * If any problem occurs, an NSException is raised, but the database connection
 * is left in a consistent state and a partially completed operation is
 * rolled back.
 * </p>
 * <p>NB. If the database is not already in a transaction, this implicitly
 * calls the -begin method to start the transaction before executing the
 * statements.<br />
 * The method always commits the transaction, even if the transaction was
 * begun earlier rather than in -execute.<br />
 * This behavior allows you to call [SQLClient-begin], then run one or more
 * queries, build up a transaction based upon the query results, and then
 * -execute that transaction, causing the entire process to be commited as
 * a single transaction .
 * </p>
 */
- (void) execute;

/** Convenience method which calls
 * -executeBatchReturningFailures:logExceptions: with
 * a nil failures argument and exception logging off.
 */
- (unsigned) executeBatch;

/**
 * <p>This is similar to the -execute method, but may allow partial
 * execution of the transaction if appropriate:
 * </p>
 * <p>If the transaction was created using the [SQLClient-batch:] method and
 * the transaction as a whole fails, individual statements are retried.<br />
 * The stopOnFailure flag for the batch creation indicates whether the
 * retries are stopped at the first statement to fail, or continue (skipping
 * any failed statements).
 * </p>
 * <p>If the transaction has had transactions appended to it, those
 * subsidiary transactions may succeed or fail atomically depending
 * on their individual attributes.
 * </p>
 * <p>If the transaction was not created using [SQLClient-batch:], then
 * calling this method is equivalent to calling the -execute method.
 * </p>
 * <p>If any statements/transactions in the batch fail, they are added to
 * the transaction supplied in the failures parameter (if it's not nil)
 * so that you can retry them later.<br />
 * NB. statements/transactions which are not executed at all (because the
 * batch is set to stop on the first failure) are <em>also</em> added to
 * the failures transaction. 
 * </p>
 * <p>If the log argument is YES, then any exceptions encountered when
 * executing the batch are logged using the [SQLClient-debug:,...] method,
 * even if debug logging is not enabled with [SQLClient-setDebugging:].
 * </p>
 * The method returns the number of statements which actually succeeded.
 */
- (unsigned) executeBatchReturningFailures: (SQLTransaction*)failures
			     logExceptions: (BOOL)log;

/**
 * Insert trn at the index'th position in the receiver.<br />
 * The transaction trn must be non-empty and must use the same
 * database client as the receiver.
 */
- (void) insertTransaction: (SQLTransaction*)trn atIndex: (unsigned)index;

/** Remove the index'th transaction or statement from the receiver.
 */
- (void) removeTransactionAtIndex: (unsigned)index;

/**
 * Resets the transaction, removing all previously added statements.
 * This allows the transaction object to be re-used for multiple
 * transactions.
 */
- (void) reset;

/** <p>Use this method to enable merging of statemements subsequently added
 * or appended to the receiver.  The history argument specifies how many
 * of the most recent statements in the transaction should be checked for
 * merging in a new statement, with a value of zero meaning that no
 * merging is done.<br />
 * Returns the previous setting for the transaction.
 * </p>
 * <p>You may use this feature with an insert statement of the form:<br />
 * INSERT INTO table (fieldnames) VALUES (values);<br />
 * For databases which support multiline inserts such that they can be
 * merged into something of the form:
 * INSERT INTO table (fieldnames) VALUES (values1),(values2),...;
 * </p>
 * <p>Or may use this with an update or delete statement of the form:<br />
 * command table SET settings WHERE condition;<br />
 * So that statements may be merged into:<br />
 * command table SET settings WHERE (condition1) OR (condition2) OR ...;
 * </p>
 * If no opportunity for merging is found, the new statement is simply
 * added to the transaction.<br />
 * Caveats:<br />
 * 1. databases may not actually support multiline insert.<br />
 * 2. Merging is done only if the statement up to the string 'VALUES'
 * (for insert) or 'WHERE' (for update) matches.<br />
 * 3. Merging into any of the last N statements (where N is greater than 1)
 * may of course change the order of statements in the transaction,
 * so care must be taken not to use this feature where that might matter.<br />
 * 4. This is a simple text match rather than sql syntactic analysis,
 * so it's possible to confuse the process with complex statements.
 */
- (uint8_t) setMerge: (uint8_t)history;

/**
 * Returns the total count of statements in this transaction including
 * those in any subsidiary transactions.  For a count of the statements
 * and/or transactions directly added to the receiver, use the -count method.
 */
- (unsigned) totalCount;

/** Return an autoreleased copy of the index'th transaction or statement
 * added to the receiver.<br />
 * Since the returned transaction contains a copy of the statement/transaction
 * in the receiver, you can modify it without effecting the original.
 */
- (SQLTransaction*) transactionAtIndex: (unsigned)index;
@end



/* A helper for building a dictionary from an SQL query which returns
 * key-value pairs (you can subclass it to handle other records).<br />
 * You create an instance of this class, and pass it as both the
 * record and list class arguments of the low level SQLClient query.<br />
 * The query (which must return a number of records, each with two fields)
 * will result in a mutable dictionary being built, with dictionary keys
 * being the first field from each record and dictionary values being the
 * second field of each record.<br />
 * You may use the same instance for more than one query, but a second query
 * will replace the content dictionary produced by the first.<br />
 * If you want to handle records containing more than two values, you
 * must create a subclass which overrides the -newWithValues:keys:count:
 * method to create the record objects and add them to the content
 * dictionary.<br />
 * See [SQLClient-simpleQuery:recordType:listType:] also.<br />
 * NB. When this class is used, the query will actually return an
 * [NSMutableDictionary] instance rather than an [NSMutableArray] of
 * [SQLRecord] objects.
 */
@interface SQLDictionaryBuilder : NSObject
{
  NSMutableDictionary   *content;
}

/** No need to do anything ... the object will already have been added by
 * the -newWithValues:keys:count: method.
 */
- (void) addObject: (id)anObject;

/** When a container is supposed to be allocated, we just return the
 * receiver (which will then quietly ignore -addObject: messages).
 */
- (id) alloc;

/** Returns the content dictionary for the receiver.
 */
- (NSMutableDictionary*) content;

/** Creates a new content dictionary ... this method will be called
 * automatically by the SQLClient object when it performs a query,
 * so there is no need to call it at any other time.
 */
- (id) initWithCapacity: (NSUInteger)capacity;

/** Makes a mutable copy of the content dictionary (called when a caching
 * query uses this helper to produce the cached collection).
 */
- (id) mutableCopyWithZone: (NSZone*)aZone;

/** This is the main workhorse of the class ... it is called once for
 * every record read from the database, and is responsible for adding
 * that record to the content dictionary.  The default implementation,
 * instead of creating an object to hold the supplied record data,
 * uses the two fields from the record as a key-value pair to add to
 * the content dictionary, and returns nil as the record object.
 * It's OK to return a nil object since we ignore the -addObject:
 * argument.
 */
- (id) newWithValues: (id*)values
		keys: (NSString**)keys
	       count: (unsigned int)count; 
@end

/* A helper for building a counted set from an SQL query which returns
 * individual values (you can subclass it to handle other records).<br />
 * You create an instance of this class, and pass it as both the
 * record and list class arguments of the low level SQLClient query.<br />
 * The query (which must return a number of records, each with one field)
 * will result in a counted set being built and a record of the number of
 * added objects being kept.<br />
 * You may use the same instance for more than one query, but a second query
 * will replace the content set produced by the first.<br />
 * If you want to handle records containing more than one value, you
 * must create a subclass which overrides the -newWithValues:keys:count:
 * method to create the record objects and add them to the content
 * set, and increment the counter.<br />
 * See [SQLClient-simpleQuery:recordType:listType:] also.<br />
 * NB. When this class is used, the query will actually return an
 * [NSCountedSet] instance rather than an [NSMutableArray] of
 * [SQLRecord] objects.
 */
@interface SQLSetBuilder : NSObject
{
  NSCountedSet  *content;
  NSUInteger    added;
}

/** Returns the number of objects actually added to the counted set.
 */
- (NSUInteger) added;

/** No need to do anything ... the object will already have been added by
 * the -newWithValues:keys:count: method.
 */
- (void) addObject: (id)anObject;

/** When a container is supposed to be allocated, we just return the
 * receiver (which will then quietly ignore -addObject: messages).
 */
- (id) alloc;

/** Returns the counted set for the receiver.
 */
- (NSCountedSet*) content;

/** Creates a new content set ... this method will be called
 * automatically by the SQLClient object when it performs a query,
 * so there is no need to call it at any other time.
 */
- (id) initWithCapacity: (NSUInteger)capacity;

/** Makes a mutable copy of the content dictionary (called when a caching
 * query uses this helper to produce the cached collection).
 */
- (id) mutableCopyWithZone: (NSZone*)aZone;

/** This is the main workhorse of the class ... it is called once for
 * every record read from the database, and is responsible for adding
 * that record to the content set.  The default implementation,
 * instead of creating an object to hold the supplied record data,
 * uses the singe field from the record to add to
 * the content set, and returns nil as the record object.
 * It's OK to return a nil object since we ignore the -addObject:
 * argument.
 */
- (id) newWithValues: (id*)values
		keys: (NSString**)keys
	       count: (unsigned int)count; 
@end

/* A helper for building a collection of singletons from an SQL query
 * which returns singleton values.<br />
 * You create an instance of this class, and pass it as the record
 * class argument of the low level SQLClient query.<br />
 * The query (which must return a number of records, each with one field)
 * will result in the singleton values being stored in the list class.<br />
 * See [SQLClient-simpleQuery:recordType:listType:] also.
 */
@interface SQLSingletonBuilder : NSObject
- (id) newWithValues: (id*)values
		keys: (NSString**)keys
	       count: (unsigned int)count; 
@end

#endif

