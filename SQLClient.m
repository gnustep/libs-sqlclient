/* -*-objc-*- */

/** Implementation of SQLClient for GNUStep
   Copyright (C) 2004 Free Software Foundation, Inc.
   
   Written by:  Richard Frith-Macdonald <rfm@gnu.org>
   Date:	April 2004
   
   This file is part of the SQLClient Library.

   This library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Library General Public
   License as published by the Free Software Foundation; either
   version 2 of the License, or (at your option) any later version.
   
   This library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Library General Public License for more details.
   
   You should have received a copy of the GNU Library General Public
   License along with this library; if not, write to the Free
   Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111 USA.

   $Date$ $Revision$
   */ 

#include	<Foundation/NSArray.h>
#include	<Foundation/NSString.h>
#include	<Foundation/NSData.h>
#include	<Foundation/NSDate.h>
#include	<Foundation/NSCalendarDate.h>
#include	<Foundation/NSException.h>
#include	<Foundation/NSProcessInfo.h>
#include	<Foundation/NSNotification.h>
#include	<Foundation/NSUserDefaults.h>
#include	<Foundation/NSMapTable.h>
#include	<Foundation/NSBundle.h>
#include	<Foundation/NSLock.h>
#include	<Foundation/NSAutoreleasePool.h>
#include	<Foundation/NSValue.h>
#include	<Foundation/NSNull.h>
#include	<Foundation/NSDebug.h>
#include	<Foundation/NSPathUtilities.h>

#include	<GNUstepBase/GSLock.h>

#include	"SQLClient.h"

@implementation	SQLRecord
+ (id) allocWithZone: (NSZone*)aZone
{
  NSLog(@"Illegal attempt to allocate an SQLRecord");
  return nil;
}

+ (id) newWithValues: (id*)v keys: (id*)k count: (unsigned int)c
{
  id		*ptr;
  SQLRecord	*r;
  unsigned	pos;

  r = (SQLRecord*)NSAllocateObject(self, c*2*sizeof(id), NSDefaultMallocZone());
  r->count = c;
  ptr = ((void*)&(r->count)) + sizeof(r->count);
  for (pos = 0; pos < c; pos++)
    {
      ptr[pos] = RETAIN(v[pos]);
      ptr[pos + c] = RETAIN(k[pos]);
    }
  return r;
}

/**
 * Returns an array containing the names of all the fields in the
 * record, in the order in which they occur in the record.
 */
- (NSArray*) allKeys
{
  id		*ptr;

  ptr = ((void*)&count) + sizeof(count);
  return [NSArray arrayWithObjects: &ptr[count] count: count];
}

- (id) copyWithZone: (NSZone*)z
{
  return RETAIN(self);
}

- (unsigned int) count
{
  return count;
}

- (void) dealloc
{
  id		*ptr;
  unsigned	pos;

  ptr = ((void*)&count) + sizeof(count);
  for (pos = 0; pos < count; pos++)
    {
      DESTROY(ptr[pos]);
      DESTROY(ptr[count + pos]);
    }
  NSDeallocateObject(self);
}

- (id) init
{
  NSLog(@"Illegal attempt to -init an SQLRecord");
  DESTROY(self);
  return self;
}

- (id) objectAtIndex: (unsigned int)pos
{
  id	*ptr;

  if (pos >= count)
    {
      [NSException raise: NSRangeException
		  format: @"Array index too large"];
    }
  ptr = ((void*)&count) + sizeof(count);
  return ptr[pos];
}

/**
 * Returns the first field in the record whose name matches the specified
 * key.  Uses an exact match in preference to a case-insensitive match.
 */
- (id) objectForKey: (NSString*)key
{
  id		*ptr;
  unsigned int 	pos;

  ptr = ((void*)&count) + sizeof(count);
  for (pos = 0; pos < count; pos++)
    {
      if ([key isEqualToString: ptr[pos + count]] == YES)
	{
	  return ptr[pos];
	}
    }
  for (pos = 0; pos < count; pos++)
    {
      if ([key caseInsensitiveCompare: ptr[pos + count]] == NSOrderedSame)
	{
	  return ptr[pos];
	}
    }
  return nil;
}
@end

/**
 * Exception raised when an error with the remote database server occurs.
 */
NSString	*SQLException = @"SQLException";
/**
 * Exception for when a connection to the server is lost.
 */
NSString	*SQLConnectionException = @"SQLConnectionException";
/**
 * Exception for when a query is supposed to return data and doesn't.
 */
NSString	*SQLEmptyException = @"SQLEmptyException";
/**
 * Exception for when an insert/update would break the uniqueness of a
 * field or index.
 */
NSString	*SQLUniqueException = @"SQLUniqueException";

@implementation	SQLClient (Logging)

static unsigned int	classDebugging = 0;
static NSTimeInterval	classDuration = -1;

+ (unsigned int) debugging
{
  return classDebugging;
}

+ (NSTimeInterval) durationLogging
{
  return classDuration;
}

+ (void) setDebugging: (unsigned int)level
{
  classDebugging = level;
}

+ (void) setDurationLogging: (NSTimeInterval)threshold
{
  classDuration = threshold;
}

- (void) debug: (NSString*)fmt, ...
{
  va_list	ap;

  va_start(ap, fmt);
  NSLogv(fmt, ap);
  va_end(ap);
}

- (unsigned int) debugging
{
  return _debugging;
}

- (NSTimeInterval) durationLogging
{
  return _duration;
}

- (void) setDebugging: (unsigned int)level
{
  _debugging = level;
}

- (void) setDurationLogging: (NSTimeInterval)threshold
{
  _duration = threshold;
}

@end

/**
 * Container for all instances.
 */
static NSMapTable	*cache = 0;
static NSRecursiveLock	*cacheLock = nil;
static NSArray		*beginStatement = nil;
static NSArray		*commitStatement = nil;
static NSArray		*rollbackStatement = nil;


@interface	SQLClient (Private)
- (void) _configure: (NSNotification*)n;
- (NSArray*) _prepare: (NSString*)stmt args: (va_list)args;
- (NSArray*) _substitute: (NSString*)str with: (NSDictionary*)vals;
@end

@implementation	SQLClient

static unsigned int	maxConnections = 8;

+ (NSArray*) allClients
{
  NSArray	*a;

  [cacheLock lock];
  a = NSAllMapTableValues(cache);
  [cacheLock unlock];
  return a;
}

+ (SQLClient*) clientWithConfiguration: (NSDictionary*)config
				  name: (NSString*)reference
{
  SQLClient	*o;

  if ([reference isKindOfClass: [NSString class]] == NO)
    {
      reference = [[NSUserDefaults standardUserDefaults] stringForKey:
	@"SQLClientName"];
      if (reference == nil)
	{
	  reference = @"Database";
	}
    }

  o = [self existingClient: reference];
  if (o == nil)
    {
      o = [[SQLClient alloc] initWithConfiguration: config name: reference];
      AUTORELEASE(o);
    }
  return o;
}

+ (SQLClient*) existingClient: (NSString*)reference
{
  SQLClient	*existing;

  if ([reference isKindOfClass: [NSString class]] == NO)
    {
      reference = [[NSUserDefaults standardUserDefaults] stringForKey:
	@"SQLClientName"];
      if (reference == nil)
	{
	  reference = @"Database";
	}
    }

  [cacheLock lock];
  existing = (SQLClient*)NSMapGet(cache, reference);
  AUTORELEASE(RETAIN(existing));
  [cacheLock unlock];
  return existing;
}

+ (void) initialize
{
  if (cache == 0)
    {
      cache = NSCreateMapTable(NSObjectMapKeyCallBacks,
        NSNonRetainedObjectMapValueCallBacks, 0);
      cacheLock = [GSLazyRecursiveLock new];
      beginStatement = RETAIN([NSArray arrayWithObject: @"begin"]);
      commitStatement = RETAIN([NSArray arrayWithObject: @"commit"]);
      rollbackStatement = RETAIN([NSArray arrayWithObject: @"rollback"]);
    }
}

+ (unsigned int) maxConnections
{
  return maxConnections;
}

+ (void) purgeConnections: (NSDate*)since
{
  NSMapEnumerator	e;
  NSString		*n;
  SQLClient			*o;
  unsigned int		connectionCount = 0;

  [cacheLock lock];
  e = NSEnumerateMapTable(cache);
  while (NSNextMapEnumeratorPair(&e, (void**)&n, (void**)&o) != 0)
    {
      if (since != nil)
	{
	  NSDate	*when = [o lastOperation];

	  if (when == nil || [when earlierDate: since] != since)
	    {
	      [o disconnect];
	    }
	}
      if ([o connected] == YES)
	{
	  connectionCount++;
	}
    }
  NSEndMapTableEnumeration(&e);
  [cacheLock unlock];

  while (connectionCount >= maxConnections)
    {
      SQLClient		*other = nil;
      NSDate		*oldest = nil;
  
      connectionCount = 0;
      [cacheLock lock];
      e = NSEnumerateMapTable(cache);
      while (NSNextMapEnumeratorPair(&e, (void**)&n, (void**)&o))
	{
	  if ([o connected] == YES)
	    {
	      NSDate	*when = [o lastOperation];

	      connectionCount++;
	      if (oldest == nil || when == nil
		|| [oldest earlierDate: when] == when)
		{
		  oldest = when;
		  other = o;
		}
	    }
	}
      NSEndMapTableEnumeration(&e);
      [cacheLock unlock];
      connectionCount--;
      if ([other debugging] > 0)
	{
	  [other debug:
	    @"Force disconnect of '%@' because pool size (%d) reached",
	    other, maxConnections]; 
	}
      [other disconnect];
    }
}

+ (void) setMaxConnections: (unsigned int)c
{
  if (c > 0)
    {
      maxConnections = c;
      [self purgeConnections: nil];
    }
}

- (void) begin
{
  [lock lock];
  if (_inTransaction == NO)
    {
      _inTransaction = YES;
      NS_DURING
	{
	  [self simpleExecute: beginStatement];
	}
      NS_HANDLER
	{
	  [lock unlock];
	  _inTransaction = NO;
	  [localException raise];
	}
      NS_ENDHANDLER
    }
  else
    {
      [lock unlock];
      [NSException raise: NSInternalInconsistencyException
		  format: @"begin used inside transaction"];
    }
}

- (NSString*) clientName
{
  return _client;
}

- (void) commit
{
  [lock lock];
  if (_inTransaction == NO)
    {
      [lock unlock];
      [NSException raise: NSInternalInconsistencyException
		  format: @"commit used outside transaction"];
    }
  NS_DURING
    {
      [self simpleExecute: commitStatement];
      _inTransaction = NO;
      [lock unlock];		// Locked by -begin
      [lock unlock];		// Locked at start of -commit
    }
  NS_HANDLER
    {
      _inTransaction = NO;
      [lock unlock];		// Locked by -begin
      [lock unlock];		// Locked at start of -commit
      [localException raise];
    }
  NS_ENDHANDLER
}

- (BOOL) connect
{
  if (connected == NO)
    {
      [lock lock];
      if (connected == NO)
	{
	  NS_DURING
	    {
	      [self backendConnect];
	    }
	  NS_HANDLER
	    {
	      [lock unlock];
	      [localException raise];
	    }
	  NS_ENDHANDLER
	}
      [lock unlock];
    }
  return connected;
}

- (BOOL) connected
{
  return connected;
}

- (NSString*) database
{
  return _database;
}

- (void) dealloc
{
  NSNotificationCenter	*nc;

  nc = [NSNotificationCenter defaultCenter];
  [nc removeObserver: self];
  if (_name != nil)
    {
      [cacheLock lock];
      NSMapRemove(cache, (void*)_name);
      [cacheLock unlock];
    }
  [self disconnect];
  DESTROY(lock);
  DESTROY(_client);
  DESTROY(_database);
  DESTROY(_password);
  DESTROY(_user);
  DESTROY(_name);
  DESTROY(_lastOperation);
  [super dealloc];
}

- (NSString*) description
{
  NSMutableString	*s = AUTORELEASE([NSMutableString new]);

  [s appendFormat: @"Database      - %@\n", [self clientName]];
  [s appendFormat: @"  Name        - %@\n", [self name]];
  [s appendFormat: @"  DBase       - %@\n", [self database]];
  [s appendFormat: @"  DB User     - %@\n", [self user]];
  [s appendFormat: @"  Password    - %@\n",
    [self password] == nil ? @"unknown" : @"known"];
  [s appendFormat: @"  Connected   - %@\n", connected ? @"yes" : @"no"];
  [s appendFormat: @"  Transaction - %@\n\n", _inTransaction ? @"yes" : @"no"];
  return s;
}

- (void) disconnect
{
  if (connected == YES)
    {
      [lock lock];
      if (connected == YES)
	{
	  NS_DURING
	    {
	      [self backendDisconnect];
	    }
	  NS_HANDLER
	    {
	      [lock unlock];
	      [localException raise];
	    }
	  NS_ENDHANDLER
	}
      [lock unlock];
    }
}

- (void) execute: (NSString*)stmt, ...
{
  NSArray	*info;
  va_list	ap;

  va_start (ap, stmt);
  info = [self _prepare: stmt args: ap];
  va_end (ap);
  [self simpleExecute: info];
}

- (void) execute: (NSString*)stmt with: (NSDictionary*)values
{
  NSArray	*info;

  info = [self _substitute: stmt with: values];
  [self simpleExecute: info];
}

- (id) init
{
  return [self initWithConfiguration: nil name: nil];
}

- (id) initWithConfiguration: (NSDictionary*)config
{
  return [self initWithConfiguration: config name: nil];
}

- (id) initWithConfiguration: (NSDictionary*)config
			name: (NSString*)reference
{
  NSNotification	*n;
  id			existing;

  if ([reference isKindOfClass: [NSString class]] == NO)
    {
      reference = [[NSUserDefaults standardUserDefaults] stringForKey:
	@"SQLClientName"];
      if (reference == nil)
	{
	  reference = @"Database";
	}
    }

  [cacheLock lock];
  existing = (SQLClient*)NSMapGet(cache, reference);
  if (existing == nil)
    {
      lock = [GSLazyRecursiveLock new];	// Ensure thread-safety.
      [self setDebugging: [[self class] debugging]];
      [self setDurationLogging: [[self class] durationLogging]];
      [self setName: reference];	// Set name and store in cache.

      if (config == nil)
	{
	  NSNotificationCenter	*nc;
	  NSUserDefaults	*defs;

	  defs = [NSUserDefaults standardUserDefaults];
	  nc = [NSNotificationCenter defaultCenter];
	  [nc addObserver: self
		 selector: @selector(_configure:)
		     name: NSUserDefaultsDidChangeNotification
		   object: defs];
	  n = [NSNotification
	    notificationWithName: NSUserDefaultsDidChangeNotification
	    object: defs
	    userInfo: nil];
	}
      else
	{
	  n = [NSNotification
	    notificationWithName: NSUserDefaultsDidChangeNotification
	    object: config
	    userInfo: nil];
	}
      [self _configure: n];	// Actually set up the configuration.
    }
  else
    {
      RELEASE(self);
      self = RETAIN(existing);
    }
  [cacheLock unlock];

  return self;
}

- (BOOL) isInTransaction
{
  return _inTransaction;
}

- (NSDate*) lastOperation
{
  return _lastOperation;
}

- (NSString*) name
{
  return _name;
}

- (NSString*) password
{
  return _password;
}

- (NSMutableArray*) query: (NSString*)stmt, ...
{
  va_list		ap;
  NSMutableArray	*result = nil;

  /*
   * First check validity and concatenate parts of the query.
   */
  va_start (ap, stmt);
  stmt = [[self _prepare: stmt args: ap] objectAtIndex: 0];
  va_end (ap);

  result = [self simpleQuery: stmt];

  return result;
}

- (NSMutableArray*) query: (NSString*)stmt with: (NSDictionary*)values
{
  NSMutableArray	*result = nil;

  stmt = [[self _substitute: stmt with: values] objectAtIndex: 0];

  result = [self simpleQuery: stmt];

  return result;
}

- (NSString*) quote: (id)obj
{
  NSRange	r;

  /**
   * For a nil or NSNull object, we return NULL.
   */
  if (obj == nil || [obj isKindOfClass: [NSNull class]] == YES)
    {
      return @"NULL";
    }

  /**
   * For a number, we simply convert directly to a string.
   */
  if ([obj isKindOfClass: [NSNumber class]] == YES)
    {
      return [obj description];
    }

  /**
   * For a date, we convert to the text format used by the database,
   * and add leading and trailing quotes.
   */
  if ([obj isKindOfClass: [NSDate class]] == YES)
    {
      return [obj descriptionWithCalendarFormat: @"'%Y-%m-%d %H:%M:%S.%F %z'"
				       timeZone: nil
					 locale: nil];
    }

  /**
   * For a data object, we don't quote ... the other parts of the code
   * need to know they have an NSData object and pass it on unchanged
   * to the -backendExecute: method.
   */
  if ([obj isKindOfClass: [NSData class]] == YES)
    {
      return obj;
    }

  /**
   * For any other type of data, we just produce a quoted string
   * representation.
   */

  /* Get a string description of the object.  */
  obj = [obj description];
  obj = AUTORELEASE([obj mutableCopy]);

  /* Escape the string.  */
  r = [obj rangeOfString: @"\\"];
  if (r.length != 0)
    {
      [obj replaceString: @"\\" withString: @"\\\\"];
    }
  r = [obj rangeOfString: @"'"];
  if (r.length != 0)
    {
      [obj replaceString: @"'" withString: @"\\'"];
    }

  /* Add quoting around it.  */
  [obj replaceCharactersInRange: NSMakeRange(0, 0) withString: @"'"];
  [obj appendString: @"'"];
  return obj;
}

- (NSString*) quoteCString: (const char *)s
{
  NSString	*str = [[NSString alloc] initWithCString: s];
  NSString	*result = [self quote: str];

  RELEASE(str);
  return result;
}

- (NSString*) quoteChar: (char)c
{
  if (c == 0)
    {
      [NSException raise: NSInvalidArgumentException
		  format: @"Attempt to quote a nul character in -quoteChar:"];
    }
  if (c == '\'' || c == '\\')
    {
      return [NSString stringWithFormat: @"'\\%c'", c];
    }
  return [NSString stringWithFormat: @"'%c'", c];
}

- (NSString*) quoteFloat: (float)f
{
  return [NSString stringWithFormat: @"%f", f];
}

- (NSString*) quoteInteger: (int)i
{
  return [NSString stringWithFormat: @"%d", i];
}

- (void) rollback
{
  [lock lock];
  if (_inTransaction == NO)
    {
      [lock unlock];
      [NSException raise: NSInternalInconsistencyException
		  format: @"rollback used outside transaction"];
    }
  NS_DURING
    {
      [self simpleExecute: rollbackStatement];
      _inTransaction = NO;
      [lock unlock];		// Locked by -begin
      [lock unlock];		// Locked at start of -rollback
    }
  NS_HANDLER
    {
      _inTransaction = NO;
      [lock unlock];		// Locked by -begin
      [lock unlock];		// Locked at start of -rollback
      [localException raise];
    }
  NS_ENDHANDLER
}

- (void) setDatabase: (NSString*)s
{
  if ([s isEqual: _database] == NO)
    {
      if (connected == YES)
	{
	  [self disconnect];
	}
      ASSIGNCOPY(_database, s);
    }
}

- (void) setName: (NSString*)s
{
  if ([s isEqual: _name] == NO)
    {
      [lock lock];
      if ([s isEqual: _name] == YES)
	{
	  [lock unlock];
	  return;
	}
      [cacheLock lock];
      if (NSMapGet(cache, s) != 0)
	{
	  [lock unlock];
	  [cacheLock unlock];
	  if ([self debugging] > 0)
	    {
	      [self debug: @"Error attempt to re-use client name %@", s];
	    }
	  return;
	}
      if (connected == YES)
	{
	  [self disconnect];
	}
      RETAIN(self);
      NSMapRemove(cache, (void*)_name);
      ASSIGNCOPY(_name, s);
      ASSIGN(_client, [[NSProcessInfo processInfo] globallyUniqueString]);
      NSMapInsert(cache, (void*)_name, (void*)self);
      [cacheLock unlock];
      [lock unlock];
      RELEASE(self);
    }
}

- (void) setPassword: (NSString*)s
{
  if ([s isEqual: _password] == NO)
    {
      if (connected == YES)
	{
	  [self disconnect];
	}
      ASSIGNCOPY(_password, s);
    }
}

- (void) setUser: (NSString*)s
{
  if ([s isEqual: _client] == NO)
    {
      if (connected == YES)
	{
	  [self disconnect];
	}
      ASSIGNCOPY(_user, s);
    }
}

- (void) simpleExecute: (NSArray*)info
{
  [lock lock];
  NS_DURING
    {
      NSDate	*start = nil;

      if (_duration >= 0)
	{
	  start = [NSDate date];
	}
      [self backendExecute: info];
      RELEASE(_lastOperation);
      _lastOperation = [NSDate new];
      if (_duration >= 0)
	{
	  NSTimeInterval	d;

	  d = [_lastOperation timeIntervalSinceDate: start];
	  if (d >= _duration)
	    {
	      /*
	       * For higher debug levels, we log data objects as well
	       * as the query string, otherwise we omit them.
	       */
	      if ([self debugging] > 1)
		{
		  [self debug: @"Duration %g for statement %@", d, info];
		}
	      else
		{
		  [self debug: @"Duration %g for statement %@",
		    d, [info objectAtIndex: 0]];
		}
	    }
	}
    }
  NS_HANDLER
    {
      [lock unlock];
      [localException raise];
    }
  NS_ENDHANDLER
  [lock unlock];
}

- (NSMutableArray*) simpleQuery: (NSString*)stmt
{
  NSMutableArray	*result = nil;

  [lock lock];
  NS_DURING
    {
      NSDate	*start = nil;

      if (_duration >= 0)
	{
	  start = [NSDate date];
	}
      result = [self backendQuery: stmt];
      RELEASE(_lastOperation);
      _lastOperation = [NSDate new];
      if (_duration >= 0)
	{
	  NSTimeInterval	d;

	  d = [_lastOperation timeIntervalSinceDate: start];
	  if (d >= _duration)
	    {
	      [self debug: @"Duration %g for query %@", d, stmt];
	    }
	}
    }
  NS_HANDLER
    {
      [lock unlock];
      [localException raise];
    }
  NS_ENDHANDLER
  [lock unlock];
  return result;
}

- (NSString*) user
{
  return _user;
}

@end

@implementation	SQLClient (Subclass)

- (BOOL) backendConnect
{
  [NSException raise: NSInternalInconsistencyException
	      format: @"Called -%@ without backend bundle loaded",
    NSStringFromSelector(_cmd)];
  return NO;
}

- (void) backendDisconnect
{
  [NSException raise: NSInternalInconsistencyException
	      format: @"Called -%@ without backend bundle loaded",
    NSStringFromSelector(_cmd)];
}

- (void) backendExecute: (NSArray*)info
{
  [NSException raise: NSInternalInconsistencyException
	      format: @"Called -%@ without backend bundle loaded",
    NSStringFromSelector(_cmd)];
}

- (NSMutableArray*) backendQuery: (NSString*)stmt
{
  [NSException raise: NSInternalInconsistencyException
	      format: @"Called -%@ without backend bundle loaded",
    NSStringFromSelector(_cmd)];
  return nil;
}

- (unsigned) copyEscapedBLOB: (NSData*)blob into: (void*)buf
{
  [NSException raise: NSInternalInconsistencyException
	      format: @"Called -%@ without backend bundle loaded",
    NSStringFromSelector(_cmd)];
  return 0;
}

- (const void*) insertBLOBs: (NSArray*)blobs
	      intoStatement: (const void*)statement
		     length: (unsigned)sLength
		 withMarker: (const void*)marker
		     length: (unsigned)mLength
		     giving: (unsigned*)result
{
  unsigned	count = [blobs count];
  unsigned	length = sLength;

  if (count > 1)
    {
      unsigned			i;
      unsigned char		*buf;
      unsigned char		*ptr;
      const unsigned char	*from = (const unsigned char*)statement;

      /*
       * Calculate length of buffer needed.
       */
      for (i = 1; i < count; i++)
	{
	  length += [self lengthOfEscapedBLOB: [blobs objectAtIndex: i]];
	  length -= mLength;
	}

      buf = NSZoneMalloc(NSDefaultMallocZone(), length + 1);
      [NSData dataWithBytesNoCopy: buf length: length + 1];	// autoreleased
      ptr = buf;

      /*
       * Merge quoted data objects into statement.
       */
      i = 1;
      from = (unsigned char*)statement;
      while (*from != 0)
	{
	  if (*from == *(unsigned char*)marker
	    && memcmp(from, marker, mLength) == 0)
	    {
	      NSData	*d = [blobs objectAtIndex: i++];

	      from += mLength;
	      ptr += [self copyEscapedBLOB: d into: ptr];
	    }
	  else
	    {
	      *ptr++ = *from++;
	    }
	}
      *ptr = '\0';
      statement = buf;
    }
  *result = length;
  return statement;
}

- (unsigned) lengthOfEscapedBLOB: (NSData*)blob
{
  [NSException raise: NSInternalInconsistencyException
	      format: @"Called -%@ without backend bundle loaded",
    NSStringFromSelector(_cmd)];
  return 0;
}
@end

@implementation	SQLClient (Private)

/**
 * Internal method to handle configuration using the notification object.
 * This object may be either a configuration front end or a user defaults
 * object ... so we have to be careful that we work with both.
 */
- (void) _configure: (NSNotification*)n
{
  id		o = [n object];
  NSDictionary	*d;
  NSString	*s;
  Class		c;

  /*
   * get dictionary containing config info for this client by name.
   */
  d = [o objectForKey: @"SQLClientReferences"];
  if ([d isKindOfClass: [NSDictionary class]] == NO)
    {
      [self debug: @"Unable to find SQLClientReferences config dictionary"];
      d = nil;
    }
  d = [d objectForKey: _name];
  if ([d isKindOfClass: [NSDictionary class]] == NO)
    {
      [self debug: @"Unable to find config for client '%@'", _name];
      d = nil;
    }

  s = [d objectForKey: @"ServerType"];
  if ([s isKindOfClass: [NSString class]] == NO)
    {
      s = @"Postgres";
    }

  c = NSClassFromString([@"SQLClient" stringByAppendingString: s]);
  if (c == nil)
    {
      NSString	*path;
      NSBundle	*bundle;
      NSArray	*paths;
      unsigned	count;

      paths = NSSearchPathForDirectoriesInDomains(NSLibraryDirectory,
	NSLocalDomainMask, NO);
      count = [paths count];
      while (count-- > 0)
	{
	  path = [paths objectAtIndex: count];
	  path = [path stringByAppendingPathComponent: @"Bundles"];
	  path = [path stringByAppendingPathComponent: @"SQLClient"];
	  path = [path stringByAppendingPathComponent: s];
	  path = [path stringByAppendingPathExtension: @"bundle"];
	  bundle = [NSBundle bundleWithPath: path];
	  if (bundle != nil)
	    {
	      break;	// Found it.
	    }
	}
      if (bundle == nil)
	{
	  [self debug: @"unable to load bundle for '%@' server type", s];
	  return;
	}
      else
	{
	  c = [bundle principalClass];
	  if (c == nil)
	    {
	      [self debug: @"No database class to support server type '%@'", s];
	      return;
	    }
	}
    }
  if (c != [self class])
    {
      [self disconnect];
      GSDebugAllocationRemove(self->isa, self);
      self->isa = c;
      GSDebugAllocationAdd(self->isa, self);
    }

  s = [d objectForKey: @"Database"];
  if ([s isKindOfClass: [NSString class]] == NO)
    {
      s = [o objectForKey: @"Database"];
      if ([s isKindOfClass: [NSString class]] == NO)
	{
	  s = nil;
	}
    }
  [self setDatabase: s];

  s = [d objectForKey: @"User"];
  if ([s isKindOfClass: [NSString class]] == NO)
    {
      s = [o objectForKey: @"User"];
      if ([s isKindOfClass: [NSString class]] == NO)
	{
	  s = @"";
	}
    }
  [self setUser: s];

  s = [d objectForKey: @"Password"];
  if ([s isKindOfClass: [NSString class]] == NO)
    {
      s = [o objectForKey: @"Password"];
      if ([s isKindOfClass: [NSString class]] == NO)
	{
	  s = @"";
	}
    }
  [self setPassword: s];
}

/**
 * Internal method to build an sql string by quoting any non-string objects
 * and concatenating the resulting strings in a nil terminated list.<br />
 * Returns an array containing the statement as the first object and
 * any NSData objects following.  The NSData objects appear in the
 * statement strings as the marker sequence - <code>'''</code>
 */
- (NSArray*) _prepare: (NSString*)stmt args: (va_list)args
{
  NSMutableArray	*ma = [NSMutableArray arrayWithCapacity: 2];
  NSString		*tmp = va_arg(args, NSString*);
  CREATE_AUTORELEASE_POOL(arp);

  if (tmp != nil)
    {
      NSMutableString	*s = [NSMutableString stringWithCapacity: 1024];

      [s appendString: stmt];
      /*
       * Append any values from the nil terminated varargs
       */ 
      while (tmp != nil)
	{
	  if ([tmp isKindOfClass: [NSString class]] == NO)
	    {
	      if ([tmp isKindOfClass: [NSData class]] == YES)
		{
		  [ma addObject: tmp];
		  [s appendString: @"'''"];	// Marker.
		}
	      else
		{
		  [s appendString: [self quote: tmp]];
		}
	    }
	  else
	    {
	      [s appendString: tmp];
	    }
	  tmp = va_arg(args, NSString*);
	}
      stmt = s;
    }
  [ma insertObject: stmt atIndex: 0];
  DESTROY(arp);
  return ma;
}

/**
 * Internal method to substitute values from the dictionary into
 * a string containing markup identifying where the values should
 * appear by name.  Non-string objects in the dictionary are quoted.<br />
 * Returns an array containing the statement as the first object and
 * any NSData objects following.  The NSData objects appear in the
 * statement strings as the marker sequence - <code>'''</code>
 */
- (NSArray*) _substitute: (NSString*)str with: (NSDictionary*)vals
{
  unsigned int		l = [str length];
  NSRange		r;
  NSMutableArray	*ma = [NSMutableArray arrayWithCapacity: 2];
  CREATE_AUTORELEASE_POOL(arp);

  if (l < 2)
    {
      [ma addObject: str];		// Can't contain a {...} sequence
    }
  else if ((r = [str rangeOfString: @"{"]).length == 0)
    {
      [ma addObject: str];		// No '{' markup
    }
  else if (l - r.location < 2)
    {
      [ma addObject: str];		// Can't contain a {...} sequence
    }
  else if ([str rangeOfString: @"}" options: NSLiteralSearch
    range: NSMakeRange(r.location, l - r.location)].length == 0
    && [str rangeOfString: @"{{" options: NSLiteralSearch
    range: NSMakeRange(0, l)].length == 0)
    {
      [ma addObject: str];		// No closing '}' or repeated '{{'
    }
  else if (r.length == 0)
    {
      [ma addObject: str];		// Nothing to do.
    }
  else
    {
      NSMutableString	*mtext = AUTORELEASE([str mutableCopy]);

      /*
       * Replace {FieldName} with the value of the field
       */
      while (r.length > 0)
	{
	  unsigned	pos = r.location;
	  unsigned	nxt;
	  unsigned	vLength;
	  NSArray	*a;
	  NSRange	s;
	  NSString	*v;
	  NSString	*alt;
	  id	o;
	  unsigned	i;

	  r.length = l - pos;

	  /*
	   * If the length of the string from the '{' onwards is less than two,
	   * there is nothing to do and we can end processing.
	   */
	  if (r.length < 2)
	    {
	      break;
	    }

	  if ([mtext characterAtIndex: r.location + 1] == '{')
	    {
	      // Got '{{' ... remove one of them.
	      r.length = 1;
	      [mtext replaceCharactersInRange: r withString: @""];
	      l--;
	      r.location++;
	      r.length = l - r.location;
	      r = [mtext rangeOfString: @"{"
			       options: NSLiteralSearch
				 range: r];
	      continue;
	    }

	  r = [mtext rangeOfString: @"}"
			   options: NSLiteralSearch
			     range: r];
	  if (r.length == 0)
	    {
	      break;	// No closing bracket
	    }
	  nxt = NSMaxRange(r);
	  r = NSMakeRange(pos, nxt - pos);
	  s.location = r.location + 1;
	  s.length = r.length - 2;
	  v = [mtext substringWithRange: s];

	  /*
	   * If the value contains a '?', it is actually in two parts,
	   * the first part is the field name, and the second part is
	   * an alternative text to be used if the value from the
	   * dictionary is empty.
	   */
	  s = [v rangeOfString: @"?"];
	  if (s.length == 0)
	    {
	      alt = @"";	// No alternative value.
	    }
	  else
	    {
	      alt = [v substringFromIndex: NSMaxRange(s)];
	      v = [v substringToIndex: s.location];
	    }
       
	  /*
	   * If the value we are substituting contains dots, we split it apart.
	   * We use the value to make a reference into the dictionary we are
	   * given.
	   */
	  a = [v componentsSeparatedByString: @"."];
	  o = vals;
	  for (i = 0; i < [a count]; i++)
	    {
	      NSString	*k = [a objectAtIndex: i];

	      if ([k length] > 0)
		{
		  o = [o objectForKey: k];
		}
	    }
	  if (o == vals)
	    {
	      v = nil;		// Mo match found.
	    }
	  else
	    {
	      if ([o isKindOfClass: [NSString class]] == YES)
		{
		  v = o;
		}
	      else
		{
		  if ([o isKindOfClass: [NSData class]] == YES)
		    {
		      [ma addObject: o];
		      v = @"'''";
		    }
		  else
		    {
		      v = [self quote: o];
		    }
		}
	    }

	  if ([v length] == 0)
	    {
	      v = alt;
	    }
	  vLength = [v length];

	  [mtext replaceCharactersInRange: r withString: v];
	  l += vLength;		// Add length of string inserted
	  l -= r.length;		// Remove length of string replaced
	  r.location += vLength;

	  if (r.location >= l)
	    {
	      break;
	    }
	  r.length = l - r.location;
	  r = [mtext rangeOfString: @"{"
			   options: NSLiteralSearch
			     range: r];
	}
      [ma insertObject: mtext atIndex: 0];
    }
  RELEASE(arp);
  return ma;
}
@end



@implementation	SQLClient(Convenience)

- (SQLRecord*) queryRecord: (NSString*)stmt, ...
{
  va_list	ap;
  NSArray	*result = nil;
  SQLRecord	*record;

  va_start (ap, stmt);
  stmt = [[self _prepare: stmt args: ap] objectAtIndex: 0];
  va_end (ap);

  result = [self simpleQuery: stmt];

  if ([result count] > 1)
    {
      [NSException raise: NSInvalidArgumentException
		  format: @"Query returns more than one record -\n%@\n", stmt];
    }
  record = [result lastObject];
  if (record == nil)
    {
      [NSException raise: SQLEmptyException
		  format: @"Query returns no data -\n%@\n", stmt];
    }
  return record;
}

- (NSString*) queryString: (NSString*)stmt, ...
{
  va_list	ap;
  NSArray	*result = nil;
  SQLRecord	*record;

  va_start (ap, stmt);
  stmt = [[self _prepare: stmt args: ap] objectAtIndex: 0];
  va_end (ap);

  result = [self simpleQuery: stmt];

  if ([result count] > 1)
    {
      [NSException raise: NSInvalidArgumentException
		  format: @"Query returns more than one record -\n%@\n", stmt];
    }
  record = [result lastObject];
  if (record == nil)
    {
      [NSException raise: SQLEmptyException
		  format: @"Query returns no data -\n%@\n", stmt];
    }
  if ([record count] > 1)
    {
      [NSException raise: NSInvalidArgumentException
		  format: @"Query returns multiple fields -\n%@\n", stmt];
    }
  return [[record lastObject] description];
}

- (void) singletons: (NSMutableArray*)records
{
  unsigned	c = [records count];

  while (c-- > 0)
    {
      [records replaceObjectAtIndex: c
			 withObject: [[records objectAtIndex: c] lastObject]];
    }
}

@end

