/* -*-objc-*- */

/** Implementation of SQLClient for GNUStep
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

   $Date$ $Revision$
   */ 

#import	<Foundation/NSArray.h>
#import	<Foundation/NSAutoreleasePool.h>
#import	<Foundation/NSBundle.h>
#import	<Foundation/NSCalendarDate.h>
#import	<Foundation/NSCharacterSet.h>
#import	<Foundation/NSData.h>
#import	<Foundation/NSDate.h>
#import	<Foundation/NSDebug.h>
#import	<Foundation/NSDictionary.h>
#import	<Foundation/NSEnumerator.h>
#import	<Foundation/NSException.h>
#import	<Foundation/NSKeyValueCoding.h>
#import	<Foundation/NSLock.h>
#import	<Foundation/NSHashTable.h>
#import	<Foundation/NSMapTable.h>
#import	<Foundation/NSNotification.h>
#import	<Foundation/NSNull.h>
#import	<Foundation/NSPathUtilities.h>
#import	<Foundation/NSProcessInfo.h>
#import	<Foundation/NSRunLoop.h>
#import	<Foundation/NSSet.h>
#import	<Foundation/NSString.h>
#import	<Foundation/NSThread.h>
#import	<Foundation/NSTimer.h>
#import	<Foundation/NSUserDefaults.h>
#import	<Foundation/NSValue.h>

#import	<Performance/GSCache.h>
#import	<Performance/GSTicker.h>

#define SQLCLIENT_PRIVATE       @public

#include	<memory.h>

#include	"SQLClient.h"

#if	defined(GNUSTEP_BASE_LIBRARY)
#define	SUBCLASS_RESPONSIBILITY	[self subclassResponsibility: _cmd];
#else
#define	SUBCLASS_RESPONSIBILITY	
#endif

NSString * const SQLClientDidConnectNotification
 = @"SQLClientDidConnectNotification";

NSString * const SQLClientDidDisconnectNotification
 = @"SQLClientDidDisconnectNotification";

static NSNull	*null = nil;
static NSArray	*queryModes = nil;
static NSThread	*mainThread = nil;
static Class	NSStringClass = Nil;
static Class	NSArrayClass = Nil;
static Class	NSDateClass = Nil;
static Class	NSSetClass = Nil;
static Class	SQLClientClass = Nil;

@interface	_ConcreteSQLRecord : SQLRecord
{
  unsigned	count;
}
@end

@interface	CacheQuery : NSObject
{
@public
  NSString	*query;
  id		recordType;
  id		listType;
  unsigned	lifetime;
}
@end

@implementation	CacheQuery
- (void) dealloc
{
  [query release];
  [super dealloc];
}
@end

static Class aClass = 0;
static Class rClass = 0;

@implementation	SQLRecord
+ (id) allocWithZone: (NSZone*)aZone
{
  NSLog(@"Illegal attempt to allocate an SQLRecord");
  return nil;
}

+ (void) initialize
{
  GSTickerTimeNow();
  if (null == nil)
    {
      null = [NSNull new];
      aClass = [NSMutableArray class];
      rClass = [_ConcreteSQLRecord class];
    }
}

+ (id) newWithValues: (id*)v keys: (NSString**)k count: (unsigned int)c
{
  return [rClass newWithValues: v keys: k count: c];
}

- (NSArray*) allKeys
{
  NSUInteger	count = [self count];

  if (count > 0)
    {
      id	buf[count];

      while (count-- > 0)
        {
          buf[count] = [self keyAtIndex: count];
        }
      return [NSArray arrayWithObjects: buf count: count];
    }
  else
    {
      return [NSArray array];
    }
}

- (id) copyWithZone: (NSZone*)z
{
  return [self retain];
}

- (NSUInteger) count
{
  SUBCLASS_RESPONSIBILITY
  return 0;
}

- (NSMutableDictionary*) dictionary
{
  NSUInteger		count = [self count];

  if (count > 0)
    {
      id	keys[count];
      id        vals[count];

      [self getKeys: keys];
      [self getObjects: vals];
      return [NSMutableDictionary dictionaryWithObjects: vals
                                                forKeys: keys
                                                  count: count];
    }
  else
    {
      return [NSMutableDictionary dictionary];
    }
}

- (void) getKeys: (id*)buf
{
  unsigned	i = [self count];

  while (i-- > 0)
    {
      buf[i] = [self keyAtIndex: i];
    }
}

- (void) getObjects: (id*)buf
{
  unsigned	i = [self count];

  while (i-- > 0)
    {
      buf[i] = [self objectAtIndex: i];
    }
}

- (id) init
{
  NSLog(@"Illegal attempt to -init an SQLRecord");
  [self release];
  return nil;
}

- (NSString*) keyAtIndex: (NSUInteger)index
{
  SUBCLASS_RESPONSIBILITY
  return nil;
}

- (id) objectAtIndex: (NSUInteger)index
{
  SUBCLASS_RESPONSIBILITY
  return nil;
}

- (id) objectForKey: (NSString*)key
{
  NSUInteger    count = [self count];

  if (count > 0)
    {
      NSUInteger        pos;
      id	        keys[count];

      [self getKeys: keys];
      for (pos = 0; pos < count; pos++)
        {
          if ([key isEqualToString: keys[pos]] == YES)
            {
              break;
            }
        }
      if (pos == count)
        {
          for (pos = 0; pos < count; pos++)
            {
              if ([key caseInsensitiveCompare: keys[pos]] == NSOrderedSame)
                {
                  break;
                }
            }
        }

      if (pos != count)
        {
          return [self objectAtIndex: pos];
        }
    }
  return nil;
}

- (void) replaceObjectAtIndex: (NSUInteger)index withObject: (id)anObject
{
  SUBCLASS_RESPONSIBILITY
}

- (void) setObject: (id)anObject forKey: (NSString*)aKey
{
  NSUInteger	count = [self count];

  if (count > 0)
    {
      NSUInteger	pos;
      id		keys[count];

      if (anObject == nil)
        {
          anObject = null;
        }
      [self getKeys: keys];
      for (pos = 0; pos < count; pos++)
        {
          if ([aKey isEqualToString: keys[pos]] == YES)
            {
              break;
            }
        }
      if (pos == count)
        {
          for (pos = 0; pos < count; pos++)
            {
              if ([aKey caseInsensitiveCompare: keys[pos]] == NSOrderedSame)
                {
                  break;
                }
            }
        }

      if (pos == count)
        {
          [NSException raise: NSInvalidArgumentException
                      format: @"Bad key (%@) in -setObject:forKey:", aKey];
        }
      else
        {
          [self replaceObjectAtIndex: pos withObject: anObject];
        }
    }
  else
    {
      [NSException raise: NSInvalidArgumentException
		  format: @"Bad key (%@) in -setObject:forKey:", aKey];
    }
}

- (NSUInteger) sizeInBytes: (NSMutableSet*)exclude
{
  NSUInteger	size = [super sizeInBytes: exclude];
  NSUInteger	count = [self count];

  if (size > 0 && count > 0)
    {
      NSUInteger	pos;
      id	        vals[count];

      [self getObjects: vals];
      for (pos = 0; pos < count; pos++)
	{
	  size += [vals[pos] sizeInBytes: exclude];
	}
    }
  return size;
}

@end

@implementation	SQLRecord (KVC)
- (void) setValue: (id)aValue forKey: (NSString*)aKey
{
  [self setObject: aValue forKey: aKey];
}
- (id) valueForKey: (NSString*)aKey
{
  id	v = [self objectForKey: aKey];

  if (v == nil)
    {
      v = [super valueForKey: aKey];
    }
  return v;
}
@end


@implementation	_ConcreteSQLRecord

+ (id) newWithValues: (id*)v keys: (NSString**)k count: (unsigned int)c
{
  id		*ptr;
  _ConcreteSQLRecord	*r;
  unsigned	pos;

  r = (_ConcreteSQLRecord*)NSAllocateObject(self,
    c*2*sizeof(id), NSDefaultMallocZone());
  r->count = c;
  ptr = ((void*)&(r->count)) + sizeof(r->count);
  for (pos = 0; pos < c; pos++)
    {
      if (v[pos] == nil)
	{
	  ptr[pos] = [null retain];
	}
      else
	{
	  ptr[pos] = [v[pos] retain];
	}
      ptr[pos + c] = [k[pos] retain];
    }
  return r;
}

- (NSArray*) allKeys
{
  id		*ptr;

  ptr = ((void*)&count) + sizeof(count);
  return [NSArray arrayWithObjects: &ptr[count] count: count];
}

- (id) copyWithZone: (NSZone*)z
{
  return [self retain];
}

- (NSUInteger) count
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
      [ptr[pos] release]; ptr[pos] = nil;
      [ptr[count + pos] release]; ptr[count + pos] = nil;
    }
  [super dealloc];
}

- (NSMutableDictionary*) dictionary
{
  NSMutableDictionary	*d;
  unsigned		pos;
  id			*ptr;

  ptr = ((void*)&count) + sizeof(count);
  d = [NSMutableDictionary dictionaryWithCapacity: count];
  for (pos = 0; pos < count; pos++)
    {
      [d setObject: ptr[pos] forKey: [ptr[pos + count] lowercaseString]];
    }
  return d;
}

- (void) getKeys: (id*)buf
{
  id		*ptr;
  unsigned	pos;

  ptr = ((void*)&count) + sizeof(count);
  ptr += count;	// Step past objects to keys.
  for (pos = 0; pos < count; pos++)
    {
      buf[pos] = ptr[pos];
    }
}

- (void) getObjects: (id*)buf
{
  id		*ptr;
  unsigned	pos;

  ptr = ((void*)&count) + sizeof(count);
  for (pos = 0; pos < count; pos++)
    {
      buf[pos] = ptr[pos];
    }
}

- (id) init
{
  NSLog(@"Illegal attempt to -init an SQLRecord");
  [self release];
  return nil;
}

- (NSString*) keyAtIndex: (NSUInteger)pos
{
  id	*ptr;

  if (pos >= count)
    {
      [NSException raise: NSRangeException
		  format: @"Array index too large"];
    }
  ptr = ((void*)&count) + sizeof(count);
  ptr += count;
  return ptr[pos];
}

- (id) objectAtIndex: (NSUInteger)pos
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

- (void) replaceObjectAtIndex: (NSUInteger)index withObject: (id)anObject
{
  id		*ptr;

  if (index >= count)
    {
      [NSException raise: NSRangeException
		  format: @"Array index too large"];
    }
  if (anObject == nil)
    {
      anObject = null;
    }
  ptr = ((void*)&count) + sizeof(count);
  ptr += index;
  [anObject retain];
  [*ptr release];
  *ptr = anObject;
}

- (void) setObject: (id)anObject forKey: (NSString*)aKey
{
  id		*ptr;
  unsigned int 	pos;

  if (anObject == nil)
    {
      anObject = null;
    }
  ptr = ((void*)&count) + sizeof(count);
  for (pos = 0; pos < count; pos++)
    {
      if ([aKey isEqualToString: ptr[pos + count]] == YES)
	{
          [anObject retain];
          [ptr[pos] release];
	  ptr[pos] = anObject;
	  return;
	}
    }
  for (pos = 0; pos < count; pos++)
    {
      if ([aKey caseInsensitiveCompare: ptr[pos + count]] == NSOrderedSame)
	{
          [anObject retain];
          [ptr[pos] release];
	  ptr[pos] = anObject;
	  return;
	}
    }
  [NSException raise: NSInvalidArgumentException
	      format: @"Bad key (%@) in -setObject:forKey:", aKey];
}

- (NSUInteger) sizeInBytes: (NSMutableSet*)exclude
{
  if ([exclude member: self] != nil)
    {
      return 0;
    }
  else
    {
      NSUInteger	size = [super sizeInBytes: exclude];
      NSUInteger	pos;
      id		*ptr;

      ptr = ((void*)&count) + sizeof(count);
      for (pos = 0; pos < count; pos++)
	{
	  size += [ptr[pos] sizeInBytes: exclude];
	}
      return size;
    }
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
static NSHashTable	*clientsHash = 0;
static NSMapTable	*clientsMap = 0;
static NSRecursiveLock	*clientsLock = nil;
static NSString		*beginString = @"begin";
static NSArray		*beginStatement = nil;
static NSString		*commitString = @"commit";
static NSArray		*commitStatement = nil;
static NSString		*rollbackString = @"rollback";
static NSArray		*rollbackStatement = nil;


@interface	SQLClient (Private)

/**
 * Internal method to handle configuration using the notification object.
 * This object may be either a configuration front end or a user defaults
 * object ... so we have to be careful that we work with both.
 */
- (void) _configure: (NSNotification*)n;

/** Internal method to make the client instance lock available to
 * an associated SQLTransaction
 */
- (NSRecursiveLock*) _lock;

/** Internal method to populate the cache with the result of a query.
 */
- (void) _populateCache: (CacheQuery*)a;

/** Internal method called to record the 'main' thread in which automated
 * cache updates are to be performed.
 */
- (void) _recordMainThread;

/**
 * Internal method to substitute values from the dictionary into
 * a string containing markup identifying where the values should
 * appear by name.  Non-string objects in the dictionary are quoted.<br />
 * Returns an array containing the statement as the first object and
 * any NSData objects following.  The NSData objects appear in the
 * statement strings as the marker sequence - <code>'?'''?'</code>
 */
- (NSMutableArray*) _substitute: (NSString*)str with: (NSDictionary*)vals;

/*
 * Called at one second intervals to ensure that our current timestamp
 * is reasonably accurate.
 */
+ (void) _tick: (NSTimer*)t;

@end

@interface	SQLClient (GSCacheDelegate)
- (BOOL) shouldKeepItem: (id)anObject
		withKey: (id)aKey
	       lifetime: (unsigned)lifetime
		  after: (unsigned)delay;
@end

@implementation	SQLClient

static unsigned int	maxConnections = 8;
static int	        poolConnections = 0;

+ (NSArray*) allClients
{
  NSMutableArray	*a;
  NSHashEnumerator	e;
  id                    o;

  [clientsLock lock];
  a = [NSMutableArray arrayWithCapacity: NSCountHashTable(clientsHash)];
  e = NSEnumerateHashTable(clientsHash);
  while (nil != (o = (id)NSNextHashEnumeratorItem(&e)))
    {
      [a addObject: o];
    }
  NSEndHashTableEnumeration(&e);
  [clientsLock unlock];
  return a;
}

+ (SQLClient*) clientWithConfiguration: (NSDictionary*)config
				  name: (NSString*)reference
{
  SQLClient	*o;

  if ([reference isKindOfClass: NSStringClass] == NO)
    {
      if (config == nil)
	{
	  reference = [[NSUserDefaults standardUserDefaults] objectForKey:
	    @"SQLClientName"];
	}
      else
	{
	  reference = [config objectForKey: @"SQLClientName"];
	}
      if ([reference isKindOfClass: NSStringClass] == NO)
	{
	  reference = @"Database";
	}
    }

  o = [self existingClient: reference];
  if (nil == o)
    {
      o = [[[SQLClient alloc] initWithConfiguration: config name: reference]
        autorelease];
    }
  return o;
}

+ (SQLClient*) existingClient: (NSString*)reference
{
  SQLClient	*existing;

  if ([reference isKindOfClass: NSStringClass] == NO)
    {
      reference = [[NSUserDefaults standardUserDefaults] stringForKey:
	@"SQLClientName"];
      if (reference == nil)
	{
	  reference = @"Database";
	}
    }

  [clientsLock lock];
  existing = (SQLClient*)NSMapGet(clientsMap, reference);
  [[existing retain] autorelease];
  [clientsLock unlock];
  return existing;
}

+ (void) initialize
{
  if (Nil == SQLClientClass && [SQLClient class] == self)
    {
      static id	modes[1];
      
      SQLClientClass = self;
      modes[0] = NSDefaultRunLoopMode;
      queryModes = [[NSArray alloc] initWithObjects: modes count: 1];
      GSTickerTimeNow();
      [SQLRecord class];	// Force initialisation
      if (0 == clientsHash)
        {
          clientsHash = NSCreateHashTable(NSNonOwnedPointerHashCallBacks, 0);
          clientsMap = NSCreateMapTable(NSObjectMapKeyCallBacks,
            NSNonRetainedObjectMapValueCallBacks, 0);
          clientsLock = [NSRecursiveLock new];
          beginStatement = [[NSArray arrayWithObject: beginString] retain];
          commitStatement = [[NSArray arrayWithObject: commitString] retain];
          rollbackStatement
            = [[NSArray arrayWithObject: rollbackString] retain];
          NSStringClass = [NSString class];
          NSArrayClass = [NSArray class];
          NSSetClass = [NSSet class];
          [NSTimer scheduledTimerWithTimeInterval: 1.0
                                           target: self
                                         selector: @selector(_tick:)
                                         userInfo: 0
                                          repeats: YES];
        }
    }
}

+ (unsigned int) maxConnections
{
  return maxConnections;
}

+ (void) purgeConnections: (NSDate*)since
{
  NSHashEnumerator	e;
  SQLClient		*o;
  unsigned int		connectionCount = 0;
  NSTimeInterval	t = [since timeIntervalSinceReferenceDate];

  [clientsLock lock];
  e = NSEnumerateHashTable(clientsHash);
  while (nil != (o = (SQLClient*)NSNextHashEnumeratorItem(&e)))
    {
      if (since != nil)
	{
	  NSTimeInterval	when = o->_lastOperation;

	  if (when < t && YES == o->connected)
	    {
	      [o disconnect];
	    }
	}
      if ([o connected] == YES)
	{
	  connectionCount++;
	}
    }
  NSEndHashTableEnumeration(&e);
  [clientsLock unlock];

  while (connectionCount >= (maxConnections + poolConnections))
    {
      SQLClient		*other = nil;
      NSTimeInterval	oldest = 0.0;
  
      connectionCount = 0;
      [clientsLock lock];
      e = NSEnumerateHashTable(clientsHash);
      while (nil != (o = (SQLClient*)NSNextHashEnumeratorItem(&e)))
	{
	  if (YES == o->connected)
	    {
	      NSTimeInterval	when = o->_lastOperation;

	      connectionCount++;
	      if (oldest == 0.0 || when < oldest)
		{
		  oldest = when;
		  other = o;
		}
	    }
	}
      NSEndHashTableEnumeration(&e);
      [clientsLock unlock];
      connectionCount--;
      if ([other debugging] > 0)
	{
	  [other debug:
	    @"Force disconnect of '%@' because max connections (%u) reached",
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
	  /* NB.  We leave the lock locked ... until a matching -commit
	   * or -rollback is called.  This prevents other threads from
	   * interfering with this transaction.
	   */
	}
      NS_HANDLER
	{
	  _inTransaction = NO;
	  [lock unlock];
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

- (NSString*) buildQuery: (NSString*)stmt, ...
{
  va_list	ap;
  NSString	*sql = nil;

  /*
   * First check validity and concatenate parts of the query.
   */
  va_start (ap, stmt);
  sql = [[self prepare: stmt args: ap] objectAtIndex: 0];
  va_end (ap);

  return sql;
}

- (NSString*) buildQuery: (NSString*)stmt with: (NSDictionary*)values
{
  NSString	*sql = nil;

  sql = [[self _substitute: stmt with: values] objectAtIndex: 0];

  return sql;
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

  /* Since we are in a transaction we must be doubly locked right now,
   * so we unlock once, and we still have the lock (which was locked
   * in the earlier call to the -begin method).
   */
  [lock unlock];
  _inTransaction = NO;
  NS_DURING
    {
      [self simpleExecute: commitStatement];
      [_statements removeAllObjects];
      [lock unlock];		// Locked by -begin
    }
  NS_HANDLER
    {
      [_statements removeAllObjects];
      [lock unlock];		// Locked by -begin
      [localException raise];
    }
  NS_ENDHANDLER
}

- (BOOL) connect
{
  if (NO == connected)
    {
      [lock lock];
      if (NO == connected)
	{
	  NS_DURING
	    {
              if (_connectFails > 1)
                {
                  NSTimeInterval	delay;
                  NSTimeInterval	elapsed;

                  /* If we have repeated connection failures, we enforce a
                   * delay of up to 30 seconds between connection attempts
                   * to avoid overloading the system with too frequent
                   * connection attempts.
                   */
                  delay = (_connectFails < 30) ? _connectFails : 30;
                  elapsed = GSTickerTimeNow() - _lastOperation;
                  if (elapsed < delay)
                    {
                      [NSThread sleepForTimeInterval: delay - elapsed];
                    }
                }

	      _lastStart = GSTickerTimeNow();
	      [self backendConnect];
              /* On establishng a new connection, we must restore any
               * listen instructions in the backend.
               */
              if (nil != _names)
                {
                  NSEnumerator  *e;
                  NSString      *n;

                  e = [_names objectEnumerator];
                  while (nil != (n = [e nextObject]))
                    {
                      [self backendListen: n];
                    }
                }
	      _connectFails = 0;
	    }
	  NS_HANDLER
	    {
	      _lastOperation = GSTickerTimeNow();
	      _connectFails++;
	      [lock unlock];
	      [localException raise];
	    }
	  NS_ENDHANDLER
	}
      [lock unlock];
      if (YES == connected)
        {
          NSNotificationCenter  *nc;

          nc = [NSNotificationCenter defaultCenter];
          [nc postNotificationName: SQLClientDidConnectNotification
                            object: self];
        }
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

  [clientsLock lock];
  NSHashRemove(clientsHash, (void*)self);
  if (_name != nil
    && (SQLClient*)NSMapGet(clientsMap, (void*)_name) == self)
    {
      NSMapRemove(clientsMap, (void*)_name);
    }
  [clientsLock unlock];
  nc = [NSNotificationCenter defaultCenter];
  [nc removeObserver: self];
  if (YES == connected) [self disconnect];
  [lock release]; lock = nil;
  [_client release]; _client = nil;
  [_database release]; _database = nil;
  [_password release]; _password = nil;
  [_user release]; _user = nil;
  [_name release]; _name = nil;
  [_statements release]; _statements = nil;
  [_cache release]; _cache = nil;
  [_cacheThread release]; _cacheThread = nil;
  if (0 != _observers)
    {
      NSNotificationCenter      *nc;
      NSMapEnumerator	        e;
      NSMutableSet              *n;
      id                        o;

      nc = [NSNotificationCenter defaultCenter];
      e = NSEnumerateMapTable(_observers);
      while (NSNextMapEnumeratorPair(&e, (void**)&o, (void**)&n) != 0)
        {
          NSEnumerator  *ne = [n objectEnumerator];
          NSString      *name;

          while (nil != (name = [ne nextObject]))
            {
              [nc removeObserver: o name: name object: nil];
            }
        }
      NSEndMapTableEnumeration(&e);
      NSFreeMapTable(_observers);
      _observers = 0;
    }
  [_names release]; _names = 0;
  [super dealloc];
}

- (NSString*) description
{
  NSMutableString	*s = [[NSMutableString new] autorelease];

  [lock lock];
  NS_DURING
    {
      [s appendFormat: @"Database      - %@\n", [self clientName]];
      [s appendFormat: @"  Name        - %@\n", [self name]];
      [s appendFormat: @"  DBase       - %@\n", [self database]];
      [s appendFormat: @"  DB User     - %@\n", [self user]];
      [s appendFormat: @"  Password    - %@\n",
        [self password] == nil ? @"unknown" : @"known"];
      [s appendFormat: @"  Connected   - %@\n", connected ? @"yes" : @"no"];
      [s appendFormat: @"  Transaction - %@\n",
        _inTransaction ? @"yes" : @"no"];
      if (_cache == nil)
        {
          [s appendString: @"\n"];
        }
      else
        {
          [s appendFormat: @"  Cache -       %@\n", _cache];
        }
    }
  NS_HANDLER
    {
      [lock unlock];
      [localException raise];
    }
  NS_ENDHANDLER
  [lock unlock];
  return s;
}

- (void) disconnect
{
  if (YES == connected)
    {
      NSNotificationCenter  *nc;

      [lock lock];
      if (YES == _inTransaction)
        {
          /* If we are inside a transaction we must be doubly locked,
           * so we do an unlock corresponding to the -begin before we
           * disconnect (the disconnect implicitly rolls back the
           * transaction).
           */
          _inTransaction = NO;
          [lock unlock];
        }
      if (YES == connected)
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
      nc = [NSNotificationCenter defaultCenter];
      [nc postNotificationName: SQLClientDidDisconnectNotification
                        object: self];
    }
}

- (NSInteger) execute: (NSString*)stmt, ...
{
  NSArray	*info;
  va_list	ap;

  va_start (ap, stmt);
  info = [self prepare: stmt args: ap];
  va_end (ap);
  return [self simpleExecute: info];
}

- (NSInteger) execute: (NSString*)stmt with: (NSDictionary*)values
{
  NSArray	*info;

  info = [self _substitute: stmt with: values];
  return [self simpleExecute: info];
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
  return [self initWithConfiguration: config name: reference pool: nil];
}


- (id) initWithConfiguration: (NSDictionary*)config
			name: (NSString*)reference
                        pool: (SQLClientPool*)pool
{
  NSNotification	*n;
  NSDictionary		*conf = config;
  id			existing;

  if (conf == nil)
    {
      // Pretend the defaults object is a dictionary.
      conf = (NSDictionary*)[NSUserDefaults standardUserDefaults];
    }

  if ([reference isKindOfClass: NSStringClass] == NO)
    {
      reference = [conf objectForKey: @"SQLClientName"];
      if ([reference isKindOfClass: NSStringClass] == NO)
	{
	  reference = [conf objectForKey: @"Database"];
	}
    }

  [clientsLock lock];
  _pool = pool;
  if (nil != _pool)
    {
      existing = nil;   // Pool, object ... can't already exist
    }
  else
    {
      existing = (SQLClient*)NSMapGet(clientsMap, reference);
    }
  if (nil == existing)
    {
      lock = [NSRecursiveLock new];	// Ensure thread-safety.
      [self setDebugging: [[self class] debugging]];
      [self setDurationLogging: [[self class] durationLogging]];
      [self setName: reference];	// Set name and store in cache.
      _statements = [NSMutableArray new];

      if ([conf isKindOfClass: [NSUserDefaults class]] == YES)
	{
	  NSNotificationCenter	*nc;

	  nc = [NSNotificationCenter defaultCenter];
	  [nc addObserver: self
		 selector: @selector(_configure:)
		     name: NSUserDefaultsDidChangeNotification
		   object: conf];
	}
      n = [NSNotification
	notificationWithName: NSUserDefaultsDidChangeNotification
	object: conf
	userInfo: nil];

      NSHashInsert(clientsHash, (void*)self);
      [self _configure: n];	// Actually set up the configuration.
    }
  else
    {
      [self release];
      self = [existing retain];
    }
  [clientsLock unlock];

  return self;
}

- (NSUInteger) hash
{
  return [[self database] hash] + [[self user] hash];
}

- (BOOL) isEqual: (id)other
{
  if (self == other)
    {
      return YES;
    }
  if ([self class] != [other class])
    {
      return NO;
    }
  if (NO == [[self database] isEqual: [other database]])
    {
      return NO;
    }
  if (NO == [[self user] isEqual: [other user]])
    {
      return NO;
    }
  return YES;
}

- (BOOL) isInTransaction
{
  return _inTransaction;
}

- (NSDate*) lastOperation
{
  if (_lastOperation > 0.0 && _connectFails == 0)
    {
      return [NSDate dateWithTimeIntervalSinceReferenceDate: _lastOperation];
    }
  return nil;
}

- (BOOL) lockBeforeDate: (NSDate*)limit
{
  if (nil == limit)
    {
      return [lock tryLock];
    }
  return [lock lockBeforeDate: limit];
}

- (SQLClient*) longestIdle: (SQLClient*)other
{
  NSTimeInterval        t0;
  NSTimeInterval        t1;

  NSAssert([other isKindOfClass: SQLClientClass], NSInvalidArgumentException);

  t0 = _lastOperation;
  if (t0 < _lastStart)
    {
      t0 = _lastStart;
    }
  if (NO == connected || 0 != _connectFails)
    {
      t0 = 0.0;
    }

  if (YES == [other isProxy])
    {
      t1 = 0.0;
    }
  else
    {
      t1 = other->_lastOperation;
      if (t1 < other->_lastStart)
        {
          t1 = other->_lastStart;
        }
      if (NO == connected || 0 != other->_connectFails)
        {
          t1 = 0.0;
        }
    }

  if (t0 <= 0.0 && t1 <= 0.0)
    {
      return nil;
    }
  if (t1 <= t0)
    {
      return other;
    }
  return self;
}

- (NSString*) name
{
  return _name;
}

- (NSString*) password
{
  return _password;
}

- (NSMutableArray*) prepare: (NSString*)stmt args: (va_list)args
{
  NSMutableArray	*ma = [NSMutableArray arrayWithCapacity: 2];
  NSString		*tmp = va_arg(args, NSString*);
  NSAutoreleasePool     *arp = [NSAutoreleasePool new];

  if (tmp != nil)
    {
      NSMutableString	*s = [NSMutableString stringWithCapacity: 1024];

      [s appendString: stmt];
      /*
       * Append any values from the nil terminated varargs
       */ 
      while (tmp != nil)
	{
	  if ([tmp isKindOfClass: NSStringClass] == NO)
	    {
	      if ([tmp isKindOfClass: [NSData class]] == YES)
		{
		  [ma addObject: tmp];
		  [s appendString: @"'?'''?'"];	// Marker.
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
  [arp release];
  return ma;
}

- (NSMutableArray*) query: (NSString*)stmt, ...
{
  va_list		ap;
  NSMutableArray	*result = nil;

  /*
   * First check validity and concatenate parts of the query.
   */
  va_start (ap, stmt);
  stmt = [[self prepare: stmt args: ap] objectAtIndex: 0];
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
  /**
   * For a nil object, we return NULL.
   */
  if (obj == nil || obj == null)
    {
      return @"NULL";
    }
  else if ([obj isKindOfClass: NSStringClass] == NO)
    {
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
      if ([obj isKindOfClass: NSDateClass] == YES)
	{
	  return [obj descriptionWithCalendarFormat:
	    @"'%Y-%m-%d %H:%M:%S.%F %z'" timeZone: nil locale: nil];
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
       * Just in case an NSNull subclass has been created by someone.
       * The normal NSNull instance should have been handled earlier.
       */
      if ([obj isKindOfClass: [NSNull class]] == YES)
	{
	  return @"NULL";
	}

      /**
       * For an NSArray or NSSet, we produce a bracketed list of the
       * (quoted) objects in the array.
       */
      if ([obj isKindOfClass: NSArrayClass] == YES ||
	[obj isKindOfClass: NSSetClass] == YES)
	{
	  NSMutableString	*ms = [NSMutableString stringWithCapacity: 100];
	  NSEnumerator		*enumerator = [obj objectEnumerator];
	  id			value = [enumerator nextObject];

	  [ms appendString: @"("];
	  if (value != nil)
	    {
	      [ms appendString: [self quote: value]];
	    }
	  while ((value = [enumerator nextObject]) != nil)
	    {
	      [ms appendString: @","];
	      [ms appendString: [self quote: value]];
	    }
	  [ms appendString: @")"];
	  return ms;
	}

      /**
       * For any other type of data, we just produce a quoted string
       * representation of the objects description.
       */
      obj = [obj description];
    }

  /* Get a string description of the object.  */
  obj = [self quoteString: obj];

  return obj;
}

- (NSString*) quotef: (NSString*)fmt, ...
{
  va_list	ap;
  NSString	*str;
  NSString	*quoted;

  va_start(ap, fmt);
  str = [[NSString allocWithZone: NSDefaultMallocZone()]
    initWithFormat: fmt arguments: ap];
  va_end(ap);

  quoted = [self quoteString: str];
  [str release];
  return quoted;
}

- (NSMutableString*) quoteArray: (NSArray *)a
                       toString: (NSMutableString *)s
                 quotingStrings: (BOOL)q
{
  [NSException raise: NSGenericException
    format: @"%@ not supported for this database", NSStringFromSelector(_cmd)]; 
  return nil;
}

- (NSString*) quoteBigInteger: (int64_t)i
{
  return [NSString stringWithFormat: @"%"PRId64, i];
}

- (NSString*) quoteCString: (const char *)s
{
  NSString	*str;
  NSString	*quoted;

  if (s == 0)
    {
      s = "";
    }
  str = [[NSString alloc] initWithCString: s];
  quoted = [self quoteString: str];
  [str release];
  return quoted;
}

- (NSString*) quoteChar: (char)c
{
  NSString	*str;
  NSString	*quoted;

  if (c == 0)
    {
      [NSException raise: NSInvalidArgumentException
		  format: @"Attempt to quote a nul character in -quoteChar:"];
    }
  str = [[NSString alloc] initWithFormat: @"%c", c];
  quoted = [self quoteString: str];
  [str release];
  return quoted;
}

- (NSString*) quoteFloat: (float)f
{
  return [NSString stringWithFormat: @"%f", f];
}

- (NSString*) quoteInteger: (int)i
{
  return [NSString stringWithFormat: @"%d", i];
}

- (NSString*) quoteString: (NSString *)s
{
  static NSCharacterSet	*special = nil;
  NSMutableString	*m;
  NSRange		r;
  unsigned		l;

  if (special == nil)
    {
      NSString	*stemp;

      /*
       * NB. length of C string is 2, so we include a nul character as a
       * special.
       */
      stemp = [[NSString alloc] initWithBytes: "'"
	 			       length: 2
		 		     encoding: NSASCIIStringEncoding];
      special = [NSCharacterSet characterSetWithCharactersInString: stemp];
      [stemp release];
      [special retain];
    }

  /*
   * Step through string removing nul characters
   * and escaping quote characters as required.
   */
  m = [[s mutableCopy] autorelease];
  l = [m length];
  r = NSMakeRange(0, l);
  r = [m rangeOfCharacterFromSet: special options: NSLiteralSearch range: r];
  while (r.length > 0)
    {
      unichar	c = [m characterAtIndex: r.location];

      if (c == 0)
	{
	  r.length = 1;
	  [m replaceCharactersInRange: r withString: @""];
	  l--;
	}
      else
        {
	  r.length = 0;
	  [m replaceCharactersInRange: r withString: @"'"];
	  l++;
	  r.location += 2;
        }
      r = NSMakeRange(r.location, l - r.location);
      r = [m rangeOfCharacterFromSet: special
			     options: NSLiteralSearch
			       range: r];
    }

  /* Add quoting around it.  */
  [m replaceCharactersInRange: NSMakeRange(0, 0) withString: @"'"];
  [m appendString: @"'"];
  return m;
}

- (oneway void) release
{
  /* We lock the table while checking, to prevent
   * another thread from grabbing this object while we are
   * checking it.
   * If we are going to deallocate the object, we first remove
   * it from the table so that no other thread will find it
   * and try to use it while it is being deallocated.
   */
  [clientsLock lock];
  if (NSDecrementExtraRefCountWasZero(self))
    {
      if (nil != _pool)
        {
          [_pool swallowClient: self];
        }
      else
        {
          [self dealloc];
        }
    }
  [clientsLock unlock];
}

- (id) retain
{
  NSIncrementExtraRefCount(self);
  return self;
}

- (void) rollback
{
  [lock lock];
  if (NO == _inTransaction)
    {
      [lock unlock];	// Not in a transaction ... nothing to do.
      return;
    }

  /* Since we are in a transaction we must be doubly locked right now,
   * so we unlock once, and we still have the lock (which was locked
   * in the earlier call to the -begin method).
   */
  [lock unlock];
  _inTransaction = NO;
  NS_DURING
    {
      [self simpleExecute: rollbackStatement];
      [_statements removeAllObjects];
      [lock unlock];		// Locked by -begin
    }
  NS_HANDLER
    {
      [_statements removeAllObjects];
      [lock unlock];		// Locked by -begin
      [localException raise];
    }
  NS_ENDHANDLER
}

- (void) setDatabase: (NSString*)s
{
  [lock lock];
  NS_DURING
    {
      if ([s isEqual: _database] == NO)
        {
          if (YES == connected)
            {
              [self disconnect];
            }
          s = [s copy];
          [_database release];
          _database = s;
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

- (void) setName: (NSString*)s
{
  [lock lock];
  NS_DURING
    {
      if ([s isEqual: _name] == NO)
        {
          [clientsLock lock];
          if (nil == _pool)
            {
              if (NSMapGet(clientsMap, s) != 0)
                {
                  [clientsLock unlock];
                  [lock unlock];
                  if ([self debugging] > 0)
                    {
                      [self
                        debug: @"Error attempt to re-use client name %@", s];
                    }
                  NS_VOIDRETURN;
                }
            }
          if (YES == connected)
            {
              [self disconnect];
            }
          if (_name != nil
            && (SQLClient*)NSMapGet(clientsMap, (void*)_name) == self)
            {
              NSMapRemove(clientsMap, (void*)_name);
            }
          s = [s copy];
          [_name release];
          _name = s;
          [_client release];
          _client = [[[NSProcessInfo processInfo] globallyUniqueString] retain];
          if (nil == _pool && _name != nil)
            {
              NSMapInsert(clientsMap, (void*)_name, (void*)self);
            }
          [clientsLock unlock];
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

- (void) setPassword: (NSString*)s
{
  [lock lock];
  NS_DURING
    {
      if ([s isEqual: _password] == NO)
        {
          if (YES == connected)
            {
              [self disconnect];
            }
          s = [s copy];
          [_password release];
          _password = s;
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

- (void) setShouldTrim: (BOOL)aFlag
{
  _shouldTrim = (YES == aFlag) ? YES : NO;
}

- (void) setUser: (NSString*)s
{
  [lock lock];
  NS_DURING
    {
      if ([s isEqual: _user] == NO)
        {
          if (YES == connected)
            {
              [self disconnect];
            }
          s = [s copy];
          [_user release];
          _user = s;
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

- (NSInteger) simpleExecute: (NSArray*)info
{
  NSInteger     result;
  NSString      *debug = nil;

  [lock lock];
  NS_DURING
    {
      NSString	        *statement;
      BOOL              isCommit = NO;
      BOOL              isRollback = NO;

      statement = [info objectAtIndex: 0];

      if ([statement isEqualToString: commitString])
        {
          isCommit = YES;
        }
      if ([statement isEqualToString: rollbackString])
        {
          isRollback = YES;
        }

      _lastStart = GSTickerTimeNow();
      result = [self backendExecute: info];
      _lastOperation = GSTickerTimeNow();
      [_statements addObject: statement];
      if (_duration >= 0)
	{
	  NSTimeInterval	d;

	  d = _lastOperation - _lastStart;
	  if (d >= _duration)
	    {
	      if (isCommit || isRollback)
		{
		  NSEnumerator	        *e = [_statements objectEnumerator];
                  NSMutableString       *m;

		  if (isCommit)
		    {
                      m = [NSMutableString stringWithFormat:
			@"Duration %g for transaction commit ...\n", d];
		    }
		  else 
		    {
                      m = [NSMutableString stringWithFormat:
			@"Duration %g for transaction rollback ...\n", d];
		    }
		  while ((statement = [e nextObject]) != nil)
		    {
                      [m appendFormat: @"  %@;\n", statement];
		    }
                  debug = m;
		}
	      else if ([self debugging] > 1)
		{
		  /*
		   * For higher debug levels, we log data objects as well
		   * as the query string, otherwise we omit them.
		   */
                  debug = [NSString stringWithFormat:
                    @"Duration %g for statement %@", d, info];
		}
	      else
		{
                  debug = [NSString stringWithFormat:
		    @"Duration %g for statement %@", d, statement];
		}
	    }
	}
      if (_inTransaction == NO)
	{
	  [_statements removeAllObjects];
	}
    }
  NS_HANDLER
    {
      result = -1;
      if (_inTransaction == NO)
	{
	  [_statements removeAllObjects];
	}
      [lock unlock];
      [localException raise];
    }
  NS_ENDHANDLER
  [lock unlock];
  if (nil != debug)
    {
      [self debug: @"%@", debug];
    }
  return result;
}

- (NSMutableArray*) simpleQuery: (NSString*)stmt
{
  return [self simpleQuery: stmt recordType: rClass listType: aClass];
}

- (NSMutableArray*) simpleQuery: (NSString*)stmt
		     recordType: (id)rtype
		       listType: (id)ltype
{
  NSMutableArray	*result = nil;
  NSString              *debug = nil;

  if (rtype == 0) rtype = rClass;
  if (ltype == 0) ltype = aClass;
  [lock lock];
  NS_DURING
    {
      _lastStart = GSTickerTimeNow();
      result = [self backendQuery: stmt recordType: rtype listType: ltype];
      _lastOperation = GSTickerTimeNow();
      if (_duration >= 0)
	{
	  NSTimeInterval	d;

	  d = _lastOperation - _lastStart;
	  if (d >= _duration)
	    {
	      debug = [NSString stringWithFormat:
                @"Duration %g for query %@", d, stmt];
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
  if (nil != debug)
    {
      [self debug: @"%@", debug];
    }
  return result;
}

- (void) unlock
{
  [lock unlock];
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

- (NSInteger) backendExecute: (NSArray*)info
{
  [NSException raise: NSInternalInconsistencyException
	      format: @"Called -%@ without backend bundle loaded",
    NSStringFromSelector(_cmd)];
  return -1;
}

- (void) backendListen: (NSString*)name
{
  return;
}

- (void) backendNotify: (NSString*)name payload: (NSString*)more
{
  [NSException raise: NSInternalInconsistencyException
	      format: @"Called -%@ without backend bundle implementation",
    NSStringFromSelector(_cmd)];
  return;
}

- (NSMutableArray*) backendQuery: (NSString*)stmt
{
  return [self backendQuery: stmt recordType: rClass listType: aClass];
}

- (NSMutableArray*) backendQuery: (NSString*)stmt
		      recordType: (id)rtype
		        listType: (id)ltype
{
  [NSException raise: NSInternalInconsistencyException
	      format: @"Called -%@ without backend bundle loaded",
    NSStringFromSelector(_cmd)];
  return nil;
}

- (void) backendUnlisten: (NSString*)name
{
  return;
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

- (void) _configure: (NSNotification*)n
{
  NSDictionary	*o;
  NSDictionary	*d;
  NSString	*s;
  Class		c;

  [lock lock];
  NS_DURING
    {
      o = [n object];
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
      if ([s isKindOfClass: NSStringClass] == NO)
        {
          s = @"Postgres";
        }

      c = NSClassFromString([@"SQLClient" stringByAppendingString: s]);
      if (c == nil)
        {
          NSString		*path;
          NSBundle		*bundle;
          NSArray		*paths;
          NSMutableArray	*tried;
          unsigned		count;

          paths = NSSearchPathForDirectoriesInDomains(NSLibraryDirectory,
            NSAllDomainsMask, YES);
          count = [paths count];
          tried = [NSMutableArray arrayWithCapacity: count];
          while (count-- > 0)
            {
              path = [paths objectAtIndex: count];
              path = [path stringByAppendingPathComponent: @"Bundles"];
              path = [path stringByAppendingPathComponent:
			     @"SQLClient"SOVERSION""];
              path = [path stringByAppendingPathComponent: s];
              path = [path stringByAppendingPathExtension: @"bundle"];
              bundle = [NSBundle bundleWithPath: path];
              if (bundle != nil)
                {
                  [tried addObject: path];
                  if ((c = [bundle principalClass]) != nil)
                    {
                      break;	// Found it.
                    }
                }
              /* Try alternative version with more libraries linked in.
               * In some systems and situations the dynamic linker needs
               * to haved the SQLClient, gnustep-base, and objc libraries
               * explicitly linked into the bundle, but in others it
               * requires them to not be linked. To handle that, we create
               * two versions of each bundle, the seond version has _libs
               * appended to the bundle name, and has the extra libraries linked.
               */
              path = [path stringByDeletingPathExtension];
              path = [path stringByAppendingString: @"_libs"];
              path = [path stringByAppendingPathExtension: @"bundle"];
              bundle = [NSBundle bundleWithPath: path];
              if (bundle != nil)
                {
                  [tried addObject: path];
                  if ((c = [bundle principalClass]) != nil)
                    {
                      break;	// Found it.
                    }
                }
            }
          if (c == nil)
            {
              if ([tried count] == 0)
                {
                  [self debug: @"unable to load bundle for '%@' server type"
                    @" ... failed to locate bundle in %@", s, paths];
                }
              else
                {
                  [self debug: @"unable to load backend class for '%@' server"
                    @" type ... dynamic library load failed in %@", s, tried];
                }
              [lock unlock];
              NS_VOIDRETURN;
            }
        }
      if (c != [self class])
        {
          if (YES == connected)
            {
              [self disconnect];
            }
#ifdef	GNUSTEP
          GSDebugAllocationRemove(object_getClass(self), self);
#endif
          object_setClass(self, c);
#ifdef	GNUSTEP
          GSDebugAllocationAdd(object_getClass(self), self);
#endif
        }

      s = [d objectForKey: @"Database"];
      if ([s isKindOfClass: NSStringClass] == NO)
        {
          s = [o objectForKey: @"Database"];
          if ([s isKindOfClass: NSStringClass] == NO)
            {
              s = nil;
            }
        }
      [self setDatabase: s];

      s = [d objectForKey: @"User"];
      if ([s isKindOfClass: NSStringClass] == NO)
        {
          s = [o objectForKey: @"User"];
          if ([s isKindOfClass: NSStringClass] == NO)
            {
              s = @"";
            }
        }
      [self setUser: s];

      s = [d objectForKey: @"Password"];
      if ([s isKindOfClass: NSStringClass] == NO)
        {
          s = [o objectForKey: @"Password"];
          if ([s isKindOfClass: NSStringClass] == NO)
            {
              s = @"";
            }
        }
      [self setPassword: s];
    }
  NS_HANDLER
    {
      [lock unlock];
      [localException raise];
    }
  NS_ENDHANDLER
  [lock unlock];
}

- (NSRecursiveLock*) _lock
{
  return lock;
}

- (void) _populateCache: (CacheQuery*)a
{
  GSCache	*cache;
  id		result;

  [lock lock];
  NS_DURING
    {
      result = [self backendQuery: a->query
		       recordType: a->recordType
		         listType: a->listType];
    }
  NS_HANDLER
    {
      result = nil;
      [lock unlock];
      [localException raise];
    }
  NS_ENDHANDLER
  [lock unlock];

  cache = [self cache];
  [cache setObject: result
	    forKey: a->query
	  lifetime: a->lifetime];
}

- (void) _recordMainThread
{
  mainThread = [NSThread currentThread];
}

- (NSMutableArray*) _substitute: (NSString*)str with: (NSDictionary*)vals
{
  unsigned int		l = [str length];
  NSRange		r;
  NSMutableArray	*ma = [NSMutableArray arrayWithCapacity: 2];
  NSAutoreleasePool     *arp = [NSAutoreleasePool new];

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
      NSMutableString	*mtext = [[str mutableCopy] autorelease];

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
	  id		o;
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
		  o = [(NSDictionary*)o objectForKey: k];
		}
	    }
	  if (o == vals)
	    {
	      v = nil;		// Mo match found.
	    }
	  else
	    {
	      if ([o isKindOfClass: NSStringClass] == YES)
		{
		  v = (NSString*)o;
		}
	      else
		{
		  if ([o isKindOfClass: [NSData class]] == YES)
		    {
		      [ma addObject: o];
		      v = @"'?'''?'";
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
	      if (v == nil)
	        {
		  v = @"";
	        }
	    }
	  vLength = [v length];

	  [mtext replaceCharactersInRange: r withString: v];
	  l += vLength;			// Add length of string inserted
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
  [arp release];
  return ma;
}

+ (void) _tick: (NSTimer*)t
{
  (void) GSTickerTimeNow();
}
@end

@implementation	SQLClient (GSCacheDelegate)
- (BOOL) shouldKeepItem: (id)anObject
		withKey: (id)aKey
	       lifetime: (unsigned)lifetime
		  after: (unsigned)delay
{
  CacheQuery	*a;
  NSDictionary	*d;

  a = [CacheQuery new];
  aKey = [aKey copy];
  [a->query release];
  a->query = aKey;
  d = [[NSThread currentThread] threadDictionary];
  a->recordType = [d objectForKey: @"SQLClientRecordType"];
  a->listType = [d objectForKey: @"SQLClientListType"];
  a->lifetime = lifetime;
  [a autorelease];
  if (_cacheThread == nil)
    {
      [self _populateCache: a];
    }
  else
    {
      /* We schedule an asynchronous update if the item is not too old,
       * otherwise (more than lifetime seconds past its expiry) we wait
       * for the update to complete.
       */
      [self performSelectorOnMainThread: @selector(_populateCache:)
			     withObject: a
			  waitUntilDone: (delay > lifetime) ? YES : NO
				  modes: queryModes];
    }
  return YES;	// Always keep items ... 
}
@end



@implementation	SQLClient(Convenience)

+ (NSMutableArray*) columns: (NSMutableArray*)records
{
  SQLRecord		*r = [records lastObject];
  unsigned		rowCount = [records count];
  unsigned		colCount = [r count];
  NSMutableArray	*m;

  if (rowCount == 0 || colCount == 0)
    {
      m = [NSMutableArray array];
    }
  else
    {
      NSMutableArray	*cols[colCount];
      unsigned		i;

      m = [NSMutableArray arrayWithCapacity: colCount];
      for (i = 0; i < colCount; i++)
	{
	  cols[i] = [[NSMutableArray alloc] initWithCapacity: rowCount];
	  [m addObject: cols[i]];
	  [cols[i] release];
	}
      for (i = 0; i < rowCount; i++)
	{
	  unsigned	j;

	  r = [records objectAtIndex: i];
	  for (j = 0; j < colCount; j++)
	    {
	      [cols[j] addObject: [r objectAtIndex: j]];
	    }
	}
    }
  return m;
}

+ (void) singletons: (NSMutableArray*)records
{
  unsigned	c = [records count];

  while (c-- > 0)
    {
      [records replaceObjectAtIndex: c
			 withObject: [[records objectAtIndex: c] lastObject]];
    }
}

- (SQLTransaction*) batch: (BOOL)stopOnFailure
{
  SQLTransaction        *transaction;

  transaction = (SQLTransaction*)NSAllocateObject([SQLTransaction class], 0,
    NSDefaultMallocZone());
 
  transaction->_db = [self retain];
  transaction->_info = [NSMutableArray new];
  transaction->_batch = YES;
  transaction->_stop = stopOnFailure;
  return [(SQLTransaction*)transaction autorelease];
}

- (NSMutableArray*) columns: (NSMutableArray*)records
{
  return [SQLClient columns: records];
}

- (SQLRecord*) queryRecord: (NSString*)stmt, ...
{
  va_list	ap;
  NSArray	*result = nil;
  SQLRecord	*record;

  va_start (ap, stmt);
  stmt = [[self prepare: stmt args: ap] objectAtIndex: 0];
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
  stmt = [[self prepare: stmt args: ap] objectAtIndex: 0];
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
  [SQLClient singletons: records];
}

- (SQLTransaction*) transaction
{
  SQLTransaction	*transaction;

  transaction = (SQLTransaction*)NSAllocateObject([SQLTransaction class], 0,
    NSDefaultMallocZone());
 
  transaction->_db = [self retain];
  transaction->_info = [NSMutableArray new];
  return [(SQLTransaction*)transaction autorelease];
}
@end



@interface	SQLClientCacheInfo : NSObject
{
@public
  NSString		*query;
  NSMutableArray	*result;
  NSTimeInterval	expires;
}
@end

@implementation	SQLClientCacheInfo
- (void) dealloc
{
  [query release]; query = nil;
  [result release]; result = nil;
  [super dealloc];
}
- (NSUInteger) hash
{
  return [query hash];
}
- (BOOL) isEqual: (id)other
{
  return [query isEqual: ((SQLClientCacheInfo*)other)->query];
}
@end

@implementation SQLClient (Caching)

- (GSCache*) cache
{
  GSCache	*c;

  [lock lock];
  if (nil == _cache)
    {
      _cache = [GSCache new];
      if (_cacheThread != nil)
	{
	  [_cache setDelegate: self];
	}
    }
  c = [_cache retain];
  [lock unlock];
  return [c autorelease];
}

- (NSMutableArray*) cacheCheckSimpleQuery: (NSString*)stmt
{
  NSMutableArray        *result = [[self cache] objectForKey: stmt];

  if (result != nil)
    {
      result = [[result mutableCopy] autorelease];
    }
  return result;
}

- (NSMutableArray*) cache: (int)seconds
		    query: (NSString*)stmt,...
{
  va_list		ap;

  va_start (ap, stmt);
  stmt = [[self prepare: stmt args: ap] objectAtIndex: 0];
  va_end (ap);

  return [self cache: seconds simpleQuery: stmt];
}

- (NSMutableArray*) cache: (int)seconds
		    query: (NSString*)stmt
		     with: (NSDictionary*)values
{
  stmt = [[self _substitute: stmt with: values] objectAtIndex: 0];
  return [self cache: seconds simpleQuery: stmt];
}

- (NSMutableArray*) cache: (int)seconds
	      simpleQuery: (NSString*)stmt
{
  return [self cache: seconds
	 simpleQuery: stmt
	  recordType: nil
	    listType: nil];
}

- (NSMutableArray*) cache: (int)seconds
	      simpleQuery: (NSString*)stmt
	       recordType: (id)rtype
	         listType: (id)ltype
{
  NSMutableArray	*result;
  NSMutableDictionary	*md;
  GSCache		*c;
  id			toCache;

  if (rtype == 0) rtype = rClass;
  if (ltype == 0) ltype = aClass;

  md = [[NSThread currentThread] threadDictionary];
  [md setObject: rtype forKey: @"SQLClientRecordType"];
  [md setObject: ltype forKey: @"SQLClientListType"];
  _lastStart = GSTickerTimeNow();
  c = [self cache];
  toCache = nil;

  if (seconds < 0)
    {
      seconds = -seconds;
      result = nil;
    }
  else
    {
      result = [c objectForKey: stmt];
    }

  if (result == nil)
    {
      CacheQuery	*a;

      a = [CacheQuery new];
      a->query = [stmt copy];
      a->recordType = rtype;
      a->listType = ltype;
      a->lifetime = seconds;
      [a autorelease];

      if (_cacheThread == nil)
	{
          [self _populateCache: a];
	}
      else
	{
	  /* Not really an asynchronous query becuse we wait until it's
	   * done in order to have a result we can return.
	   */
	  [self performSelectorOnMainThread: @selector(_populateCache:)
				 withObject: a
			      waitUntilDone: YES
				      modes: queryModes];
	}
      result = [c objectForKey: stmt];
      _lastOperation = GSTickerTimeNow();
      if (_duration >= 0)
	{
	  NSTimeInterval	d;

	  d = _lastOperation - _lastStart;
	  if (d >= _duration)
	    {
	      [self debug: @"Duration %g for query %@", d, stmt];
	    }
	}
    }

  if (seconds == 0)
    {
      // We have been told to remove the existing cached item.
      [c setObject: nil forKey: stmt lifetime: seconds];
      toCache = nil;
    }

  if (toCache != nil)
    {
      // We have a newly retrieved object ... cache it.
      [c setObject: toCache forKey: stmt lifetime: seconds];
    }

  if (result != nil)
    {
      /*
       * Return an autoreleased copy ... not the original cached data.
       */
      result = [[result mutableCopy] autorelease];
    }
  return result;
}

- (void) setCache: (GSCache*)aCache
{
  [lock lock];
  NS_DURING
    {
      if (_cacheThread != nil)
        {
          [_cache setDelegate: nil];
        }
      [aCache retain];
      [_cache release];
      _cache = aCache;
      if (_cacheThread != nil)
        {
          [_cache setDelegate: self];
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

- (void) setCacheThread: (NSThread*)aThread
{
  if (mainThread == nil)
    {
      [self performSelectorOnMainThread: @selector(_recordMainThread)
			     withObject: nil
			  waitUntilDone: NO
				  modes: queryModes];
    }
  if (aThread != nil && aThread != mainThread)
    {
      NSLog(@"SQLClient: only the main thread is usable as cache thread");
      aThread = mainThread;
    }
  [lock lock];
  NS_DURING
    {
      if (_cacheThread != nil)
        {
          [_cache setDelegate: nil];
        }
      [aThread retain];
      [_cacheThread release];
      _cacheThread = aThread;
      if (_cacheThread != nil)
        {
          [_cache setDelegate: self];
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
@end

@implementation	SQLTransaction

- (void) _addSQL: (NSMutableString*)sql andArgs: (NSMutableArray*)args
{
  unsigned      count = [_info count];
  unsigned      index;

  for (index = 0; index < count; index++)
    {
      id        o = [_info objectAtIndex: index];

      if ([o isKindOfClass: NSArrayClass] == YES)
        {
          unsigned      c = [(NSArray*)o count];

          if (c > 0)
            {
              unsigned  i;

              [sql appendString: [(NSArray*)o objectAtIndex: 0]];
              [sql appendString: @";"];
              for (i = 1; i < c; i++)
                {
                  [args addObject: [(NSArray*)o objectAtIndex: i]];
                }
            }
        }
      else
        {
          [(SQLTransaction*)o _addSQL: sql andArgs: args];
        }
    }
}

- (void) _addPrepared: (NSArray*)statement
{
  [_info addObject: statement];
  _count++;
}

- (void) _countLength: (unsigned*)length andArgs: (unsigned*)args
{
  unsigned      count = [_info count];
  unsigned      index;

  for (index = 0; index < count; index++)
    {
      id        o = [_info objectAtIndex: index];

      if ([o isKindOfClass: NSArrayClass] == YES)
        {
          unsigned      c = [(NSArray*)o count];

          if (c > 0)
            {
              length += [[(NSArray*)o objectAtIndex: 0] length] + 1;
              args += c - 1;
            }
        }
      else
        {
          [(SQLTransaction*)o _countLength: length andArgs: args];
        }
    }
}

/* Try to merge the prepared statement p with an earlier statement in the
 * transaction.  We search up to 5 earlier statements and we merge if we can.
 */
- (void) _merge: (NSMutableArray*)p
{
  if (_count > 0 && _merge > 0)
    {
      static NSCharacterSet     *w = nil;
      NSString  *s;
      NSRange   r;

      s = [p objectAtIndex: 0];         // Get SQL part of array

      if (nil == w)
        {
          w = [[NSCharacterSet whitespaceAndNewlineCharacterSet] retain];
        }

      r = [s rangeOfString: @"INSERT" options: NSCaseInsensitiveSearch];
      if (r.length > 0 && 0 == r.location)
        {
          r = [s rangeOfString: @"VALUES" options: NSCaseInsensitiveSearch];
          if (r.length > 0)
            {
              NSUInteger        l = [s length];
              NSUInteger        pos = NSMaxRange(r);

              while (pos < l
                && [w characterIsMember: [s characterAtIndex: pos]])
                {
                  pos++;
                }
              if (pos < l && [s characterAtIndex: pos] == '(')
                {
                  NSString              *t = [s substringToIndex: pos];
                  NSUInteger            index = _count;
                  NSUInteger            attempts = 0;

                  s = [s substringFromIndex: pos];
                  while (index-- > 0 && attempts++ < _merge)
                    {
                      NSMutableArray    *o;
                      NSString          *os;

                      o = [_info objectAtIndex: index];
                      os = [o objectAtIndex: 0];
                      if ([os hasPrefix: t])
                        {
                          NSMutableString       *m;

                          if ([os isKindOfClass: [NSMutableString class]])
                            {
                              m = (NSMutableString*)os;
                            }
                          else
                            {
                              m = [NSMutableString
                                stringWithCapacity: [os length] * 100];
                              [m appendString: os];
                            }
                          [m appendString: @","];
                          [m appendString: s];
                          [o replaceObjectAtIndex: 0 withObject: m];
                          for (index = 1; index < [p count]; index++)
                            {
                              [o addObject: [p objectAtIndex: index]];
                            }
                          return;
                        }
                    }
                }
            }
        }

      r = [s rangeOfString: @"UPDATE" options: NSCaseInsensitiveSearch];
      if (0 == r.length)
        {
          r = [s rangeOfString: @"DELETE" options: NSCaseInsensitiveSearch];
        }
      if (r.length > 0 && 0 == r.location)
        {
          r = [s rangeOfString: @"WHERE" options: NSCaseInsensitiveSearch];
          if (r.length > 0)
            {
              NSUInteger        l = [s length];
              NSUInteger        pos = NSMaxRange(r);

              while (pos < l
                && [w characterIsMember: [s characterAtIndex: pos]])
                {
                  pos++;
                }
              if (pos < l && [s characterAtIndex: pos] == '(')
                {
                  NSString              *t = [s substringToIndex: pos];
                  NSUInteger            index = _count;
                  NSUInteger            attempts = 0;

                  /* Get the condition after the WHERE and if it's not
                   * in brackets, add them so the merge can work.
                   */
                  s = [s substringFromIndex: pos];
                  if ([s characterAtIndex: 0] != '(')
                    {
                      s = [NSString stringWithFormat: @"(%@)", s];
                    }
                    
                  while (index-- > 0 && attempts++ < _merge)
                    {
                      NSMutableArray    *o;
                      NSString          *os;

                      o = [_info objectAtIndex: index];
                      os = [o objectAtIndex: 0];
                      if ([os hasPrefix: t])
                        {
                          NSMutableString       *m;

                          l = [os length];
                          if ([os characterAtIndex: l - 1] == ')')
                            {
                              if ([os isKindOfClass: [NSMutableString class]])
                                {
                                  m = (NSMutableString*)os;
                                }
                              else
                                {
                                  m = [NSMutableString
                                    stringWithCapacity: l * 100];
                                  [m appendString: os];
                                }
                            }
                          else
                            {
                              /* The condition of the WHERE clause was not
                               * bracketed, so we extract it and build a
                               * new statement in which it is bracketed.
                               */
                              os = [os substringFromIndex: pos];
                              m = [NSMutableString
                                stringWithCapacity: l * 100];
                              [m appendFormat: @"%@(%@)", t, os];
                            }
                          [m appendString: @" OR "];
                          [m appendString: s];
                          [o replaceObjectAtIndex: 0 withObject: m];
                          for (index = 1; index < [p count]; index++)
                            {
                              [o addObject: [p objectAtIndex: index]];
                            }
                          return;
                        }
                    }
                }
            }
        }
    }
  [_info addObject: p];
  _count++;
}

- (void) add: (NSString*)stmt,...
{
  va_list               ap;
  NSMutableArray        *p;

  va_start (ap, stmt);
  p = [_db prepare: stmt args: ap];
  va_end (ap);
  [self _merge: p];
}

- (void) add: (NSString*)stmt with: (NSDictionary*)values
{
  NSMutableArray        *p;

  p = [_db _substitute: stmt with: values];
  [self _merge: p];
}

- (void) append: (SQLTransaction*)other
{
  if (other != nil && other->_count > 0)
    {
      if (NO == [_db isEqual: other->_db])
	{
	  [NSException raise: NSInvalidArgumentException
		      format: @"[%@-%@] database client missmatch",
	    NSStringFromClass([self class]), NSStringFromSelector(_cmd)];
	}
      if (_merge > 0)
        {
          unsigned      index;

          /* Merging of statements is turned on ... try to merge statements
           * from other transaction rather than simply appending a copy of it.
           */
          for (index = 0; index < other->_count; index++)
            {
              [self _merge: [other->_info objectAtIndex: index]];
            }
        }
      else
        {
          other = [other copy];
          [_info addObject: other];
          _count += other->_count;
          [other release];
        }
    }
}

- (id) copyWithZone: (NSZone*)z
{
  SQLTransaction        *c;

  c = (SQLTransaction*)NSCopyObject(self, 0, z);
  c->_db = [c->_db retain];
  c->_info = [c->_info mutableCopy];
  return c;
}

- (NSUInteger) count
{
  return [_info count];
}

- (SQLClient*) db
{
  return _db;
}

- (void) dealloc
{
  [_db release]; _db = nil;
  [_info release]; _info = nil;
  [super dealloc];
}

- (NSString*) description
{
  return [NSString stringWithFormat: @"%@ with SQL '%@' for %@",
    [super description],
    (_count == 0 ? (id)@"" : (id)_info), _db];
}

- (void) execute
{
  if (_count > 0)
    {
      NSMutableArray    *info = nil;
      NSRecursiveLock   *dbLock = [_db _lock];
      BOOL              wrap;

      [dbLock lock];
      wrap = [_db isInTransaction] ? NO : YES;
      NS_DURING
	{
          NSMutableString   *sql;
          unsigned          sqlSize = 0;
          unsigned          argCount = 0;

          [self _countLength: &sqlSize andArgs: &argCount];

          /* Allocate and initialise the transaction statement.
           */
          info = [[NSMutableArray alloc] initWithCapacity: argCount + 1];
          sql = [[NSMutableString alloc] initWithCapacity: sqlSize + 13];
          [info addObject: sql];
          [sql release];
          if (YES == wrap)
            {
              [sql appendString: @"begin;"];
            }

          [self _addSQL: sql andArgs: info];

          if (YES == wrap)
            {
              [sql appendString: @"commit;"];
            }

          [_db simpleExecute: info];
          [info release]; info = nil;
          [dbLock unlock];
	}
      NS_HANDLER
	{
          NSException   *e = localException;

          [info release];
          if (YES == wrap)
            {
              NS_DURING
                {
                  [_db simpleExecute: rollbackStatement];
                }
              NS_HANDLER
                {
                  [_db disconnect];
                  NSLog(@"Disconnected due to failed rollback after %@", e);
                }
              NS_ENDHANDLER
            }
          [dbLock unlock];
          [e raise];
	}
      NS_ENDHANDLER
    }
}

- (unsigned) executeBatch
{
  return [self executeBatchReturningFailures: nil logExceptions: NO];
}

- (unsigned) executeBatchReturningFailures: (SQLTransaction*)failures
			     logExceptions: (BOOL)log
{
  unsigned      executed = 0;

  if (_count > 0)
    {
      NSRecursiveLock   *dbLock = [_db _lock];

      [dbLock lock];
      NS_DURING
        {
          [self execute];
          executed = _count;
        }
      NS_HANDLER
        {
	  if (log == YES || [_db debugging] > 0)
	    {
	      [_db debug: @"Initial failure executing batch %@: %@",
		self, localException];
	    }
          if (_batch == YES)
            {
	      SQLTransaction	*wrapper = nil;
              unsigned  	count = [_info count];
              unsigned  	i;

              for (i = 0; i < count; i++)
                {
                  BOOL      success = NO;
	          id        o = [_info objectAtIndex: i];

		  if ([o isKindOfClass: NSArrayClass] == YES)
		    {
		      NS_DURING
			{
			  /* Wrap the statement inside a transaction so
			   * its context will still be that of a statement
			   * in a transaction rather than a standalone
			   * statement.  This might be important if the
			   * statement is actually a call to a stored
			   * procedure whose code must all be executed
			   * with the visibility rules of a single
			   * transaction.
			   */
			  if (wrapper == nil)
			    {
			      wrapper = [_db transaction];
			    }
			  [wrapper reset];
			  [wrapper _addPrepared: o];
                          [wrapper execute];
                          executed++;
                          success = YES;
                        }
		      NS_HANDLER
			{
			  if (failures != nil)
			    {
			      [failures _addPrepared: o];
			    }
			  if (log == YES || [_db debugging] > 0)
			    {
			      [_db debug:
				@"Failure of %d executing batch %@: %@",
				i, self, localException];
			    }
			  success = NO;
			}
		      NS_ENDHANDLER
		    }
		  else
		    {
		      unsigned      result;

		      result = [(SQLTransaction*)o
			executeBatchReturningFailures: failures
			logExceptions: log];
		      executed += result;
		      if (result == [(SQLTransaction*)o totalCount])
			{
			  success = YES;
			}
		    }
                  if (success == NO && _stop == YES)
                    {
		      /* We are configured to stop after a failure,
		       * so we need to add all the subsequent statements
		       * or transactions to the list of those which have
		       * not been done.
		       */
		      i++;
		      while (i < count)
			{
			  id        o = [_info objectAtIndex: i++];

			  if ([o isKindOfClass: NSArrayClass] == YES)
			    {
			      [failures _addPrepared: o];
			    }
			  else
			    {
			      [failures append: (SQLTransaction*)o];
			    }
			}
                      break;
                    }
                }
            }
        }
      NS_ENDHANDLER
      [dbLock unlock];
    }
  return executed;
}

- (void) insertTransaction: (SQLTransaction*)trn atIndex: (unsigned)index
{
  if (index > [_info count])
    {
      [NSException raise: NSRangeException
		  format: @"[%@-%@] index too large",
	NSStringFromClass([self class]), NSStringFromSelector(_cmd)];
    }
  if (trn == nil || trn->_count == 0)
    {
      [NSException raise: NSInvalidArgumentException
		  format: @"[%@-%@] attempt to insert nil/empty transaction",
	NSStringFromClass([self class]), NSStringFromSelector(_cmd)];
    }
  if (NO == [_db isEqual: trn->_db])
    {
      [NSException raise: NSInvalidArgumentException
		  format: @"[%@-%@] database client missmatch",
	NSStringFromClass([self class]), NSStringFromSelector(_cmd)];
    }
  trn = [trn copy];
  [_info addObject: trn];
  _count += trn->_count;
  [trn release];
}

- (void) removeTransactionAtIndex: (unsigned)index
{
  id	o;

  if (index >= [_info count])
    {
      [NSException raise: NSRangeException
		  format: @"[%@-%@] index too large",
	NSStringFromClass([self class]), NSStringFromSelector(_cmd)];
    }
  o = [_info objectAtIndex:  index];
  if ([o isKindOfClass: NSArrayClass] == YES)
    {
      _count--;
    }  
  else
    {
      _count -= [(SQLTransaction*)o totalCount];
    }
  [_info removeObjectAtIndex: index];
}

- (void) reset
{
  [_info removeAllObjects];
  _count = 0;
}

- (uint8_t) setMerge: (uint8_t)history
{
  uint8_t       old = _merge;

  _merge = history;
  return old;
}

- (unsigned) totalCount
{
  return _count;
}

- (SQLTransaction*) transactionAtIndex: (unsigned)index
{
  id	o;

  if (index >= [_info count])
    {
      [NSException raise: NSRangeException
		  format: @"[%@-%@] index too large",
	NSStringFromClass([self class]), NSStringFromSelector(_cmd)];
    }
  o = [_info objectAtIndex: index];
  if ([o isKindOfClass: NSArrayClass] == YES)
    {
      SQLTransaction	*t = [[self db] transaction];

      [t _addPrepared: o];
      return t;
    }
  else
    {
      o = [o copy];
      return [o autorelease];
    }
}
@end


@implementation SQLClient (Notifications)

static NSString *
validName(NSString *name)
{
  const char    *ptr;

  if (NO == [name isKindOfClass: [NSString class]])
    {
      [NSException raise: NSInvalidArgumentException
        format: @"Notification name must be a string"];
    }
  ptr = [name UTF8String];
  if (!isalpha(*ptr))
    {
      [NSException raise: NSInvalidArgumentException
        format: @"Notification name must begin with letter"];
    }
  ptr++;
  while (0 != *ptr)
    {
      if (!isdigit(*ptr) && !isalpha(*ptr) && *ptr != '_')
        {
          [NSException raise: NSInvalidArgumentException
                      format: @"Notification name must contain only letters,"
            @" digits, and underscores"];
        }
      ptr++;
    }
  return [name lowercaseString];
}

- (void) addObserver: (id)anObserver
            selector: (SEL)aSelector
                name: (NSString*)name
{
  NSMutableSet          *set;

  name = validName(name);
  [lock lock];
  NS_DURING
    {
      if (nil == _observers)
        {
          _observers = NSCreateMapTable(NSNonRetainedObjectMapKeyCallBacks,
            NSObjectMapValueCallBacks, 0);
          _names = [NSCountedSet new];
        }
      set = (NSMutableSet*)NSMapGet(_observers, (void*)anObserver);
      if (nil == set)
        {
          set = [NSMutableSet new];
          NSMapInsert(_observers, anObserver, set);
          [set release];
        }
      if (nil == [set member: name])
        {
          NSUInteger        count = [_names countForObject: name];

          [set addObject: name];
          [_names addObject: name];
          if (0 == count)
            {
              [self backendListen: name];
            }
        }
      [[NSNotificationCenter defaultCenter] addObserver: anObserver
                                               selector: aSelector
                                                   name: name
                                                 object: self];
    }
  NS_HANDLER
    {
      [lock unlock];
      [localException raise];
    }
  NS_ENDHANDLER
  [lock unlock];
}

- (void) postNotificationName: (NSString*)name payload: (NSString*)more
{
  name = validName(name);
  if (nil != more)
    {
      if (NO == [more isKindOfClass: [NSString class]])
        {
          [NSException raise: NSInvalidArgumentException
                      format: @"Notification payload is not a string"];
        }
    }
  [lock lock];
  NS_DURING
    {
      [self backendNotify: name payload: more];
    }
  NS_HANDLER
    {
      [lock unlock];
      [localException raise];
    }
  NS_ENDHANDLER
  [lock unlock];
}

- (void) removeObserver: (id)anObserver name: (NSString*)name
{
  if (nil != name)
    {
      name = validName(name);
    }
  [lock lock];
  NS_DURING
    {
      if (_observers != nil)
        {
          NSNotificationCenter  *nc;
          NSMutableSet          *set;
          NSEnumerator          *e;

          nc = [NSNotificationCenter defaultCenter];
          set = (NSMutableSet*)NSMapGet(_observers, (void*)anObserver);
          if (nil == name)
            {
              e = [[set allObjects] objectEnumerator];
              name = [e nextObject];
            }
          else
            {
              e = nil;
              name = [[name retain] autorelease];
            }
          while (nil != name)
            {
              if (nil != [set member: name])
                {
                  [nc removeObserver: anObserver
                                name: name
                              object: self];
                  [[name retain] autorelease];
                  [set removeObject: name];
                  [_names removeObject: name];
                  if (0 == [_names countForObject: name])
                    {
                      [self backendUnlisten: name];
                    }
                }
              name = [e nextObject];
            }
          if ([set count] == 0)
            {
              NSMapRemove(_observers, (void*)anObserver);
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
@end



@implementation SQLDictionaryBuilder
- (void) addObject: (id)anObject
{
  return;
}

- (id) alloc
{
  return [self retain];
}

- (NSMutableDictionary*) content
{
  return content;
}

- (void) dealloc
{
  [content release];
  [super dealloc];
}

- (id) initWithCapacity: (NSUInteger)capacity
{
  if (nil != (self = [super init]))
    {
      DESTROY(content);
      content = [[NSMutableDictionary alloc] initWithCapacity: capacity];
    }
  return self;
}

- (id) mutableCopyWithZone: (NSZone*)aZone
{
  return [content mutableCopyWithZone: aZone];
}

- (id) newWithValues: (id*)values
		keys: (NSString**)keys
	       count: (unsigned int)count
{
  if (count != 2)
    {
      [NSException raise: NSInvalidArgumentException
                  format: @"Query did not return key/value pairs"];
    }
  [content setObject: values[1] forKey: values[0]];
  return nil;
}
@end

@implementation SQLSetBuilder
- (NSUInteger) added
{
  return added;
}

- (void) addObject: (id)anObject
{
  return;
}

- (id) alloc
{
  return [self retain];
}

- (NSCountedSet*) content
{
  return content;
}

- (void) dealloc
{
  [content release];
  [super dealloc];
}

- (id) initWithCapacity: (NSUInteger)capacity
{
  if (nil != (self = [super init]))
    {
      DESTROY(content);
      content = [[NSCountedSet alloc] initWithCapacity: capacity];
      added = 0;
    }
  return self;
}

- (id) mutableCopyWithZone: (NSZone*)aZone
{
  return [content mutableCopyWithZone: aZone];
}

- (id) newWithValues: (id*)values
		keys: (NSString**)keys
	       count: (unsigned int)count
{
  if (count != 1)
    {
      [NSException raise: NSInvalidArgumentException
                  format: @"Query did not return a single value"];
    }
  added++;
  [content addObject: values[0]];
  return nil;
}
@end

@implementation SQLSingletonBuilder
- (id) newWithValues: (id*)values
		keys: (NSString**)keys
	       count: (unsigned int)count
{
  /* Instead of creating an object to hold the supplied record,
   * we use the field from the record as the value to be used.
   */
  if (count != 1)
    {
      [NSException raise: NSInvalidArgumentException
                  format: @"Query did not return singleton values"];
    }
  return [values[0] retain];
}
@end

@implementation	SQLClientPool (Adjust)

+ (void) _adjustPoolConnections: (int)n
{
  unsigned      err = 0;

  [clientsLock lock];
  poolConnections += n;
  if (poolConnections < 0)
    {
      err -= poolConnections;
      poolConnections = 0;
    }
  [clientsLock unlock];
  NSAssert(0 == err, NSInvalidArgumentException);
}

@end

