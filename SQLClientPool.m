/* -*-objc-*- */

/** Implementation of SQLClientPool for GNUStep
   Copyright (C) 2014 Free Software Foundation, Inc.
   
   Written by:  Richard Frith-Macdonald <rfm@gnu.org>
   Date:	June 2014
   
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

   $Date: 2014-05-19 10:31:02 +0100 (Mon, 19 May 2014) $ $Revision: 37893 $
   */ 

#import	<Foundation/NSArray.h>
#import	<Foundation/NSAutoreleasePool.h>
#import	<Foundation/NSDebug.h>
#import	<Foundation/NSDictionary.h>
#import	<Foundation/NSException.h>
#import	<Foundation/NSInvocation.h>
#import	<Foundation/NSLock.h>
#import	<Foundation/NSString.h>
#import	<Foundation/NSUserDefaults.h>

#import	<Performance/GSCache.h>
#import	"SQLClient.h"

@interface      SQLClient(Pool)
- (void) _clearPool: (SQLClientPool*)p;
@end

@implementation SQLClient(Pool)
- (void) _clearPool: (SQLClientPool*)p
{
  NSAssert(_pool == p, NSInternalInconsistencyException);

  _pool = nil;
}
@end

@interface SQLClientPool (Adjust)
+ (void) _adjustPoolConnections: (int)n;
@end

@interface SQLClientPool (Private)
- (void) _lock;
- (NSString*) _rc: (SQLClient*)o;
- (void) _unlock;
@end

@implementation	SQLClientPool

- (int) availableConnections
{
  int   available;
  int   index;

  [self _lock];
  available = index = max;
  while (index-- > 0)
    {
      if (YES == u[index])
        {
          available--;
        }
    }
  [self _unlock];
  return available;
}

- (GSCache*) cache
{
  return [c[0] cache];
}

- (void) dealloc
{
  SQLClient             **clients;
  NSTimeInterval        *times;
  BOOL                  *used;
  int                   count;
  int                   i;

  [lock lock];
  count = max;
  max = 0;
  min = 0;
  clients = c;
  times = t;
  used = u;
  c = 0;
  t = 0;
  u = 0;
  [lock unlock];
  DESTROY(lock);
  DESTROY(_config);
  DESTROY(_name);
  if (0 != clients)
    {
      for (i = 0; i < count; i++)
        {
          [clients[i] _clearPool: self];
          if (NO == used[i])
            {
              [clients[i] release];
            }
        }
      free(clients);
      free(times);
      free(used);
    }
  [SQLClientPool _adjustPoolConnections: -count];
  [super dealloc];
}

- (NSString*) description
{
  return [NSString stringWithFormat: @"%@ '%@'", [super description], _name];
}

- (id) initWithConfiguration: (NSDictionary*)config
			name: (NSString*)reference
                         max: (int)maxConnections
                         min: (int)minConnections
{
  if (nil != (self = [super init]))
    {
      if (nil == config)
        {
          config = (NSDictionary*)[NSUserDefaults standardUserDefaults];
        }
      ASSIGN(_config, config);
      if (NO == [reference isKindOfClass: [NSString class]])
        {
          reference = [_config objectForKey: @"SQLClientName"];
          if (NO == [reference isKindOfClass: [NSString class]])
            {
              reference = @"Database";
            }
        }
      ASSIGNCOPY(_name, reference);
      lock = [[NSConditionLock alloc] initWithCondition: 0];
      [self setMax: maxConnections min: minConnections];
    }
  return self;
}

- (NSString*) longDescription
{
  NSMutableString	*s = [[NSMutableString new] autorelease];

  [s appendString: [self description]];
  [s appendString: @", "];
  [s appendString: [self status]];
  [s appendString: [self statistics]];
  [s appendString: [c[0] description]];
  return s;
}

- (int) maxConnections
{
  return max;
}

- (int) minConnections
{
  return min;
}

- (NSString*) name
{
  return  _name;
}

- (SQLClient*) provideClient
{
  return [self provideClientBeforeDate: nil];
}

- (SQLClient*) provideClientBeforeDate: (NSDate*)when
{
  NSTimeInterval        start = [NSDate timeIntervalSinceReferenceDate];
  NSTimeInterval        now = start;
  SQLClient             *client;
  int                   connected = -1;
  int                   found = -1;
  int                   cond = 0;
  int                   index;

  /* If we haven't been given a timeout, we should wait for a client
   * indefinitely ... so we set the timeout to be in the distant future.
   */
  if (nil == when)
    {
      static NSDate     *future = nil;

      if (nil == future)
        {
          future = RETAIN([NSDate distantFuture]);
        }
      when = future;
    }

  /* We want to log stuff if we don't get a client quickly.
   * Ideally we get the lock straight away,
   * but if not we want to log every ten seconds (and possibly
   * when we begin waiting).
   */
  if (YES == [lock tryLockWhenCondition: 1])
    {
      _immediate++;
    }
  else
    {
      NSTimeInterval    end = [when timeIntervalSinceReferenceDate];
      NSTimeInterval    dif = 0.0;
      NSDate            *until;
      BOOL              locked;

      if (_debugging > 1)
        {
          NSLog(@"%@ has no clients available", self);
        }
      until = [[NSDate alloc]
        initWithTimeIntervalSinceReferenceDate: now + 10.0];
      locked = NO;
      while (NO == locked && now < end)
        {
          if (now >= end)
            {
              /* End date is passed ... try to get the lock immediately.
               */
              locked = [lock tryLockWhenCondition: 1];
            }
          else if ([when earlierDate: until] == until)
            { 
              locked = [lock lockWhenCondition: 1 beforeDate: until];
            }
          else
            { 
              locked = [lock lockWhenCondition: 1 beforeDate: when];
            }
          now = [NSDate timeIntervalSinceReferenceDate];
          dif = now - start;
          if (NO == locked && now < end)
            {
              if (_debugging > 0 || dif > 30.0
                || (_duration >= 0.0 && dif > _duration))
                {
                  NSLog(@"%@ still waiting after %g seconds:\n%@",
                    self, dif, [self status]);
                }
              [until release];
              until = [[NSDate alloc] initWithTimeIntervalSinceNow: 10.0];
            }
        }
      [until release];
      if (dif > _longest)
        {
          _longest = dif;
        }
      if (NO == locked)
        {
          if (_debugging > 0 || dif > 30.0
            || (_duration >= 0.0 && dif > _duration))
            {
              NSLog(@"%@ abandoned wait after %g seconds:\n%@",
                self, dif, [self status]);
            }
          _failed++;
          _failWaits += dif;
          return nil;
        }
      if (_debugging > 0 || (_duration >= 0.0 && dif > _duration))
        {
          NSLog(@"%@ provided client after %g seconds",
            self, dif);
        }
      _delayed++;
      _delayWaits += dif;
    }

  for (index = 0; index < max && 0 == cond; index++)
    {
      if (NO == u[index])
        {
          if (connected >= 0 || found >= 0)
            {
              /* There's at least one more client available to be
               * provided, so we want to re-lock with condition 1.
               */
              cond = 1;
            }
          if (connected < 0 && YES == [c[index] connected])
            {
              connected = index;
            }
          else
            {
              found = index;
            }
        }
    }

  /* We prefer to use a client which is already connected, so we
   * avoid opening unnecessary connections.
   */
  if (connected >= 0)
    {
      found = connected;
    }
  u[found] = YES;
  t[found] = now;
  [lock unlockWithCondition: cond];
  client = [c[found] autorelease];
  if (_debugging > 2)
    {
      NSLog(@"%@ provides %p%@", self, c[found], [self _rc: client]);
    }
  return client;
}

- (void) purge
{
  BOOL  more = YES;

  [self _lock];

  while (YES == more)
    {
      SQLClient *found = nil;
      int       connected = 0;
      int       index;

      more = NO;
      for (index = 0; index < max; index++)
        {
          if (YES == [c[index] connected])
            {
              /* This is a connected client.
               */
              connected++;
              if (NO == u[index])
                {
                  /* Not in use; so a candidate to be purged
                   */
                  found = [c[index] longestIdle: found];
                }
            }
        }
      if (nil != found)
        {
          NSTimeInterval        age;

          age = -[[found lastOperation] timeIntervalSinceNow];
          if (_debugging > 2)
            {
              NSLog(@"%@ purge found %p age %g",
                self, found, age);
            }
          if (age > _purgeAll
            || (connected > min && age > _purgeMin))
            {
              NS_DURING
                {
                  [found disconnect];
                  more = YES;
                }
              NS_HANDLER
                {
                  NSLog(@"Error disconnecting client in pool: %@",
                    localException);
                }
              NS_ENDHANDLER
            }
        }
    }
  [self _unlock];
}

- (void) setCache: (GSCache*)aCache
{
  int   index;

  /* We don't allow a nil cache for the pool (each client would create its
   * own cache on demand). So we treat a nil cache as a request to create
   * a new cache with the default config.
   */
  [self _lock];
  if (nil == aCache)
    {
      [c[0] setCache: nil];
      aCache = [c[0] cache];
    }
  for (index = 1; index < max; index++)
    {
      [c[index] setCache: aCache];
    }
  [self _unlock];
}

- (void) setCacheThread: (NSThread*)aThread
{
  int   index;

  [self _lock];
  for (index = 0; index < max; index++)
    {
      [c[index] setCacheThread: aThread];
    }
  [self _unlock];
}

- (void) setDebugging: (unsigned int)level
{
  int   index;

  [self _lock];
  _debugging = level;
  for (index = 0; index < max; index++)
    {
      [c[index] setDebugging: _debugging];
    }
  [self _unlock];
}

- (void) setDurationLogging: (NSTimeInterval)threshold
{
  int   index;

  [self _lock];
  _duration = threshold;
  for (index = 0; index < max; index++)
    {
      [c[index] setDurationLogging: _duration];
    }
  [self _unlock];
}

- (void) setMax: (int)maxConnections min: (int)minConnections
{
  int   old;
  int   index;

  if (minConnections < 1) minConnections = 1;
  if (maxConnections > 100) maxConnections = 100;
  if (minConnections > maxConnections) minConnections = maxConnections;

  [self _lock];
  old = max;
  if (maxConnections != max)
    {
      GSCache   *cache = nil;

      if (max > 0)
        {
          while (max > maxConnections)
            {
              max--;
              [c[max] _clearPool: self];
              if (NO == u[max])
                {
                  [c[max] release];
                }
            }
          c = realloc(c, maxConnections * sizeof(SQLClient*));
          t = realloc(t, maxConnections * sizeof(NSTimeInterval));
          u = realloc(u, maxConnections * sizeof(BOOL));
        }
      else
        {
          c = calloc(maxConnections, sizeof(SQLClient*));
          t = calloc(maxConnections, sizeof(NSTimeInterval));
          u = calloc(maxConnections, sizeof(BOOL));
        }
      for (index = max; index < maxConnections; index++)
        {
          u[index] = NO;
          c[index] = [[SQLClient alloc] initWithConfiguration: _config
                                                         name: _name
                                                         pool: self];

          /* All the clients in the pool should share the same cache.
           */
          if (0 == index)
            {
              cache = [c[index] cache];
            }
          else
            {
              [c[index] setCache: cache];
            }
        }
      max = maxConnections;
      [SQLClientPool _adjustPoolConnections: max - old];
    }
  min = minConnections;
  [self _unlock];
}

- (void) setPurgeAll: (int)allSeconds min: (int)minSeconds
{
  if (allSeconds < 1)
    {
      allSeconds = 300;
    }
  if (minSeconds < 1)
    {
      minSeconds = 10;
    }
  if (allSeconds < minSeconds)
    {
      allSeconds = minSeconds;
    }
  _purgeMin = minSeconds;
  _purgeAll = allSeconds;
}

- (NSString*) statistics
{
  NSString      *s;

  s = [NSString stringWithFormat:
    @"  Immediate provisions: %llu\n"
    @"  Delayed provisions:   %llu\n"
    @"  Timed out provisions: %llu\n"
    @"  Slowest provision:    %g\n"
    @"  Average delay:        %g\n"
    @"  Average timeout:      %g\n"
    @"  Average over all:     %g\n",
    (unsigned long long)_immediate,
    (unsigned long long)_delayed,
    (unsigned long long)_failed,
    _longest,
    (_delayed > 0) ? _delayWaits / _delayed : 0.0,
    (_failed > 0) ? _failWaits / _failed : 0.0,
    (_immediate + _delayed + _failed) > 0
      ? (_failWaits + _delayWaits) / (_immediate + _delayed + _failed)
      : 0.0];
  return s;
}

- (NSString*) status
{
  NSMutableArray        *idleInfo = nil;
  NSMutableArray        *liveInfo = nil;
  unsigned int          cond = 0;
  unsigned int          free = 0;
  unsigned int          dead = 0;
  unsigned int          idle = 0;
  unsigned int          live = 0;
  unsigned int          used = 0;
  int                   index;
  NSMutableString       *s;

  [lock lock];
  for (index = 0; index < max; index++)
    {
      SQLClient *client = c[index];

      /* Check to see if this client is free to be taken from the pool.
       * Also, if a client is connected but not in use, we call it idle.
       */
      if (YES == u[index])
        {
          /* This is a client which has been provided by the pool,
           * so it is in use by some code.
           */
          if (YES == [client isInTransaction])
            {
              NSDate    *d = [client lastOperation];

              live++;
              if (nil == liveInfo)
                {
                  liveInfo = [NSMutableArray array];
                }
              [liveInfo addObject: [NSString stringWithFormat:
                @"  Client '%@' active in transaction since %@\n",
                [client name], d]];
            }
          else
            {
              if (YES == [client connected])
                {
                  NSDate        *d = [client lastOperation];

                  used++;
                  if (nil == d)
                    {
                      d = [client lastConnect];
                    }
                  if ([d timeIntervalSinceReferenceDate] < t[index])
                    {
                      d = [NSDate dateWithTimeIntervalSinceReferenceDate:
                        t[index]];
                    }
                  if (nil == idleInfo)
                    {
                      idleInfo = [NSMutableArray array];
                    }
                  [idleInfo addObject: [NSString stringWithFormat:
                    @"  Client '%@' taken from pool but idle since %@\n",
                    [client name], d]];
                }
              else
                {
                  idle++;
                }
            }
        }
      else
        {
          /* The client is not in use and can be provided by the pool,
           * so we must therefore re-lock with condition 1.
           */
          cond = 1;
          if (YES == [c[index] connected])
            {
              /* Still connected, so we count it as a free connection.
               */
              free++;
            }
          else
            {
              /* Not connected, so we count it as a dead connection.
               */
              dead++;
            }
        }
    }

  s = [NSMutableString stringWithFormat: @" size min: %u, max: %u\n"
    @"live:%u, used:%u, idle:%u, free:%u, dead:%u\n",
    min, max, live, used, idle, free, dead];
  if (liveInfo)
    {
      for (index = 0; index < [liveInfo count]; index++)
        {
          [s appendString: [liveInfo objectAtIndex: index]];
        }
    }
  if (idleInfo)
    {
      for (index = 0; index < [idleInfo count]; index++)
        {
          [s appendString: [idleInfo objectAtIndex: index]];
        }
    }
  [lock unlockWithCondition: cond];
  return s;
}

- (BOOL) _swallowClient: (SQLClient*)client withRetain: (BOOL)shouldRetain
{
  BOOL  found = NO;
  int   index;

  if (YES == [client isInTransaction])
    {
      /* The client has a transaction in progress ... if it's in the
       * current thread we should be able to disconnect (implicit rollback)
       * and return the client to the pool, otherwise we raise an exception.
       */
      if (YES == [client lockBeforeDate: nil])
        {
          [client disconnect];
          [client unlock];
          NSLog(@"ERROR: Disconnected client which was returned to pool"
            @" while a transaction was in progress: %@", client);
        }
      else
        {
          [NSException raise: SQLConnectionException
            format: @"failed to return to pool because a transaction"
            @" was in progress: %@", client];
        }
    }

  [self _lock];
  for (index = 0; index < max && NO == found; index++)
    {
      if (YES == u[index] && client == c[index])
        {
          u[index] = NO;
          found = YES;
          if (YES == shouldRetain)
            {
              NSIncrementExtraRefCount(client);
            }
        }
    }
  [self _unlock];

  if (_debugging > 2)
    {
      if (YES == found)
        {
          NSLog(@"%@ swallows %p%@", self, client, [self _rc: client]);
        }
      else
        {
          NSLog(@"%@ rejects %p%@", self, client, [self _rc: client]);
        }
    }

  return found;
}

- (BOOL) swallowClient: (SQLClient*)client
{
  return [self _swallowClient: client withRetain: YES];
}
@end

@implementation SQLClientPool (Private)

- (void) _lock
{
  [lock lock];
}

- (NSString*) _rc: (SQLClient*)o
{
#if     defined(GNUSTEP)
  if (_debugging > 3)
    {
      static Class      cls = Nil;
      unsigned long     rc;
      unsigned long     ac;

      if (Nil == cls)
        {
          cls = [NSAutoreleasePool class];
        }
      rc = (unsigned long)[o retainCount];
      ac = (unsigned long)[cls autoreleaseCountForObject: o];
      return [NSString stringWithFormat: @" refs %ld (%lu-%lu)",
        rc - ac, rc, ac];
    }
#endif
  return @"";
}

- (void) _unlock
{
  int   index;

  for (index = 0; index < max; index++)
    {
      /* Check to see if this client is free to be taken from the pool.
       */
      if (NO == u[index])
        {
          [lock unlockWithCondition: 1];
          return;
        }
    }
  [lock unlockWithCondition: 0];
}

@end

@implementation SQLClientPool (ConvenienceMethods)

- (NSString*) buildQuery: (NSString*)stmt, ...
{
  NSString	*sql;
  va_list	ap;

  /*
   * First check validity and concatenate parts of the query.
   */
  va_start (ap, stmt);
  sql = [[c[0] prepare: stmt args: ap] objectAtIndex: 0];
  va_end (ap);

  return sql;
}

- (NSString*) buildQuery: (NSString*)stmt with: (NSDictionary*)values
{
  NSString      *result = [c[0] buildQuery: stmt with: values];

  return result;
}

- (NSMutableArray*) cache: (int)seconds
		    query: (NSString*)stmt,...
{
  SQLClient             *db;
  NSMutableArray        *result;
  va_list	        ap;

  va_start (ap, stmt);
  stmt = [[c[0] prepare: stmt args: ap] objectAtIndex: 0];
  va_end (ap);

  db = [self provideClient];
  NS_DURING
    result = [db cache: seconds simpleQuery: stmt];
  NS_HANDLER
    [self swallowClient: db];
    [localException raise];
  NS_ENDHANDLER
  [self swallowClient: db];
  return result;
}

- (NSMutableArray*) cache: (int)seconds
		    query: (NSString*)stmt
		     with: (NSDictionary*)values
{
  SQLClient             *db;
  NSMutableArray        *result;

  db = [self provideClient];
  NS_DURING
    result = [db cache: seconds query: stmt with: values];
  NS_HANDLER
    [self swallowClient: db];
    [localException raise];
  NS_ENDHANDLER
  [self swallowClient: db];
  return result;
}

- (NSMutableArray*) cache: (int)seconds simpleQuery: (NSString*)stmt;
{
  SQLClient             *db;
  NSMutableArray        *result;

  db = [self provideClient];
  NS_DURING
    result = [db cache: seconds simpleQuery: stmt];
  NS_HANDLER
    [self swallowClient: db];
    [localException raise];
  NS_ENDHANDLER
  [self swallowClient: db];
  return result;
}

- (NSMutableArray*) cache: (int)seconds
	      simpleQuery: (NSString*)stmt
	       recordType: (id)rtype
	         listType: (id)ltype
{
  SQLClient             *db;
  NSMutableArray        *result;

  db = [self provideClient];
  NS_DURING
    result = [db cache: seconds
           simpleQuery: stmt
            recordType: rtype
              listType: ltype];
  NS_HANDLER
    [self swallowClient: db];
    [localException raise];
  NS_ENDHANDLER
  [self swallowClient: db];
  return result;
}

- (NSMutableArray*) columns: (NSMutableArray*)records
{
  return [SQLClient columns: records];
}

- (NSInteger) execute: (NSString*)stmt, ...
{
  SQLClient     *db;
  NSInteger     result;
  NSArray	*info;
  va_list	ap;

  va_start (ap, stmt);
  info = [c[0] prepare: stmt args: ap];
  va_end (ap);
  db = [self provideClient];
  NS_DURING
    result = [db simpleExecute: info];
  NS_HANDLER
    [self swallowClient: db];
    [localException raise];
  NS_ENDHANDLER
  [self swallowClient: db];
  return result;
}

- (NSInteger) execute: (NSString*)stmt with: (NSDictionary*)values
{
  SQLClient     *db;
  NSInteger     result;

  db = [self provideClient];
  NS_DURING
    result = [db execute: stmt with: values];
  NS_HANDLER
    [self swallowClient: db];
    [localException raise];
  NS_ENDHANDLER
  [self swallowClient: db];
  return result;
}

- (NSMutableArray*) query: (NSString*)stmt, ...
{
  SQLClient             *db;
  NSMutableArray	*result;
  va_list		ap;

  /*
   * First check validity and concatenate parts of the query.
   */
  va_start (ap, stmt);
  stmt = [[c[0] prepare: stmt args: ap] objectAtIndex: 0];
  va_end (ap);

  db = [self provideClient];
  NS_DURING
    result = [db simpleQuery: stmt];
  NS_HANDLER
    [self swallowClient: db];
    [localException raise];
  NS_ENDHANDLER
  [self swallowClient: db];

  return result;
}

- (NSMutableArray*) query: (NSString*)stmt with: (NSDictionary*)values
{
  SQLClient             *db;
  NSMutableArray        *result;

  db = [self provideClient];
  NS_DURING
    result = [db query: stmt with: values];
  NS_HANDLER
    [self swallowClient: db];
    [localException raise];
  NS_ENDHANDLER
  [self swallowClient: db];
  return result;
}

- (SQLRecord*) queryRecord: (NSString*)stmt, ...
{
  SQLClient     *db;
  NSArray	*result;
  SQLRecord	*record;
  va_list	ap;

  va_start (ap, stmt);
  stmt = [[c[0] prepare: stmt args: ap] objectAtIndex: 0];
  va_end (ap);

  db = [self provideClient];
  NS_DURING
    result = [db simpleQuery: stmt];
  NS_HANDLER
    [self swallowClient: db];
    [localException raise];
  NS_ENDHANDLER
  [self swallowClient: db];

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
  SQLClient     *db;
  NSArray	*result;
  SQLRecord	*record;
  va_list	ap;

  va_start (ap, stmt);
  stmt = [[c[0] prepare: stmt args: ap] objectAtIndex: 0];
  va_end (ap);

  db = [self provideClient];
  NS_DURING
    result = [db simpleQuery: stmt];
  NS_HANDLER
    [self swallowClient: db];
    [localException raise];
  NS_ENDHANDLER
  [self swallowClient: db];

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

- (NSString*) quote: (id)obj
{
  NSString      *result = [c[0] quote: obj];

  return result;
}

- (NSMutableString*) quoteArray: (NSArray *)a
                       toString: (NSMutableString *)s
                 quotingStrings: (BOOL)_q
{
  NSMutableString       *result;

  result = [c[0] quoteArray: a toString:s quotingStrings: _q];

  return result;
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
  quoted = [c[0] quoteString: str];
  [str release];
  return quoted;
}

- (NSString*) quoteBigInteger: (int64_t)i
{
  NSString      *result = [c[0] quoteBigInteger: i];

  return result;
}

- (NSString*) quoteCString: (const char *)s
{
  NSString      *result = [c[0] quoteCString: s];

  return result;
}

- (NSString*) quoteChar: (char)chr
{
  NSString      *result = [c[0] quoteChar: chr];

  return result;
}

- (NSString*) quoteFloat: (float)f
{
  NSString      *result = [c[0] quoteFloat: f];

  return result;
}

- (NSString*) quoteInteger: (int)i
{
  NSString      *result = [c[0] quoteInteger: i];

  return result;
}

- (NSString*) quoteString: (NSString *)s
{
  NSString      *result = [c[0] quoteString: s];

  return result;
}

- (NSInteger) simpleExecute: (NSArray*)info
{
  SQLClient     *db;
  NSInteger     result;

  db = [self provideClient];
  NS_DURING
    result = [db simpleExecute: info];
  NS_HANDLER
    [self swallowClient: db];
    [localException raise];
  NS_ENDHANDLER
  [self swallowClient: db];
  return result;
}

- (NSMutableArray*) simpleQuery: (NSString*)stmt
{
  SQLClient             *db;
  NSMutableArray        *result;

  db = [self provideClient];
  NS_DURING
    result = [db simpleQuery: stmt];
  NS_HANDLER
    [self swallowClient: db];
    [localException raise];
  NS_ENDHANDLER
  [self swallowClient: db];
  return result;
}

- (NSMutableArray*) simpleQuery: (NSString*)stmt
		     recordType: (id)rtype
		       listType: (id)ltype
{
  SQLClient             *db;
  NSMutableArray        *result;

  db = [self provideClient];
  NS_DURING
    result = [db simpleQuery: stmt
                  recordType: rtype
                    listType: ltype];
  NS_HANDLER
    [self swallowClient: db];
    [localException raise];
  NS_ENDHANDLER
  [self swallowClient: db];
  return result;
}

- (void) singletons: (NSMutableArray*)records
{
  [SQLClient singletons: records];
}

@end

