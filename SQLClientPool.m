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
#import	<Foundation/NSThread.h>
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

@interface      SQLTransaction (Creation)
+ (SQLTransaction*) _transactionUsing: (id)clientOrPool
                                batch: (BOOL)isBatched
                                 stop: (BOOL)stopOnFailure;
@end

@implementation	SQLClientPool

- (int) availableConnections
{
  int   available;
  int   index;

  [self _lock];
  available = index = _max;
  while (index-- > 0)
    {
      if (_items[index].u > 0)
        {
          available--;
        }
    }
  [self _unlock];
  return available;
}

- (SQLTransaction*) batch: (BOOL)stopOnFailure
{
  return [SQLTransaction _transactionUsing: self
                                     batch: YES
                                      stop: stopOnFailure];
}

- (GSCache*) cache
{
  return [_items[0].c cache];
}

- (void) dealloc
{
  SQLClientPoolItem     *old;
  int                   count;
  int                   i;

  [_lock lock];
  count = _max;
  old = _items;
  _max = 0;
  _min = 0;
  _items = 0;
  if (0 != old)
    {
      for (i = 0; i < count; i++)
        {
          [old[i].c _clearPool: self];
          [old[i].o release];
          if (0 == old[i].u)
            {
              [old[i].c release];
            }
        }
      free(old);
    }
  [_lock unlock];
  DESTROY(_lock);
  DESTROY(_config);
  DESTROY(_name);
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
      _lock = [[NSConditionLock alloc] initWithCondition: 0];
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
  [s appendString: [_items[0].c description]];
  return s;
}

- (int) maxConnections
{
  return _max;
}

- (int) minConnections
{
  return _min;
}

- (NSString*) name
{
  return  _name;
}

- (SQLClient*) provideClient
{
  return [self provideClientBeforeDate: nil exclusive: NO];
}

- (SQLClient*) provideClientBeforeDate: (NSDate*)when
{
  return [self provideClientBeforeDate: when exclusive: NO];
}

- (SQLClient*) provideClientBeforeDate: (NSDate*)when exclusive: (BOOL)isLocal
{
  NSThread              *thread = [NSThread currentThread];
  NSTimeInterval        start = [NSDate timeIntervalSinceReferenceDate];
  NSTimeInterval        now = start;
  SQLClient             *client = nil;
  int                   preferred = -1;
  int                   found = -1;
  int                   cond = 0;
  int                   index;

  /* If this is a request for a non-exclusive connection, we can simply
   * check to see if there's already such a connection available.
   */
  if (NO == isLocal)
    {
      [_lock lock];
      for (index = 0; index < _max; index++)
        {
          if (_items[index].o == thread && _items[index].u < NSNotFound
            && NO == [_items[index].c isInTransaction])
            {
              preferred = index;        // Ignore any other connected client
              break;
            }
          if (nil == _items[index].o && 0 == _items[index].u)
            {
              if (preferred < 0 && YES == [_items[index].c connected])
                {
                  preferred = index;
                }
              else
                {
                  found = index;
                }
            }
        }
      if (preferred >= 0)
        {
          found = preferred;    // Prefer a connected client.
        }
      if (found >= 0)
        {
          _items[found].t = now;
          if (0 == _items[found].u++)
            {
              ASSIGN(_items[found].o, thread);
              client = [_items[found].c autorelease];
            }
          else
            {
              /* We have already provided this client, so we must retain it
               * before we autorelease it, to keep retain counts  in sync.
               */
              client = [[_items[found].c retain] autorelease];
            }
          _immediate++;
        }
      [self _unlock];
      if (nil != client)
        {
          if (_debugging > 2)
            {
              NSLog(@"%@ provides %p%@",
                self, _items[found].c, [self _rc: client]);
            }
          return client;
        }
    }

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
  if (YES == [_lock tryLockWhenCondition: 1])
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
              locked = [_lock tryLockWhenCondition: 1];
            }
          else if ([when earlierDate: until] == until)
            { 
              locked = [_lock lockWhenCondition: 1 beforeDate: until];
            }
          else
            { 
              locked = [_lock lockWhenCondition: 1 beforeDate: when];
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

  for (index = 0; index < _max && 0 == cond; index++)
    {
      if (0 == _items[index].u)
        {
          if (preferred >= 0 || found >= 0)
            {
              /* There's at least one more client available to be
               * provided, so we want to re-lock with condition 1.
               */
              cond = 1;
            }
          if (preferred < 0 && YES == [_items[index].c connected])
            {
              preferred = index;
            }
          else
            {
              found = index;
            }
        }
      else if (NO == isLocal
        && _items[index].o == thread
        && _items[index].u < NSNotFound
        && NO == [_items[index].c isInTransaction])
        {
          /* We are allowed to re-use connections in the current thread,
           * so if we have found one, treat it as the preferred choice.
           */
          preferred = index;
        }
    }

  /* We prefer to use a client which is already connected, so we
   * avoid opening unnecessary connections.
   */
  if (preferred >= 0)
    {
      found = preferred;
    }
  if (YES == isLocal)
    {
      _items[found].u = NSNotFound;
    }
  else
    {
      _items[found].u++;
    }
  _items[found].t = now;
  ASSIGN(_items[found].o, thread);
  [self _unlock];
  client = [_items[found].c autorelease];
  if (_debugging > 2)
    {
      NSLog(@"%@ provides %p%@", self, _items[found].c, [self _rc: client]);
    }
  return client;
}

- (SQLClient*) provideClientExclusive
{
  return [self provideClientBeforeDate: nil exclusive: YES];
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
      for (index = 0; index < _max; index++)
        {
          if (YES == [_items[index].c connected])
            {
              /* This is a connected client.
               */
              connected++;
              if (0 == _items[index].u)
                {
                  /* Not in use; so a candidate to be purged
                   */
                  found = [_items[index].c longestIdle: found];
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
            || (connected > _min && age > _purgeMin))
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
      [_items[0].c setCache: nil];
      aCache = [_items[0].c cache];
    }
  for (index = 1; index < _max; index++)
    {
      [_items[index].c setCache: aCache];
    }
  [self _unlock];
}

- (void) setCacheThread: (NSThread*)aThread
{
  int   index;

  [self _lock];
  for (index = 0; index < _max; index++)
    {
      [_items[index].c setCacheThread: aThread];
    }
  [self _unlock];
}

- (void) setDebugging: (unsigned int)level
{
  int   index;

  [self _lock];
  _debugging = level;
  for (index = 0; index < _max; index++)
    {
      [_items[index].c setDebugging: _debugging];
    }
  [self _unlock];
}

- (void) setDurationLogging: (NSTimeInterval)threshold
{
  int   index;

  [self _lock];
  _duration = threshold;
  for (index = 0; index < _max; index++)
    {
      [_items[index].c setDurationLogging: _duration];
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
  old = _max;
  if (maxConnections != _max)
    {
      GSCache   *cache = nil;

      if (_max > 0)
        {
          while (_max > maxConnections)
            {
              _max--;
              [_items[_max].c _clearPool: self];
              if (0 == _items[_max].u)
                {
                  [_items[_max].c release];
                }
            }
          _items = realloc(_items, maxConnections * sizeof(SQLClientPoolItem));
        }
      else
        {
          _items = calloc(maxConnections, sizeof(SQLClientPoolItem));
        }
      for (index = _max; index < maxConnections; index++)
        {
          _items[index].o = nil;
          _items[index].t = 0.0;
          _items[index].u = 0;
          _items[index].c = [[SQLClient alloc] initWithConfiguration: _config
                                                                name: _name
                                                                pool: self];

          /* All the clients in the pool should share the same cache.
           */
          if (0 == index)
            {
              cache = [_items[index].c cache];
            }
          else
            {
              [_items[index].c setCache: cache];
            }
        }
      _max = maxConnections;
      [SQLClientPool _adjustPoolConnections: _max - old];
    }
  _min = minConnections;
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
  NSMutableString       *retainInfo = nil;
  unsigned int          cond = 0;
  unsigned int          free = 0;
  unsigned int          dead = 0;
  unsigned int          idle = 0;
  unsigned int          live = 0;
  unsigned int          used = 0;
  int                   index;
  NSMutableString       *s;

  [_lock lock];
  for (index = 0; index < _max; index++)
    {
      SQLClient         *client = _items[index].c;
      NSUInteger        rc = [client retainCount];

      /* Check to see if this client is free to be taken from the pool.
       * Also, if a client is connected but not in use, we call it idle.
       */
      if (_items[index].u > 0)
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
                @"  Client '%@' (retain count %"PRIuPTR
                @") active in transaction since %@\n",
                [client name], rc, d]];
              rc = 0;
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
                  if ([d timeIntervalSinceReferenceDate] < _items[index].t)
                    {
                      d = [NSDate dateWithTimeIntervalSinceReferenceDate:
                        _items[index].t];
                    }
                  if (nil == idleInfo)
                    {
                      idleInfo = [NSMutableArray array];
                    }
                  [idleInfo addObject: [NSString stringWithFormat:
                    @"  Client '%@' (retain count %"PRIuPTR
                    @") taken from pool but idle since %@\n",
                    [client name], rc, d]];
                  rc = 0;
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
          if (YES == [_items[index].c connected])
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
      if (rc > 1)
        {
          if (nil == retainInfo)
            {
              retainInfo = [NSMutableString stringWithCapacity: 100];
            }
          [retainInfo appendFormat:
            @"  Client '%@' (retain count %"PRIuPTR
            @") %s pool\n",
            [client name], rc,
            (_items[index].u > 0) ? "taken from" : "available in"];
        }
    }

  s = [NSMutableString stringWithFormat: @" size min: %u, max: %u\n"
    @"live:%u, used:%u, idle:%u, free:%u, dead:%u\n",
    _min, _max, live, used, idle, free, dead];

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
  if (retainInfo)
    {
      [s appendString: retainInfo];
    }
  [_lock unlockWithCondition: cond];
  return s;
}

- (BOOL) _swallowClient: (SQLClient*)client explicit: (BOOL)swallowed
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
  for (index = 0; index < _max && NO == found; index++)
    {
      if (_items[index].u > 0 && client == _items[index].c)
        {
          found = YES;
          if (YES == swallowed)
            {
              if (NSNotFound == _items[index].u || 1 == _items[index].u)
                {
                  /* This was only provided once, and has been explicitly
                   * swallowed by the pool again, so we should increment
                   * the reference count to prevent an implicit swallow
                   * caused by deallocation.
                   */
                  _items[index].u = 0;
                  NSIncrementExtraRefCount(client);
                }
              else
                {
                  _items[index].u--;
                }
            }
          else
            {
              /* Nothing is using this client connection any more (it had
               * -dealloc called), so we know the count must be zero.
               */
              _items[index].u = 0;
            }
          if (0 == _items[index].u)
            {
              DESTROY(_items[index].o);
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
  return [self _swallowClient: client explicit: YES];
}

- (void) setClientName: (NSString*)s
{
  unsigned int   index;

  [self _lock];
  for (index = 0; index < _max; index++)
    {
      SQLClient *client = _items[index].c;

      if (nil == s)
        {
          [client setClientName: s];
        }
      else
        {
          NSString      *n;

          n = [s stringByAppendingFormat: @" (%u)", index];
          [client setClientName: n];
        }
    }
  [self _unlock];
}

- (SQLTransaction*) transaction
{
  return [SQLTransaction _transactionUsing: self
                                     batch: NO
                                      stop: NO];
}

@end

@implementation SQLClientPool (Private)

- (void) _lock
{
  [_lock lock];
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

  for (index = 0; index < _max; index++)
    {
      /* Check to see if this client is free to be taken from the pool.
       */
      if (0 == _items[index].u)
        {
          [_lock unlockWithCondition: 1];
          return;
        }
    }
  [_lock unlockWithCondition: 0];
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
  sql = [[_items[0].c prepare: stmt args: ap] objectAtIndex: 0];
  va_end (ap);

  return sql;
}

- (NSString*) buildQuery: (NSString*)stmt with: (NSDictionary*)values
{
  NSString      *result = [_items[0].c buildQuery: stmt with: values];

  return result;
}

- (NSMutableArray*) cacheCheckSimpleQuery: (NSString*)stmt
{
  NSMutableArray        *result = [[_items[0].c cache] objectForKey: stmt];

  if (result != nil)
    {
      result = [[result mutableCopy] autorelease];
    }
  return result;
}

- (NSMutableArray*) cache: (int)seconds
		    query: (NSString*)stmt,...
{
  SQLClient             *db;
  NSMutableArray        *result;
  va_list	        ap;

  va_start (ap, stmt);
  stmt = [[_items[0].c prepare: stmt args: ap] objectAtIndex: 0];
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
  info = [_items[0].c prepare: stmt args: ap];
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

- (SQLClientPool*) pool
{
  return self;
}

- (NSMutableArray*) prepare: (NSString*)stmt args: (va_list)args
{
  return [_items[0].c prepare: stmt args: args];
}

- (NSMutableArray*) prepare: (NSString*)stmt with: (NSDictionary*)values
{
  return [_items[0].c prepare: stmt with: values];
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
  stmt = [[_items[0].c prepare: stmt args: ap] objectAtIndex: 0];
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
  stmt = [[_items[0].c prepare: stmt args: ap] objectAtIndex: 0];
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
  stmt = [[_items[0].c prepare: stmt args: ap] objectAtIndex: 0];
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
  NSString      *result = [_items[0].c quote: obj];

  return result;
}

- (NSMutableString*) quoteArray: (NSArray *)a
                       toString: (NSMutableString *)s
                 quotingStrings: (BOOL)_q
{
  NSMutableString       *result;

  result = [_items[0].c quoteArray: a toString:s quotingStrings: _q];

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
  quoted = [_items[0].c quoteString: str];
  [str release];
  return quoted;
}

- (NSString*) quoteBigInteger: (int64_t)i
{
  NSString      *result = [_items[0].c quoteBigInteger: i];

  return result;
}

- (NSString*) quoteCString: (const char *)s
{
  NSString      *result = [_items[0].c quoteCString: s];

  return result;
}

- (NSString*) quoteChar: (char)chr
{
  NSString      *result = [_items[0].c quoteChar: chr];

  return result;
}

- (NSString*) quoteFloat: (float)f
{
  NSString      *result = [_items[0].c quoteFloat: f];

  return result;
}

- (NSString*) quoteInteger: (int)i
{
  NSString      *result = [_items[0].c quoteInteger: i];

  return result;
}

- (NSString*) quoteString: (NSString *)s
{
  NSString      *result = [_items[0].c quoteString: s];

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

