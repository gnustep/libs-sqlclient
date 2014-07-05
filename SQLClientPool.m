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
#import	<Foundation/NSException.h>
#import	<Foundation/NSLock.h>
#import	<Foundation/NSString.h>

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

@interface SQLClientPool (Private)
- (void) _lock;
- (void) _unlock;
@end

@implementation	SQLClientPool

- (void) dealloc
{
  SQLClient     **clients;
  BOOL          *used;
  int           count;
  int           i;

  [lock lock];
  count = max;
  max = 0;
  min = 0;
  clients = c;
  used = u;
  c = 0;
  u = 0;
  [lock unlock];
  DESTROY(lock);
  DESTROY(_config);
  DESTROY(_name);
  if (0 != clients)
    {
      for (i = 0; i < count; i++)
        {
          if (YES == used[i])
            {
              [clients[i] _clearPool: self];
            }
          else
            {
              [clients[i] release];
            }
        }
      free(clients);
      free(used);
    }
  [super dealloc];
}

- (id) initWithConfiguration: (NSDictionary*)config
			name: (NSString*)reference
                         max: (int)maxConnections
                         min: (int)minConnections
{
  if (nil != (self = [super init]))
    {
      ASSIGN(_config, config);
      ASSIGNCOPY(_name, reference);
      lock = [[NSConditionLock alloc] initWithCondition: 0];
      [self setMax: maxConnections min: minConnections];
    }
  return self;
}

- (int) maxConnections
{
  return max;
}

- (int) minConnections
{
  return min;
}

- (SQLClient*) provideClient
{
  return [self provideClientBeforeDate: nil];
}

- (SQLClient*) provideClientBeforeDate: (NSDate*)when
{
  int   connected = -1;
  int   found = -1;
  int   index;
  int   cond = 0;

  if (nil == when)
    {
      static NSDate     *future = nil;

      if (nil == future)
        {
          future = [[NSDate distantFuture] retain];
        }
      when = future;
    }

  /* We want to log stuff if we don't get a client quickly.
   * Ideally we get the lock straight away,
   * but if not we want to log every ten seconds (and possibly
   * when we begin waiting.
   */
  if (YES == [lock tryLockWhenCondition: 1])
    {
      _immediate++;
    }
  else
    {
      NSTimeInterval    start = [NSDate timeIntervalSinceReferenceDate];
      NSTimeInterval    end = [when timeIntervalSinceReferenceDate];
      NSTimeInterval    now = 0.0;
      NSTimeInterval    dif = 0.0;
      NSDate            *until;
      BOOL              locked;

      if (_debugging > 1)
        {
          NSLog(@"%@ has no clients available", self);
        }
      until = [[NSDate alloc] initWithTimeIntervalSinceNow: 10.0];
      locked = NO;
      while (NO == locked && now < end)
        {
          if ([when earlierDate: until] == until)
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
              if (_debugging > 0 || (_duration >= 0.0 && dif > _duration))
                {
                  NSLog(@"%@ still waiting after %g seconds", self, dif);
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
          if (_debugging > 0 || (_duration >= 0.0 && dif > _duration))
            {
              NSLog(@"%@ abandoned wait after %g seconds", self, dif);
            }
          _failed++;
          _failWaits += dif;
          return nil;
        }
      if (_debugging > 0 || (_duration >= 0.0 && dif > _duration))
        {
          NSLog(@"%@ provided client after %g seconds", self, dif);
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
  [lock unlockWithCondition: cond];
  return [c[found] autorelease];
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
  for (index = 0; index < max; index++)
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
  int   index;

  if (minConnections < 1) minConnections = 1;
  if (maxConnections > 100) maxConnections = 100;
  if (minConnections > maxConnections) minConnections = maxConnections;

  [self _lock];
  if (maxConnections != max)
    {
      GSCache   *cache = nil;

      if (max > 0)
        {
          while (max > maxConnections)
            {
              max--;
              if (YES == u[max])
                {
                  [c[max] _clearPool: self];
                }
              else
                {
                  [c[max] release];
                }
            }
          c = realloc(c, maxConnections * sizeof(SQLClient*));
          u = realloc(u, maxConnections * sizeof(BOOL));
        }
      else
        {
          c = calloc(maxConnections, sizeof(SQLClient*));
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
    }
  max = maxConnections;
  min = minConnections;
  [self _unlock];
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

- (BOOL) swallowClient: (SQLClient*)client
{
  BOOL  found = NO;
  int   index;

  [self _lock];
  for (index = 0; index < max && NO == found; index++)
    {
      if (YES == u[index] && client == c[index])
        {
          u[index] = NO;
          [c[index] retain];
          found = YES;
        }
    }
  [self _unlock];
  return found;
}

@end

@implementation SQLClientPool (Private)

- (void) _lock
{
  [lock lock];
}

- (void) _unlock
{
  int   idle = 0;
  int   used = 0;
  int   cond = 0;
  int   index;

  for (index = 0; index < max; index++)
    {
      /* Check to see if this client is free to be taken from the pool.
       * Also, if a client is connected but not in use, we call it idle.
       */
      if (YES == u[index])
        {
          /* This is a client which has been provided by the pool,
           * so it is in use by some code.
           */
          used++;
        }
      else
        {
          /* The client is not in use and can be provided by the pool,
           * so we must therefore re-lock with condition 1.
           */
          cond = 1;

          if (YES == [c[index] connected])
            {
              /* This unused client is still connected, so we count
               * it as an idle connection.
               */
              idle++;
            }
        }
    }

  /* If we have more idle client connections than we want,
   * disconnect the longest idle first.
   */
  while (idle > 0 && (used + idle) > min)
    {
      SQLClient *oldest = nil;

      for (index = 0; index < max; index++)
        {
          if (NO == u[index] && YES == [c[index] connected])
            {
              if (nil == oldest)
                {
                  oldest = c[index];
                }
              else
                {
                  oldest = [oldest longestIdle: c[index]];
                }
            }
        }
      NS_DURING
        [oldest disconnect];
      NS_HANDLER
        NSLog(@"Failed to disconnect %@ ... %@", oldest, localException);
      NS_ENDHANDLER
      idle--;
    }

  /* If we have fewer connections than we want, connect clients until we
   * are back up to the minimum.
   */
  for (index = 0; index < max && (used + idle) < min; index++)
    {
      if (NO == u[index] && NO == [c[index] connected])
        {
          NS_DURING
            [c[index] connect];
          NS_HANDLER
            NSLog(@"Failed to connect %@ ... %@", c[index], localException);
          NS_ENDHANDLER
          idle++;
        }
    }

  [lock unlockWithCondition: cond];
}

@end

