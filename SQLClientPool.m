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
  NSAssert(minConnections > 0, NSInvalidArgumentException);
  NSAssert(maxConnections >= minConnections, NSInvalidArgumentException);
  NSAssert(maxConnections <= 100, NSInvalidArgumentException);

  if (nil != (self = [super init]))
    {
      GSCache   *cache = nil;
      int       i;

      max = maxConnections;
      min = minConnections;
      c = calloc(max, sizeof(SQLClient*));
      u = calloc(max, sizeof(BOOL));
      
      for (i = 0; i < max; i++)
        {
          c[i] = [[SQLClient alloc] initWithConfiguration: config
                                                     name: reference
                                                     pool: self];

          /* All the clients in the pool should share the same cache.
           */
          if (0 == i)
            {
              cache = [c[i] cache];
            }
          else
            {
              [c[i] setCache: cache];
            }
        }
      /* We start the condition lock with condition '1' to indicate
       * that there are clients ion the pool that we can provide.
       */
      lock = [[NSConditionLock alloc] initWithCondition: 1];
    }
  return self;
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

  /* We don't allow a nil cache for the pool (each client would creae its
   * own cache on demand). So we treat a nil cache as a request to create
   * a new cache with the default config.
   */
  if (nil == aCache)
    {
      [c[0] setCache: nil];
      aCache = [c[0] cache];
    }

  for (index = 0; index < max; index++)
    {
      [c[index] setCache: aCache];
    }
}

- (void) setCacheThread: (NSThread*)aThread
{
  int   index;

  for (index = 0; index < max; index++)
    {
      [c[index] setCacheThread: aThread];
    }
}

- (void) setDebugging: (unsigned int)level
{
  int   index;

  _debugging = level;
  for (index = 0; index < max; index++)
    {
      [c[index] setDebugging: _debugging];
    }
}

- (void) setDurationLogging: (NSTimeInterval)threshold
{
  int   index;

  _duration = threshold;
  for (index = 0; index < max; index++)
    {
      [c[index] setDurationLogging: _duration];
    }
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
  int   idle = 0;
  int   used = 0;
  int   cond = 0;
  int   index;

  [lock lock];
  for (index = 0; index < max; index++)
    {
      if (YES == u[index] && client == c[index])
        {
          u[index] = NO;
          [c[index] retain];
          found = YES;
        }

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
      [oldest disconnect];
      idle--;
    }
  [lock unlockWithCondition: cond];
  return found;
}

@end

