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

   $Date$ $Revision$
   */ 

#import	<Foundation/Foundation.h>
#import	<Performance/GSCache.h>
#import	"SQLClient.h"

@interface	Logger : NSObject
- (void) notified: (NSNotification*)n;
@end

@implementation	Logger
- (void) notified: (NSNotification*)n
{
  NSLog(@"Received %@", n);
}
@end

int
main()
{
  NSAutoreleasePool	*pool = [NSAutoreleasePool new];
  SQLClientPool         *sp;
  SQLClient		*db;
  NSUserDefaults	*defs;
  NSMutableArray	*records;
  SQLRecord		*record;
  unsigned char		dbuf[256];
  unsigned int		i;
  NSData		*data;
  NSString		*name;
  Logger		*l;

  defs = [NSUserDefaults standardUserDefaults];
  [defs registerDefaults:
    [NSDictionary dictionaryWithObjectsAndKeys:
      [NSDictionary dictionaryWithObjectsAndKeys:
	[NSDictionary dictionaryWithObjectsAndKeys:
	  @"template1@localhost", @"Database",
	  @"postgres", @"User",
	  @"postgres", @"Password",
	  @"Postgres", @"ServerType",
	  nil],
	@"test",
	nil],
      @"SQLClientReferences",
      nil]
    ];

  sp = [[[SQLClientPool alloc] initWithConfiguration: nil
                                                name: @"test"
                                                 max: 2
                                                 min: 1] autorelease];
#if 0
{
  NSAutoreleasePool     *p;
  SQLClient             *c0;
  SQLClient             *c1;

  [sp setDebugging: 2];
  p = [NSAutoreleasePool new];
  c0 = [sp provideClient];
  c1 = [sp provideClient];
  [sp provideClientBeforeDate: [NSDate dateWithTimeIntervalSinceNow: 15.0]];
  [p release];
  p = [NSAutoreleasePool new];
  c0 = [sp provideClient];
  c1 = [sp provideClient];
  [sp provideClientBeforeDate: [NSDate dateWithTimeIntervalSinceNow: 15.0]];
  [p release];
  [sp provideClientBeforeDate: [NSDate dateWithTimeIntervalSinceNow: 25.0]];
}
#endif
  db = [sp provideClient];
  [sp swallowClient: db];
  [sp queryString: @"SELECT CURRENT_TIMESTAMP", nil];
  db = [sp provideClient];

  l = [Logger new];
  [[NSNotificationCenter defaultCenter] addObserver: l
	selector: @selector(notified:)
	name: SQLClientDidConnectNotification
	object: db];
  [[NSNotificationCenter defaultCenter] addObserver: l
	selector: @selector(notified:)
	name: SQLClientDidDisconnectNotification
	object: db];

  if ((name = [defs stringForKey: @"Producer"]) != nil)
    {
      NS_DURING
	{
	  [db execute: @"CREATE TABLE Queue ( "
	    @"ID SERIAL, "
	    @"Consumer CHAR(40) NOT NULL, "
	    @"ServiceID INT NOT NULL, "
	    @"Status CHAR(1) DEFAULT 'Q' NOT NULL, "
	    @"Delivery TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL, "
	    @"Reference CHAR(128), "
	    @"Destination CHAR(15) NOT NULL, "
	    @"Payload CHAR(250) DEFAULT '' NOT NULL,"
	    @")",
	    nil];
	  [db execute:
	    @"CREATE UNIQUE INDEX QueueIDX ON Queue (ID)",
	    nil];
	  [db execute:
	    @"CREATE INDEX ServiceIDX ON Queue (ServiceID)",
	    nil];
	  [db execute:
	    @"CREATE INDEX ConsumerIDX ON Queue (Consumer,Status,Delivery)",
	    nil];
	  [db execute:
	    @"CREATE INDEX ReferenceIDX ON Queue (Reference,Consumer)",
	    nil];
	}
      NS_HANDLER
	{
	  NSLog(@"%@", localException);
	}
      NS_ENDHANDLER
      NSLog(@"Start producing");
      for (i = 0; i < 100000; i++)
	{
	  NSAutoreleasePool	*arp = [NSAutoreleasePool new];
	  NSString	*destination = [NSString stringWithFormat: @"%d", i];
	  NSString	*sid = [NSString stringWithFormat: @"%d", i%100];

          if (i % 1000 == 999)
            {
              [db postNotificationName: @"Producing"
                               payload: [NSString stringWithFormat: @"%d", i]];
            }
	  [db execute: @"INSERT INTO Queue (Consumer, Destination,"
            @" ServiceID, Payload) VALUES (",
	    [db quote: name], @", ",
            [db quote: destination], @", ",
            sid, @", ",
	    @"'helo there'",
            @")", nil];
	  [arp release];
	}
      NSLog(@"End producing");
    }
  else if ((name = [defs stringForKey: @"Consumer"]) != nil)
    {
      [db addObserver: l 
             selector: @selector(notified:)
                 name: @"Producing"];
      NSLog(@"Start consuming");
      for (i = 0; i < 100000;)
	{
	  NSAutoreleasePool	*arp = [NSAutoreleasePool new];
	  unsigned		count;
	  int			j;

	  [db begin];
	  records = [db query: @"SELECT * FROM Queue WHERE Consumer = ",
	    [db quote: name],
	    @" AND Status = 'Q' AND Delivery < CURRENT_TIMESTAMP",
	    @" ORDER BY Delivery LIMIT 1000 FOR UPDATE"  , nil];
	  count = [records count];
	  if (count == 0)
	    {
	      [db commit];
	      [NSThread sleepForTimeInterval: 1.0];
	      [db begin];
	      records = [db query: @"SELECT * FROM Queue WHERE Consumer = ",
		[db quote: name],
		@" AND Status = 'Q' AND Delivery < CURRENT_TIMESTAMP",
		@" ORDER BY Delivery LIMIT 50 FOR UPDATE"  , nil];
	      count = [records count];
	      if (count == 0)
		{
		  break;
		}
	    }

	  for (j = 0; j < count; j++)
	    {
	      SQLRecord	*record = [records objectAtIndex: j];
	      NSString	*reference = [record objectForKey: @"ID"];

	      [db execute: @"UPDATE Queue SET Status = 'S', Reference = ",
		[db quote: reference], @" WHERE ID = ",
		[record objectForKey: @"ID"], nil];

	      [db execute: @"UPDATE Queue SET Status = 'D'",
		@" WHERE Consumer = ", [db quote: name],
		@" AND Reference = ", [db quote: reference],
		nil];
	    }
	  [db commit];
	  i += count;
	  [arp release];
	}
      NSLog(@"End consuming (%d records)", i);
/*
      [db execute: @"DROP INDEX ReferenceIDX", nil];
      [db execute: @"DROP INDEX ServiceIDX", nil];
      [db execute: @"DROP INDEX ConsumerIDX", nil];
      [db execute: @"DROP INDEX QueueIDX", nil];
      [db execute: @"DROP TABLE Queue", nil];
*/
    }
  else
    {
      NSString	*oddChars;
      NSString	*nonLatin;
      id	e1, e2, e3, e4, e5;
      id	r0;
      id	r1;

      oddChars = @"'a\\b'c\r\nd'\\ed\\";
      nonLatin = [[NSString stringWithCString: "\"\\U2A11\""] propertyList];
      for (i = 0; i < 256; i++)
	{
	  dbuf[i] = i;
	}
      data = [NSData dataWithBytes: dbuf length: i];

      NS_DURING
      [db execute: @"drop table xxx", nil];
      NS_HANDLER
      NS_ENDHANDLER

      [db setDurationLogging: 0];

      [db begin];
      [db execute: @"create table xxx ( "
	@"id int, "
	@"k char(40), "
	@"char1 char(1), "
	@"boolval BOOL, "
	@"intval int, "
	@"when1 timestamp with time zone, "
	@"when2 timestamp, "
	@"b bytea,"
        @"extra1 int[],"
        @"extra2 varchar[],"
        @"extra3 bytea[],"
        @"extra4 boolean[],"
        @"extra5 timestamp with time zone[]"
	@")",
	nil];

      if (1 != [db execute: @"insert into xxx (id, k, char1, boolval, intval,"
        @" when1, when2, b, extra1, extra2, extra3, extra4, extra5) "
	@"values (1,"
	@"'{hello', "
	@"'X', "
	@"TRUE, "
	@"1, "
	@"CURRENT_TIMESTAMP, "
	@"CURRENT_TIMESTAMP, ",
	data, @", ",
        [db quoteArray:
          (e1 = [NSArray arrayWithObjects: @"1", @"2", [NSNull null], nil])
              toString: nil
        quotingStrings: NO], @", ",
        [db quoteArray:
          (e2 = [NSArray arrayWithObjects: @"on,e", @"t'wo", @"many", nil])
              toString: nil
        quotingStrings: YES], @", ",
        [db quoteArray:
          (e3 = [NSArray arrayWithObjects: data, nil])
              toString: nil
        quotingStrings: YES], @", ",
        [db quoteArray:
          (e4  =[NSArray arrayWithObjects: @"TRUE", @"FALSE", nil])
              toString: nil
        quotingStrings: NO], @", ",
        [db quoteArray:
          (e5 = [NSArray arrayWithObjects: [NSDate date], nil])
              toString: nil
        quotingStrings: YES], @")",
	nil])
        {
          NSLog(@"Insert failed to return row count");
        }

[db setDebugging: 9];
[db query: @"select * from xxx", nil];
[db setDebugging: 0];

      [db execute: @"insert into xxx "
	@"(id, k, char1, boolval, intval, when1, when2, b) "
	@"values (2,"
	@"'hello', "
	@"'X', "
	@"TRUE, "
	@"1, ",
	[NSDate date], @", ",
	[NSDate date], @", ",
	[NSData dataWithBytes: "" length: 0],
	@")",
	nil];
      [db execute: @"insert into xxx "
	@"(id, k, char1, boolval, intval, when1, when2, b) "
	@"values (3,",
	[db quote: oddChars],
	@", ",
	[db quote: nonLatin],
	@",TRUE, "
	@"1, ",
	[NSDate date], @", ",
	[NSDate date], @", ",
	[NSData dataWithBytes: "" length: 0],
	@")",
	nil];
      [db commit];

      r0 = [db cache: 1 query: @"select * from xxx order by id", nil];
      r1 = [db cache: 1 query: @"select * from xxx order by id", nil];
      NSCAssert([r0 lastObject] == [r1 lastObject], @"Cache failed");
      [NSThread sleepForTimeInterval: 2.0];
      records = [db cache: 1 query: @"select * from xxx order by id", nil];
      NSCAssert([r0 lastObject] != [records lastObject], @"Lifetime failed");

      [db addObserver: l 
             selector: @selector(notified:)
                 name: @"foo"];

      [db postNotificationName: @"foo" payload: @"hello"];

      [db execute: @"drop table xxx", nil];

      if ([records count] != 3)
	{
	  NSLog(@"Expected 3 records but got %lu", [records count]);
	}
      else
	{
	  record = [records objectAtIndex: 0];
	  if ([[record objectForKey: @"b"] isEqual: data] == NO)
	    {
	      NSLog(@"Retrieved data does not match saved data %@ %@",
		data, [record objectForKey: @"b"]);
	    }
	  record = [records objectAtIndex: 1];
	  if ([[record objectForKey: @"b"] isEqual: [NSData data]] == NO)
	    {
	      NSLog(@"Retrieved empty data does not match saved data");
	    }
	  record = [records objectAtIndex: 2];
	  if ([[record objectForKey: @"char1"] isEqual: nonLatin] == NO)
	    {
	      NSLog(@"Retrieved non-latin does not match saved string");
	    }
          id o = [[record objectForKey: @"k"] stringByTrimmingSpaces];
	  if ([o isEqual: oddChars] == NO)
	    {
	      NSLog(@"Retrieved odd chars (%@) does not match saved string (%@)", o, oddChars);
	    }
	  record = [records objectAtIndex: 0];
          o = [record objectForKey: @"extra1"];
	  if ([o isEqual: e1] == NO)
	    {
	      NSLog(@"Retrieved extra1 (%@) does not match saved (%@)", o, e1);
	    }
          o = [record objectForKey: @"extra2"];
	  if ([o isEqual: e2] == NO)
	    {
	      NSLog(@"Retrieved extra2 (%@) does not match saved (%@)", o, e2);
	    }
          o = [record objectForKey: @"extra3"];
	  if ([o isEqual: e3] == NO)
	    {
	      NSLog(@"Retrieved extra3 (%@) does not match saved (%@)", o, e3);
	    }
          o = [record objectForKey: @"extra4"];
	  if ([o count] != [e4 count])
	    {
	      NSLog(@"Retrieved extra4 (%@) does not match saved (%@)", o, e4);
	    }
	  for (int i = 0; i < [o count]; i++)
	    {
	      if ([[o objectAtIndex: i] boolValue]
	        != [[e4 objectAtIndex: i] boolValue])
		{
		  NSLog(@"Retrieved extra4 (%@) does not match saved (%@)",
		    o, e4);
		}
	    }
          o = [record objectForKey: @"extra5"];
	  if ([o count] != [e5 count])
	    {
	      NSLog(@"Retrieved extra5 (%@) does not match saved (%@)", o, e5);
	    }
	  for (int i = 0; i < [o count]; i++)
	    {
	      if (floor([[o objectAtIndex: i] timeIntervalSinceReferenceDate])
	        != floor([[e5 objectAtIndex: i] timeIntervalSinceReferenceDate]))
		{
		  NSLog(@"Retrieved extra5 (%@) does not match saved (%@)",
		    o, e5);
		}
	    }
	}

      NSLog(@"Records - %@", [GSCache class]);
    }

  NSLog(@"Pool stats:\n%@", [sp statistics]);

  [pool release];
  return 0;
}

