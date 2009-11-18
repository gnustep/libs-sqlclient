/** 
   Copyright (C) 2006 Free Software Foundation, Inc.
   
   Written by:  Richard Frith-Macdonald <rfm@gnu.org>
   Date:	August 2006
   
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

   $Date: 2006-05-25 12:34:03 +0100 (Thu, 25 May 2006) $ $Revision: 22982 $
   */ 

#import	<Foundation/Foundation.h>
#import	<Performance/GSCache.h>
#import	"SQLClient.h"

int
main()
{
  NSAutoreleasePool	*pool = [NSAutoreleasePool new];
  SQLClient		*db;
  NSUserDefaults	*defs;
  NSMutableArray	*records;
  SQLRecord		*record;
  unsigned char		dbuf[256];
  unsigned int		i;
  NSData		*data;
  NSString		*name;

  defs = [NSUserDefaults standardUserDefaults];
  [defs registerDefaults:
    [NSDictionary dictionaryWithObjectsAndKeys:
      [NSDictionary dictionaryWithObjectsAndKeys:
	[NSDictionary dictionaryWithObjectsAndKeys:
	  @"org.postgresql.Driver:jdbc:postgresql://localhost/template1",
	    @"Database",
	  @"postgres", @"User",
	  @"postgres", @"Password",
	  @"JDBC", @"ServerType",
	  nil],
	@"test",
	nil],
      @"SQLClientReferences",
      nil]
    ];

  db = [SQLClient clientWithConfiguration: nil name: @"test"];
  [db connect];

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
	    @"Payload CHAR(250) DEFAULT '' NOT NULL"
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

	  [db execute: @"INSERT INTO Queue (Consumer, Destination, ServiceID, Payload) VALUES (",
	    [db quote: name], @", ", [db quote: destination], @", ", sid, @", ",
	    @"'helo there'", @")", nil];
	  [arp release];
	}
      NSLog(@"End producing");
    }
  else if ((name = [defs stringForKey: @"Consumer"]) != nil)
    {
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
	      sleep(1);
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
      SQLTransaction	*t;
      NSString	*oddChars;
      NSString	*nonLatin;
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
	@"k char(40), "
	@"char1 char(1), "
	@"boolval BOOL, "
	@"intval int, "
	@"when1 timestamp with time zone, "
	@"when2 timestamp, "
	@"b bytea"
	@")",
	nil];

      [db execute: @"insert into xxx "
	@"(k, char1, boolval, intval, when1, when2, b) "
	@"values ("
	@"'hello', "
	@"'X', "
	@"TRUE, "
	@"1, "
	@"CURRENT_TIMESTAMP, "
	@"CURRENT_TIMESTAMP, ",
	data,
	@")",
	nil];
      [db execute: @"insert into xxx "
	@"(k, char1, boolval, intval, when1, when2, b) "
	@"values ("
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
	@"(k, char1, boolval, intval, when1, when2, b) "
	@"values (",
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

      r0 = [db cache: 1 query: @"select * from xxx", nil];
      r1 = [db cache: 1 query: @"select * from xxx", nil];
      NSCAssert([r0 lastObject] == [r1 lastObject], @"Cache failed");
      sleep(2);
      records = [db cache: 1 query: @"select * from xxx", nil];
      NSCAssert([r0 lastObject] != [records lastObject], @"Lifetime failed");

      [db execute: @"drop table xxx", nil];

      if ([records count] != 3)
	{
	  NSLog(@"Expected 3 records but got %u", [records count]);
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
	  if ([[record objectForKey: @"k"] isEqual: oddChars] == NO)
	    {
	      NSLog(@"Retrieved odd chars does not match saved string");
	    }
	}

      [db execute: @"create table xxx ( "
	@"k char(40), "
	@"char1 char(1), "
	@"boolval BOOL, "
	@"intval int, "
	@"when1 timestamp with time zone, "
	@"when2 timestamp, "
	@"b bytea"
	@")",
	nil];
      t = [db transaction];
      [t add: @"insert into xxx "
	@"(k, char1, boolval, intval, when1, when2, b) "
	@"values (",
	[db quote: oddChars],
	@", ",
	[db quote: nonLatin],
	@",TRUE, "
	@"0, ",
	[NSDate date], @", ",
	[NSDate date], @", ",
	[db quote: nil],
	@")",
	nil];
      [t add: @"insert into xxx "
	@"(k, char1, boolval, intval, when1, when2, b) "
	@"values (",
	[db quote: oddChars],
	@", ",
	[db quote: nonLatin],
	@",TRUE, "
	@"1, ",
	[NSDate date], @", ",
	[NSDate date], @", ",
	[db quote: nil],
	@")",
	nil];
      [t add: @"insert into xxx "
	@"(k, char1, boolval, intval, when1, when2, b) "
	@"values (",
	[db quote: oddChars],
	@", ",
	[db quote: nonLatin],
	@",TRUE, "
	@"2, ",
	[NSDate date], @", ",
	[NSDate date], @", ",
	[db quote: nil],
	@")",
	nil];
      [t execute];
      [db execute: @"drop table xxx", nil];

      NSLog(@"Records - %@", [GSCache class]);
    }

  [pool release];
  return 0;
}

