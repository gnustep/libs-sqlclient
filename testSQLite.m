/** 
   Copyright (C) 2005 Free Software Foundation, Inc.
   
   Written by:  Richard Frith-Macdonald <rfm@gnu.org>
   Date:	April 2005
   
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

#include	<Foundation/Foundation.h>
#include	"SQLClient.h"

int
main()
{
  CREATE_AUTORELEASE_POOL(pool);
  SQLClient		*db;
  NSUserDefaults	*defs;
  NSMutableArray	*records;
  SQLRecord		*record;
  unsigned char		dbuf[256];
  unsigned int		i;
  NSData		*data;

  defs = [NSUserDefaults standardUserDefaults];
  [defs registerDefaults:
    [NSDictionary dictionaryWithObjectsAndKeys:
      [NSDictionary dictionaryWithObjectsAndKeys:
	[NSDictionary dictionaryWithObjectsAndKeys:
	  @"test", @"Database",
	  @"", @"User",
	  @"", @"Password",
	  @"SQLite", @"ServerType",
	  nil],
	@"test",
	nil],
      @"SQLClientReferences",
      nil]
    ];

  for (i = 0; i < 256; i++)
    {
      dbuf[i] = i;
    }
  data = [NSData dataWithBytes: dbuf length: i];

  db = [SQLClient clientWithConfiguration: nil name: @"test"];
  [db setDurationLogging: 0];

  NS_DURING
  [db execute: @"drop table xxx", nil];
  NS_HANDLER
  NS_ENDHANDLER

  [db execute: @"create table xxx ( "
    @"k char(40), "
    @"char1 char(1), "
    @"intval int, "
    @"realval real, "
    @"b blob)",
    nil];

  [db execute: @"insert into xxx "
    @"(k, char1, intval, realval, b) "
    @"values ("
    @"'hello', "
    @"'X', "
    @"1, "
    @"9.99, ",
    data,
    @")",
    nil];

  [NSThread sleepUntilDate: [NSDate dateWithTimeIntervalSinceNow: 1]];

  [db execute: @"insert into xxx "
    @"(k, char1, intval, realval, b) "
    @"values ("
    @"'hello', "
    @"'X', "
    @"1, ",
    @"12345.6789, ",
    [NSData dataWithBytes: "" length: 0], @")",
    nil];

  records = [db query: @"select * from xxx", nil];
  [db execute: @"drop table xxx", nil];

  if ([records count] != 2)
    {
      NSLog(@"Expected 2 records but got %u", [records count]);
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
    }

  NSLog(@"Records - %@", records);

  RELEASE(pool);
  return 0;
}

