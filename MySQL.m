/* -*-objc-*- */

/** Implementation of SQLClientMySQL for GNUStep
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
#include	<Foundation/NSString.h>
#include	<Foundation/NSData.h>
#include	<Foundation/NSDate.h>
#include	<Foundation/NSCalendarDate.h>
#include	<Foundation/NSException.h>
#include	<Foundation/NSProcessInfo.h>
#include	<Foundation/NSNotification.h>
#include	<Foundation/NSUserDefaults.h>
#include	<Foundation/NSMapTable.h>
#include	<Foundation/NSLock.h>
#include	<Foundation/NSNull.h>
#include	<Foundation/NSValue.h>
#include	<Foundation/NSAutoreleasePool.h>

#include	"config.h"
#include	"SQLClient.h"

#include	<mysql/mysql.h>


@interface SQLClientMySQL : SQLClient
@end

@implementation	SQLClientMySQL

#define	connection	((MYSQL*)(self->extra))

static NSDate	*future = nil;
static NSNull	*null = nil;

+ (void) initialize
{
  if (future == nil)
    {
      future = [NSCalendarDate dateWithString: @"9999-01-01 00:00:00 +0000"
			       calendarFormat: @"%Y-%m-%d %H:%M:%S %z"
				       locale: nil];
      RETAIN(future);
      null = [NSNull null];
      RETAIN(null);
    }
}

- (BOOL) backendConnect
{
  if (connected == NO)
    {
      if ([self database] != nil
	&& [self user] != nil
	&& [self password] != nil)
	{
	  NSString		*host = nil;
	  NSString		*port = nil;
	  NSString		*dbase = [self database];
	  NSRange		r;

	  [[self class] purgeConnections: nil];

	  r = [dbase rangeOfString: @"@"];
	  if (r.length > 0)
	    {
	      host = [dbase substringFromIndex: NSMaxRange(r)];
	      dbase = [dbase substringToIndex: r.location];
	      r = [host rangeOfString: @":"];
	      if (r.length > 0)
		{
		  port = [host substringFromIndex: NSMaxRange(r)];
		  host = [host substringToIndex: r.location];
		}
	    }

	  if ([self debugging] > 0)
	    {
	      [self debug: @"Connect to '%@' as %@", [self database], [self name]];
	    }
	  extra = mysql_init(0);
	  if (mysql_real_connect(connection,
	    [host UTF8String],
	    [[self user] UTF8String],
	    [[self password] UTF8String],
	    [dbase UTF8String],
	    [port intValue],
	    NULL,
	    0
	    ) == 0)
	    {
	      [self debug: @"Error connecting to '%@' (%@) - %s",
		[self name], [self database], mysql_error(connection)];
	      mysql_close(connection);
	      extra = 0;
	    }
#if 0
	  else if (mysql_query(connection, "SET CHARACTER SET utf8") != 0)
	    {
	      [self debug: @"Error setting utf8 support for '%@' (%@) - %s",
		[self name], [self database], mysql_error(connection)];
	      mysql_close(connection);
	      extra = 0;
	    }
#endif
	  else
	    {
	      connected = YES;

	      if ([self debugging] > 0)
		{
		  [self debug: @"Connected to '%@'", [self name]];
		}
	    }
	}
      else
	{
	  [self debug:
	    @"Connect to '%@' with no user/password/database configured",
	    [self name]];
	}
    }
  return connected;
}

- (void) backendDisconnect
{
  if (connected == YES)
    {
      NS_DURING
	{
	  if ([self isInTransaction] == YES)
	    {
	      [self rollback];
	    }

	  if ([self debugging] > 0)
	    {
	      [self debug: @"Disconnecting client %@", [self clientName]];
	    }
          mysql_close(connection);
          extra = 0;
	  if ([self debugging] > 0)
	    {
	      [self debug: @"Disconnected client %@", [self clientName]];
	    }
	}
      NS_HANDLER
	{
	  [self debug: @"Error disconnecting from database (%@): %@",
	    [self clientName], localException];
	}
      NS_ENDHANDLER
      connected = NO;
    }
}

- (void) backendExecute: (NSArray*)info
{
  NSString	*stmt;
  CREATE_AUTORELEASE_POOL(arp);

  stmt = [info objectAtIndex: 0];
  if ([stmt length] == 0)
    {
      RELEASE (arp);
      [NSException raise: NSInternalInconsistencyException
		  format: @"Statement produced null string"];
    }

  NS_DURING
    {
      const char	*statement;
      unsigned		length;

      /*
       * Ensure we have a working connection.
       */
      if ([self backendConnect] == NO)
	{
	  [NSException raise: SQLException
	    format: @"Unable to connect to '%@' to execute statement %@",
	    [self name], stmt];
	} 

      statement = (char*)[stmt UTF8String];
      length = strlen(statement);
      statement = [self insertBLOBs: info
	              intoStatement: statement
			     length: length
			 withMarker: "'''"
			     length: 3
			     giving: &length];

      if (mysql_real_query(connection, statement, length) != 0)
	{
	  [NSException raise: SQLException format: @"%s",
	    mysql_error(connection)];
	}
    }
  NS_HANDLER
    {
      NSString	*n = [localException name];

      if ([n isEqual: SQLConnectionException] == YES) 
	{
	  [self backendDisconnect];
	}
      if ([self debugging] > 0)
	{
	  [self debug: @"Error executing statement:\n%@\n%@",
	    stmt, localException];
	}
      RETAIN (localException);
      RELEASE (arp);
      AUTORELEASE (localException);
      [localException raise];
    }
  NS_ENDHANDLER
  DESTROY(arp);
}

static unsigned int trim(char *str)
{
  char	*start = str;

  while (isspace(*str))
    {
      str++;
    }
  if (str != start)
    {
      strcpy(start, str);
    }
  str = start;
  while (*str != '\0')
    {
      str++;
    }
  while (str > start && isspace(str[-1]))
    {
      *--str = '\0';
    }
  return (str - start);
}

- (NSMutableArray*) backendQuery: (NSString*)stmt
{
  CREATE_AUTORELEASE_POOL(arp);
  NSMutableArray	*records = nil;
  MYSQL_RES		*result = 0;

  if ([stmt length] == 0)
    {
      RELEASE (arp);
      [NSException raise: NSInternalInconsistencyException
		  format: @"Statement produced null string"];
    }

  NS_DURING
    {
      char	*statement;

      /*
       * Ensure we have a working connection.
       */
      if ([self backendConnect] == NO)
	{
	  [NSException raise: SQLException
	    format: @"Unable to connect to '%@' to run query %@",
	    [self name], stmt];
	} 

      statement = (char*)[stmt UTF8String];
      if (mysql_query(connection, statement) == 0
	&& (result = mysql_store_result(connection)) != 0)
	{
	  int	recordCount = mysql_num_rows(result);
	  int	fieldCount = mysql_num_fields(result);
	  MYSQL_FIELD	*fields = mysql_fetch_fields(result);
	  NSString	*keys[fieldCount];
	  int	i;

	  for (i = 0; i < fieldCount; i++)
	    {
	      keys[i] = [NSString stringWithUTF8String: fields[i].name];
	    }

	  records = [[NSMutableArray alloc] initWithCapacity: recordCount];
	  for (i = 0; i < recordCount; i++)
	    {
	      SQLRecord	*record;
	      MYSQL_ROW	row = mysql_fetch_row(result);
	      unsigned long *lengths = mysql_fetch_lengths(result);
	      id	values[fieldCount];
	      int	j;

	      for (j = 0; j < fieldCount; j++)
		{
		  id		v = null;
		  unsigned char	*p = row[j];

		  if (p != 0)
		    {
		      int	size = lengths[j];

NSLog(@"%@ type:%d size: %d val:%*.*s\n", keys[j], fields[j].type, size, size, size, p);
		      switch (fields[j].type)
			{
			  case FIELD_TYPE_TIMESTAMP:
			    {
			      NSString	*s;

			      s = [[NSString alloc] initWithBytes: p
				length: size encoding: NSASCIIStringEncoding];
			      if (size > 14)
				{
				  v = [NSCalendarDate dateWithString: s
				  calendarFormat: @"%Y-%m-%d %H:%M:%S"
				      locale: nil];
				}
			      else if (size == 14)
				{
				  v = [NSCalendarDate dateWithString: s
				  calendarFormat: @"%Y%m%d%H%M%S"
				      locale: nil];
				}
			      else if (size == 12)
				{
				  v = [NSCalendarDate dateWithString: s
				  calendarFormat: @"%y%m%d%H%M%S"
				      locale: nil];
				}
			      else if (size == 10)
				{
				  v = [NSCalendarDate dateWithString: s
				  calendarFormat: @"%y%m%d%H%M"
				      locale: nil];
				}
			      else if (size == 8)
				{
				  v = [NSCalendarDate dateWithString: s
				  calendarFormat: @"%y%m%d%H"
				      locale: nil];
				}
			      else if (size == 6)
				{
				  v = [NSCalendarDate dateWithString: s
				  calendarFormat: @"%y%m%d"
				      locale: nil];
				}
			      else if (size == 4)
				{
				  v = [NSCalendarDate dateWithString: s
				  calendarFormat: @"%y%m"
				      locale: nil];
				}
			      else 
				{
				  v = [NSCalendarDate dateWithString: s
				  calendarFormat: @"%y"
				      locale: nil];
				}
			      RELEASE(s);
			      [v setCalendarFormat: @"%Y-%m-%d %H:%M:%S %z"];
			    }
			    break;

			  case FIELD_TYPE_TINY:
			    v = [NSString stringWithFormat: @"%u", *p];
			    break;

			  case FIELD_TYPE_BLOB:
			  case FIELD_TYPE_TINY_BLOB:
			  case FIELD_TYPE_MEDIUM_BLOB:
			  case FIELD_TYPE_LONG_BLOB:
			    v = [NSData dataWithBytes: p length: size];
			    break;

			  default:
			    trim(p);
			    v = [NSString stringWithUTF8String: p];
			    break;
			}
		    }
		  values[j] = v;
		}
	      record = [SQLRecord newWithValues: values
					   keys: keys
					  count: fieldCount];
	      [records addObject: record];
	      RELEASE(record);
	    }
	}
      else
	{
	  [NSException raise: SQLException format: @"%s",
	    mysql_error(connection)];
	}
    }
  NS_HANDLER
    {
      NSString	*n = [localException name];

      if ([n isEqual: SQLConnectionException] == YES) 
	{
	  [self backendDisconnect];
	}
      if ([self debugging] > 0)
	{
	  [self debug: @"Error executing statement:\n%@\n%@",
	    stmt, localException];
	}
      if (result != 0)
	{
	  mysql_free_result(result);
	}
      DESTROY(records);
      RETAIN (localException);
      RELEASE (arp);
      AUTORELEASE (localException);
      [localException raise];
    }
  NS_ENDHANDLER
  DESTROY(arp);
  if (result != 0)
    {
      mysql_free_result(result);
    }
  return AUTORELEASE(records);
}

- (unsigned) copyEscapedBLOB: (NSData*)blob into: (void*)buf
{
  const unsigned char	*bytes = [blob bytes];
  unsigned char		*ptr = buf;
  unsigned		l = [blob length];
  unsigned		i;

  *ptr++ = '\'';
  for (i = 0; i < l; i++)
    {
      unsigned char	c = bytes[i];

      if (c == '\0')
	{
	  *ptr++ = '\\';
	  *ptr++ = '0';
	}
      else if (c == '\\' || c == '\'' || c == '"')
	{
	  *ptr++ = '\\';
	  *ptr++ = c;
	}
      else
	{
	  *ptr++ = c;
	}
    }
  *ptr++ = '\'';
  return ((void*)ptr - buf);
}

- (unsigned) lengthOfEscapedBLOB: (NSData*)blob
{
  const unsigned char	*bytes = [blob bytes];
  unsigned		l = [blob length];
  unsigned		length = 2;		// Quotes around BLOB

  while (l-- > 0)
    {
      unsigned char	c = bytes[l];

      if (c == '\0' || c == '\\' || c == '\'' || c == '"')
	{
	  length++;
	}
      length++;
    }
  return length;
}

@end

