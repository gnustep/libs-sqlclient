/* -*-objc-*- */

/** Implementation of SQLClientMySQL for GNUStep
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
#import	<Foundation/NSString.h>
#import	<Foundation/NSData.h>
#import	<Foundation/NSDate.h>
#import	<Foundation/NSCalendarDate.h>
#import	<Foundation/NSException.h>
#import	<Foundation/NSProcessInfo.h>
#import	<Foundation/NSNotification.h>
#import	<Foundation/NSUserDefaults.h>
#import	<Foundation/NSMapTable.h>
#import	<Foundation/NSLock.h>
#import	<Foundation/NSNull.h>
#import	<Foundation/NSValue.h>
#import	<Foundation/NSAutoreleasePool.h>

#include	"config.h"

#define SQLCLIENT_PRIVATE       @public

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
      future = [NSCalendarDate dateWithString: @"9999-01-01 00:00:00"
			       calendarFormat: @"%Y-%m-%d %H:%M:%S"
				       locale: nil];
      [future retain];
      null = [NSNull null];
      [null retain];
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
	      [self debug: @"Connect to '%@' as %@",
		[self database], [self name]];
	    }
	  extra = mysql_init(0);
	  mysql_options(connection, MYSQL_SET_CHARSET_NAME, "utf8");
	  if (mysql_real_connect(connection,
	    [host UTF8String],
	    [[self user] UTF8String],
	    [[self password] UTF8String],
	    [dbase UTF8String],
	    [port intValue],
	    NULL,
	    CLIENT_MULTI_STATEMENTS) == 0)
	    {
	      [self debug: @"Error connecting to '%@' (%@) - %s",
		[self name], [self database], mysql_error(connection)];
	      mysql_close(connection);
	      extra = 0;
	    }
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
	  extra = 0;
	  [self debug: @"Error disconnecting from database (%@): %@",
	    [self clientName], localException];
	}
      NS_ENDHANDLER
      connected = NO;
    }
}

- (void) backendExecute: (NSArray*)info
{
  NSString	        *stmt;
  NSAutoreleasePool     *arp = [NSAutoreleasePool new];

  stmt = [info objectAtIndex: 0];
  if ([stmt length] == 0)
    {
      [arp release];
      [NSException raise: NSInternalInconsistencyException
		  format: @"Statement produced null string"];
    }

  NS_DURING
    {
      MYSQL_RES		*result;
      const char	*statement;
      unsigned		length;

      /*
       * Ensure we have a working connection.
       */
      if ([self connect] == NO)
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
			 withMarker: "'?'''?'"
			     length: 7
			     giving: &length];

      if (mysql_real_query(connection, statement, length) != 0)
	{
	  [NSException raise: SQLException format: @"%s",
	    mysql_error(connection)];
	}
      /* discard any results.
       */
      result = mysql_store_result(connection);
      if (result != 0) mysql_free_result(result);
      while (mysql_more_results(connection))
	{
          if (mysql_next_result(connection) == 0)
	    {
              result = mysql_store_result(connection);
	      if (result != 0) mysql_free_result(result);
	    }
	}
    }
  NS_HANDLER
    {
      NSString	*n = [localException name];

      if ([n isEqual: SQLConnectionException] == YES) 
	{
	  [self disconnect];
	}
      if ([self debugging] > 0)
	{
	  [self debug: @"Error executing statement:\n%@\n%@",
	    stmt, localException];
	}
      [localException retain];
      [arp release];
      [localException autorelease];
      [localException raise];
    }
  NS_ENDHANDLER
  [arp release];
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
		      recordType: (Class)rtype
		        listType: (Class)ltype
{
  NSAutoreleasePool     *arp = [NSAutoreleasePool new];
  NSMutableArray	*records = nil;
  MYSQL_RES		*result = 0;

  if ([stmt length] == 0)
    {
      [arp release];
      [NSException raise: NSInternalInconsistencyException
		  format: @"Statement produced null string"];
    }

  NS_DURING
    {
      char	*statement;

      /*
       * Ensure we have a working connection.
       */
      if ([self connect] == NO)
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
	      keys[i] = [NSString stringWithUTF8String: (char*)fields[i].name];
	    }

	  records = [[ltype alloc] initWithCapacity: recordCount];
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
		  unsigned char	*p = (unsigned char*)row[j];

		  if (p != 0)
		    {
		      int	size = lengths[j];

		      if ([self debugging] > 1)
			{
			  [self debug: @"%@ type:%d size: %d val:%*.*s\n",
			    keys[j], fields[j].type, size, size, size, p];
			}

		      switch (fields[j].type)
			{
			  case FIELD_TYPE_TIMESTAMP:
			    {
			      char	b[32];
			      NSString	*f;
			      NSString	*s;

			      if (size > 14)
				{
				  size = 19;
				  f = @"%Y-%m-%d %H:%M:%S %z";
				}
			      else if (size == 14)
				{
				  f = @"%Y%m%d%H%M%S %z";
				}
			      else if (size == 12)
				{
				  f = @"%y%m%d%H%M%S %z";
				}
			      else if (size == 10)
				{
				  f = @"%y%m%d%H%M %z";
				}
			      else if (size == 8)
				{
				  f = @"%y%m%d%H %z";
				}
			      else if (size == 6)
				{
				  f = @"%y%m%d %z";
				}
			      else if (size == 4)
				{
				  f = @"%y%m %z";
				}
			      else 
				{
				  f = @"%y %z";
				}
			      strncpy(b, (char*)p, size);
			      strncpy(b + size, (char*)" +0000", 6);
			      s = [[NSString alloc] initWithBytes: b
				length: size + 6
				encoding: NSASCIIStringEncoding];
			      v = [NSCalendarDate dateWithString: s
				calendarFormat: f
				locale: nil];
			      [v setCalendarFormat: @"%Y-%m-%d %H:%M:%S %z"];
			      if ([self debugging] > 1)
				[self debug: @"Parsed '%@' as '%@'\n", s, v];
			      [s release];
			    }
			    break;

			  case FIELD_TYPE_TINY:
			    v = [NSString stringWithFormat: @"%u", *p];
			    break;

			  case FIELD_TYPE_BLOB:
			  case FIELD_TYPE_TINY_BLOB:
			  case FIELD_TYPE_MEDIUM_BLOB:
			  case FIELD_TYPE_LONG_BLOB:
		            if (63 == fields[j].charsetnr)
			      {
			        v = [NSData dataWithBytes: p length: size];
			      }
			    else
			      {
			        v = [[[NSString alloc] initWithBytes: p
				  length: size
				  encoding: NSUTF8StringEncoding] autorelease];
			      }
			    break;

			  default:
			    trim((char*)p);
			    v = [NSString stringWithUTF8String: (char*)p];
			    break;
			}
		    }
		  values[j] = v;
		}
	      record = [rtype newWithValues: values
				       keys: keys
				      count: fieldCount];
	      [records addObject: record];
	      [record release];
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
	  [self disconnect];
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
      [records release];
      records = nil;
      [localException retain];
      [arp release];
      [localException autorelease];
      [localException raise];
    }
  NS_ENDHANDLER
  [arp release];
  if (result != 0)
    {
      mysql_free_result(result);
    }
  return [records autorelease];
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

- (NSString*) quote: (id)obj
{
  /* MySQL doesn't support timezones ... convert dates to simple GMT.
   */
  if ([obj isKindOfClass: [NSDate class]] == YES)
    {
      NSString		*fmt = nil;
      static NSTimeZone	*gmt = nil;

      if (nil == gmt)
	{
	  gmt = [[NSTimeZone timeZoneForSecondsFromGMT: 0] retain];
	}
      if ([obj isKindOfClass: [NSCalendarDate class]] == YES)
	{
	  fmt = [obj calendarFormat];
	  if ([fmt length] > 17)
	    {
	      fmt = nil;	// bad format ... had timezone
	    }
	}
      if (nil == fmt)
	{
	  fmt = @"%Y-%m-%d %H:%M:%S";
	}
      fmt = [NSString stringWithFormat: @"'%@'", fmt];
      return [obj descriptionWithCalendarFormat: fmt
				       timeZone: gmt
				         locale: nil];
    }
  return [super quote: obj];
}

@end

