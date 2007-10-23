/* -*-objc-*- */

/** Implementation of SQLClientPostgres for GNUStep
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
#import	<Foundation/NSCharacterSet.h>
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
#include	"SQLClient.h"

#include	<libpq-fe.h>

@interface SQLClientPostgres : SQLClient
@end

@interface	SQLClientPostgres(Embedded)
- (NSData*) dataFromBLOB: (const char *)blob;
- (NSDate*) dbToDateFromBuffer: (char*)b length: (int)l;
@end

typedef struct	{
  PGconn	*_connection;
  BOOL		_escapeStrings;         /* Can we use E'...' syntax?    */
} ConnectionInfo;

#define	cInfo			((ConnectionInfo*)(self->extra))
#define	connection		(cInfo->_connection)
#define	escapeStrings	        (cInfo->_escapeStrings)

static NSDate	*future = nil;
static NSNull	*null = nil;

@implementation	SQLClientPostgres

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

static NSString*
connectQuote(NSString *str)
{
  NSMutableString	*m;

  m = [str mutableCopy];
  [m replaceString: @"\\" withString: @"\\\\"];
  [m replaceString: @"'" withString: @"\\'"];
  [m replaceCharactersInRange: NSMakeRange(0, 0) withString: @"'"];
  [m appendString: @"'"];
  return AUTORELEASE(m);
}

- (BOOL) backendConnect
{
  if (extra == 0)
    {
      extra = NSZoneMalloc(NSDefaultMallocZone(), sizeof(ConnectionInfo));
      memset(extra, '\0', sizeof(ConnectionInfo));
    }
  if (connection == 0)
    {
      connected = NO;
      if ([self database] != nil)
	{
	  NSString		*host = nil;
	  NSString		*port = nil;
	  NSString		*dbase = [self database];
	  NSString		*str;
	  NSRange		r;
	  NSMutableString	*m;

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

	  m = [NSMutableString stringWithCapacity: 156];
	  [m appendString: @"dbname="];
	  [m appendString: connectQuote(dbase)];
	  str = connectQuote(host);
	  if (str != nil)
	    {
	      unichar	c = [str characterAtIndex: 1];

	      if (c >= '0' && c <= '9')
		{
		  [m appendString: @" hostaddr="];	// Numeric IP
		}
	      else
		{
		  [m appendString: @" host="];		// Domain name
		}
	      [m appendString: str];
	    }
	  str = connectQuote(port);
	  if (str != nil)
	    {
	      [m appendString: @" port="];
	      [m appendString: str];
	    }
	  str = connectQuote([self user]);
	  if (str != nil)
	    {
	      [m appendString: @" user="];
	      [m appendString: str];
	    }
	  str = connectQuote([self password]);
	  if (str != nil)
	    {
	      [m appendString: @" password="];
	      [m appendString: str];
	    }

	  if ([self debugging] > 0)
	    {
	      [self debug: @"Connect to '%@' as %@", m, [self name]];
	    }
	  connection = PQconnectdb([m UTF8String]);
	  if (PQstatus(connection) != CONNECTION_OK)
	    {
	      [self debug: @"Error connecting to '%@' (%@) - %s",
		[self name], m, PQerrorMessage(connection)];
	      PQfinish(connection);
	      connection = 0;
	    }
	  else if (PQsetClientEncoding(connection, "UTF-8") < 0)
	    {
	      [self debug: @"Error setting UTF-8 with '%@' (%@) - %s",
		[self name], m, PQerrorMessage(connection)];
	      PQfinish(connection);
	      connection = 0;
	    }
	  else
	    {
	      const char	*p;

	      p = PQparameterStatus(connection, "standard_conforming_strings");
              if (p != 0)
                {
                  escapeStrings = YES;
                }
              else
                {
                  escapeStrings = NO;
                }

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
  if (extra != 0 && connection != 0)
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
	  PQfinish(connection);
	  connection = 0;
	  if ([self debugging] > 0)
	    {
	      [self debug: @"Disconnected client %@", [self clientName]];
	    }
	}
      NS_HANDLER
	{
	  connection = 0;
	  [self debug: @"Error disconnecting from database (%@): %@",
	    [self clientName], localException];
	}
      NS_ENDHANDLER
      connected = NO;
    }
}

- (void) backendExecute: (NSArray*)info
{
  CREATE_AUTORELEASE_POOL(arp);
  PGresult	*result = 0;
  NSString	*stmt = [info objectAtIndex: 0];

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
			 withMarker: "'?'''?'"
			     length: 7
			     giving: &length];

      result = PQexec(connection, statement);
      if (result == 0 || PQresultStatus(result) == PGRES_FATAL_ERROR)
	{
	  NSString	*str;
	  const char	*cstr;

	  if (result == 0)
	    {
	      cstr = PQerrorMessage(connection);
	    }
	  else
	    {
	      cstr = PQresultErrorMessage(result);
	    }
	  str = [NSString stringWithUTF8String: cstr];
	  [self backendDisconnect];
	  [NSException raise: SQLException format: @"%@ %@", str, stmt];
	}
      if (PQresultStatus(result) != PGRES_COMMAND_OK)
	{
	  [NSException raise: SQLException format: @"%s",
	    PQresultErrorMessage(result)];
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
	  PQclear(result);
	}
      RETAIN (localException);
      RELEASE (arp);
      AUTORELEASE (localException);
      [localException raise];
    }
  NS_ENDHANDLER
  if (result != 0)
    {
      PQclear(result);
    }
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

- (NSMutableArray*) backendQuery: (NSString*)stmt recordClass: (Class)rClass
{
  CREATE_AUTORELEASE_POOL(arp);
  PGresult		*result = 0;
  NSMutableArray	*records = nil;

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
      result = PQexec(connection, statement);
      if (result == 0 || PQresultStatus(result) == PGRES_FATAL_ERROR)
	{
	  NSString	*str;
	  const char	*cstr;

	  if (result == 0)
	    {
	      cstr = PQerrorMessage(connection);
	    }
	  else
	    {
	      cstr = PQresultErrorMessage(result);
	    }
	  str = [NSString stringWithUTF8String: cstr];
	  [self backendDisconnect];
	  [NSException raise: SQLException format: @"%@", str];
	}
      if (PQresultStatus(result) == PGRES_TUPLES_OK)
	{
	  int		recordCount = PQntuples(result);
	  int		fieldCount = PQnfields(result);
	  NSString	*keys[fieldCount];
	  int		types[fieldCount];
	  int		modifiers[fieldCount];
	  int		formats[fieldCount];
	  int		i;

	  for (i = 0; i < fieldCount; i++)
	    {
	      keys[i] = [NSString stringWithUTF8String: PQfname(result, i)];
	      types[i] = PQftype(result, i);
	      modifiers[i] = PQfmod(result, i);
	      formats[i] = PQfformat(result, i);
	    }

	  records = [[NSMutableArray alloc] initWithCapacity: recordCount];
	  for (i = 0; i < recordCount; i++)
	    {
	      SQLRecord	*record;
	      id	values[fieldCount];
	      int	j;

	      for (j = 0; j < fieldCount; j++)
		{
		  id	v = null;

		  if (PQgetisnull(result, i, j) == 0)
		    {
		      char	*p = PQgetvalue(result, i, j);
		      int	size = PQgetlength(result, i, j);

		      if ([self debugging] > 1)
			{ 
			  [self debug: @"%@ type:%d mod:%d size: %d\n",
			    keys[j], types[j], modifiers[j], size];
			}
		      if (formats[j] == 0)	// Text
			{
			  switch (types[j])
			    {
			      case 1082:	// Date
			      case 1083:	// Time
			      case 1114:	// Timestamp without time zone.
			      case 1184:	// Timestamp with time zone.
				v = [self dbToDateFromBuffer: p
						      length: trim(p)];
				break;

			      case 16:		// BOOL
				if (*p == 't')
				  {
				    v = @"YES";
				  }
				else
				  {
				    v = @"NO";
				  }
				break;

			      case 17:		// BYTEA
				v = [self dataFromBLOB: p];
				break;

			      default:
				trim(p);
				v = [NSString stringWithUTF8String: p];
				break;
			    }
			}
		      else			// Binary
			{
			  NSLog(@"Binary data treated as NSNull "
			    @"in %@ type:%d mod:%d size:%d\n",
			    keys[j], types[j], modifiers[j], size);
			}
		    }
		  values[j] = v;
		}
	      record = [rClass newWithValues: values
					keys: keys
				       count: fieldCount];
	      [records addObject: record];
	      RELEASE(record);
	    }
	}
      else
	{
	  [NSException raise: SQLException format: @"%s",
	    PQresultErrorMessage(result)];
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
	  PQclear(result);
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
      PQclear(result);
    }
  return AUTORELEASE(records);
}

- (unsigned) copyEscapedBLOB: (NSData*)blob into: (void*)buf
{
  const unsigned char	*src = [blob bytes];
  unsigned		sLen = [blob length];
  unsigned char		*ptr = (unsigned char*)buf;
  unsigned		length = 0;
  unsigned		i;

  if (YES == escapeStrings)
    {
      ptr[length++] = 'E';
    }
  ptr[length++] = '\'';
  for (i = 0; i < sLen; i++)
    {
      unsigned char	c = src[i];

      if (c < 32 || c > 126 || c == '\'')
        {
          ptr[length] = '\\';
          ptr[length+1] = '\\';
          ptr[length + 4] = (c & 7) + '0';
          c >>= 3;
          ptr[length + 3] = (c & 7) + '0';
          c >>= 3;
          ptr[length + 2] = (c & 7) + '0';
          length += 5;
        }
      else if (c == '\\')
        {
          ptr[length++] = '\\';
          ptr[length++] = '\\';
          ptr[length++] = '\\';
          ptr[length++] = '\\';
        }
      else
        {
          ptr[length++] = c;
        }
    }
  ptr[length++] = '\'';
  return length;
}

- (unsigned) lengthOfEscapedBLOB: (NSData*)blob
{
  unsigned int	sLen = [blob length];
  unsigned char	*src = (unsigned char*)[blob bytes];
  unsigned int	length = sLen + 2;
  unsigned int	i;

  if (YES == escapeStrings)
    {
      length++;         // Allow for leading 'E'
    }
  for (i = 0; i < sLen; i++)
    {
      unsigned char	c = src[i];

      if (c < 32 || c > 126 || c == '\'')
        {
          length += 4;
        }
      else if (c == '\\')
        {
          length += 3;
        }
    }
  return length;
}

- (NSData *) dataFromBLOB: (const char *)blob
{
  NSMutableData	*md;
  unsigned	sLen = strlen(blob == 0 ? "" : blob);
  unsigned	dLen = 0;
  unsigned char	*dst;
  unsigned	i;

  for (i = 0; i < sLen; i++)
    {
      unsigned	c = blob[i];

      dLen++;
      if (c == '\\')
	{
	  c = blob[++i];
	  if (c != '\\')
	    {
	      i += 2;	// Skip 2 digits octal
	    }
	}
    }
  md = [NSMutableData dataWithLength: dLen];
  dst = (unsigned char*)[md mutableBytes];

  dLen = 0;
  for (i = 0; i < sLen; i++)
    {
      unsigned	c = blob[i];

      if (c == '\\')
	{
	  c = blob[++i];
	  if (c != '\\')
	    {
	      c = c - '0';
	      c <<= 3;
	      c += blob[++i] - '0';
	      c <<= 3;
	      c += blob[++i] - '0';
	    }
	}
      dst[dLen++] = c;
    }
  return md;
}

- (NSDate*) dbToDateFromBuffer: (char*)b length: (int)l
{
  char		buf[l+32];	/* Allow space to expand buffer. */
  NSDate	*d;
  BOOL		milliseconds = NO;
  NSString	*s;
  int		i;

  memcpy(buf, b, l);
  b = buf;
  /*
   * Find end of string.
   */
  for (i = 0; i < l; i++)
    {
      if (b[i] == '\0')
	{
	  l = i;
	  break;
	}
    }
  while (l > 0 && isspace(b[l-1]))
    {
      l--;
    }
  b[l] = '\0';

  if (l == 10)
    {
      s = [NSString stringWithUTF8String: b];
      return [NSCalendarDate dateWithString: s
			     calendarFormat: @"%Y-%m-%d"
				     locale: nil];
    }
  else
    {
      int	e;

      /* If it's a simple date (YYYY-MM-DD) append time for start of day. */
      if (l == 10)
	{
	  strcat(b, " 00:00:00 +0000");
	  l += 15;
	}

      i = l;
      while (i-- > 0)
	{
	  if (b[i] == '+' || b[i] == '-')
	    {
	      break;
	    }
	  if (b[i] == ':' || b[i] == ' ')
	    {
	      i = 0;
	      break;	/* No time zone found */
	    }
	}
      if (i == 0)
	{
	  /* A date and time without a timezone ... assume gmt */
	  strcpy(b + l, " +0000");
	  i = l + 1;
	  l += 6;
	}

      e = i;
      if (isdigit(b[i-1]))
	{
	  /*
	   * Make space between seconds and timezone.
	   */
	  memmove(&b[i+1], &b[i], l - i);
	  b[i++] = ' ';
	  b[++l] = '\0';
	}

      /*
       * Ensure we have a four digit timezone value.
       */
      if (isdigit(b[i+1]) && isdigit(b[i+2]))
	{
	  if (b[i+3] == '\0')
	    {
	      // Two digit time zone ... append zero minutes
	      b[l++] = '0';
	      b[l++] = '0';
	      b[l] = '\0';
	    }
	  else if (b[i+3] == ':')
	    {
	      // Zone with colon before minutes ... remove it
	      b[i+3] = b[i+4];
	      b[i+4] = b[i+5];
	      b[--l] = '\0';
	    }
	}

      /* FIXME ... horrible kludge for postgres returning timestamps with
	 fractional second information. Force it to 3 digit millisecond */
      while (i-- > 0)
	{
	  if (b[i] == '.')
	    {
	      milliseconds = YES;
	      i++;
	      if (!isdigit(b[i]))
		{
		  memmove(&b[i+3], &b[i], e-i);
		  l += 3;
		  memcpy(&b[i], "000", 3);
		}
	      i++;
	      if (!isdigit(b[i]))
		{
		  memmove(&b[i+2], &b[i], e-i);
		  l += 2;
		  memcpy(&b[i], "00", 2);
		}
	      i++;
	      if (!isdigit(b[i]))
		{
		  memmove(&b[i+1], &b[i], e-i);
		  l += 1;
		  memcpy(&b[i], "0", 1);
		}
	      i++;
	      break;
	    }
	}
      if (i > 0 && i < e)
	{
	  memmove(&b[i], &b[e], l - e);
	  l -= (e - i);
	}
      b[l] = '\0';
      if (l == 0)
	{
	  return nil;
	}
      
      s = [NSString stringWithUTF8String: b];
      if (milliseconds == YES)
	{
	  d = [NSCalendarDate dateWithString: s
			      calendarFormat: @"%Y-%m-%d %H:%M:%S.%F %z"
				      locale: nil];
	}
      else
	{
	  d = [NSCalendarDate dateWithString: s
			      calendarFormat: @"%Y-%m-%d %H:%M:%S %z"
				      locale: nil];
	}
      return d;
    }
}

- (void) dealloc
{
  if (extra != 0)
    {
      [self backendDisconnect];
      NSZoneFree(NSDefaultMallocZone(), extra);
    }
  [super dealloc];
}

- (NSString*) quoteString: (NSString *)s
{
  NSData	*d = [s dataUsingEncoding: NSUTF8StringEncoding];
  unsigned	l = [d length];
  unsigned char	*to = NSZoneMalloc(NSDefaultMallocZone(), (l * 2) + 3);

#ifdef	HAVE_PQESCAPESTRINGCONN
  int		err;

  [self backendConnect];
  l = PQescapeStringConn(connection, (char*)(to + 1), [d bytes], l, &err);
#else
  l = PQescapeString(to + 1, [d bytes], l);
#endif
  to[0] = '\'';
  to[l + 1] = '\'';
  s = [[NSString alloc] initWithBytesNoCopy: to
				     length: l + 2
				   encoding: NSUTF8StringEncoding
			       freeWhenDone: YES];
  return AUTORELEASE(s);
}

@end

