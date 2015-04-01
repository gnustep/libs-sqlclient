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
#import	<Foundation/NSDictionary.h>
#import	<Foundation/NSException.h>
#import	<Foundation/NSFileHandle.h>
#import	<Foundation/NSProcessInfo.h>
#import	<Foundation/NSNotification.h>
#import	<Foundation/NSNotificationQueue.h>
#import	<Foundation/NSUserDefaults.h>
#import	<Foundation/NSMapTable.h>
#import	<Foundation/NSLock.h>
#import	<Foundation/NSNull.h>
#import	<Foundation/NSValue.h>
#import	<Foundation/NSAutoreleasePool.h>

#include	"config.h"

#define SQLCLIENT_PRIVATE       @public

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
  int           _backendPID;
} ConnectionInfo;

#define	cInfo			((ConnectionInfo*)(self->extra))
#define	backendPID		(cInfo->_backendPID)
#define	connection		(cInfo->_connection)

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
      [future retain];
      null = [NSNull null];
      [null retain];
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
  return [m autorelease];
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
	  str = connectQuote([self clientName]);
	  if (str != nil)
	    {
	      [m appendString: @" application_name="];
	      [m appendString: str];
	    }

	  if ([self debugging] > 0)
	    {
	      [self debug: @"Connect to '%@' as %@ (%@)",
                m, [self name], [self clientName]];
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

              backendPID = PQbackendPID(connection);

	      connected = YES;

	      p = PQparameterStatus(connection, "standard_conforming_strings");
              if (p != 0)
                {
                  /* If the escape_string_warning setting is on,
                   * the server will warn about backslashes even
                   * in properly quoted strings, so turn it off.
                   */
                  if (strcmp(p, "on") == 0)
                    {
                      [self execute: @"SET escape_string_warning=off", nil];
                    }
                  else
                    {
                      [self execute: @"SET standard_conforming_strings=on;"
                        @"SET escape_string_warning=off", nil];
                    }
                }
              else
                {
                  PQfinish(connection);
                  connection = 0;
                  connected = NO;
		  [self debug: @"Postgres without standard conforming strings"];
                }

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

- (void) _checkNotifications
{
  NSNotificationQueue   *nq = nil;
  PGnotify              *notify;

  while ((notify = PQnotifies(connection)) != 0)
    {
      NS_DURING
        {
          NSNotification        *n;
          NSMutableDictionary   *userInfo;
          NSString              *name;

          name = [[NSString alloc] initWithUTF8String: notify->relname];
          userInfo = [[NSMutableDictionary alloc] initWithCapacity: 2];
          if (0 != notify->extra)
            {
              NSString      *payload;

              payload = [[NSString alloc] initWithUTF8String: notify->extra];
              if (nil != payload)
                {
                  [userInfo setObject: payload forKey: @"Payload"];
                  [payload release];
                }
            }
          if (notify->be_pid == backendPID)
            {
              static NSNumber   *nY = nil;

              if (nil == nY)
                {
                  nY = [[NSNumber numberWithBool: YES] retain];
                }
              [userInfo setObject: nY forKey: @"Local"];
            }
          else
            {
              static NSNumber   *nN = nil;

              if (nil == nN)
                {
                  nN = [[NSNumber numberWithBool: NO] retain];
                }
              [userInfo setObject: nN forKey: @"Local"];
            }
          n = [NSNotification notificationWithName: name
                                            object: self
                                          userInfo: (NSDictionary*)userInfo];
          [name release];
          [userInfo release];

          if (nil == nq)
            {
              nq = [NSNotificationQueue defaultQueue];
            }
          /* Post asynchronously
           */
          [nq enqueueNotification: n postingStyle: NSPostASAP];
        }
      NS_HANDLER
        {
          NSLog(@"Problem handling asynchronous notification: %@",
            localException);
        }
      NS_ENDHANDLER
      PQfreemem(notify);
    }
}

- (NSInteger) backendExecute: (NSArray*)info
{
  NSAutoreleasePool     *arp = [NSAutoreleasePool new];
  NSInteger     rowCount = -1;
  PGresult	*result = 0;
  NSString	*stmt = [info objectAtIndex: 0];

  if ([stmt length] == 0)
    {
      [arp release];
      [NSException raise: NSInternalInconsistencyException
		  format: @"Statement produced null string"];
    }

  NS_DURING
    {
      const char	*statement;
      const char        *tuples;
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

      result = PQexec(connection, statement);
      if (0 == result || PQresultStatus(result) == PGRES_FATAL_ERROR)
	{
	  NSString	*str;
	  const char	*cstr;

	  if (0 == result)
	    {
	      cstr = PQerrorMessage(connection);
	    }
	  else
	    {
	      cstr = PQresultErrorMessage(result);
	    }
	  str = [NSString stringWithUTF8String: cstr];
          if (nil == str)
            {
              str = [NSString stringWithCString: cstr];
            }
	  [NSException raise: SQLException format: @"Error executing %@: %@",
	    stmt, str];
	}
      if (PQresultStatus(result) != PGRES_COMMAND_OK
        && PQresultStatus(result) != PGRES_TUPLES_OK)
	{
	  [NSException raise: SQLException format: @"Error executing %@: %s",
	    stmt, PQresultErrorMessage(result)];
	}
      tuples = PQcmdTuples(result);
      if (0 != tuples)
        {
          rowCount = atol(tuples);
        }
    }
  NS_HANDLER
    {
      NSString	*n = [localException name];

      if (result != 0)
	{
	  PQclear(result);
	}
      if (YES == connected && [n isEqual: SQLConnectionException] == YES) 
	{
	  [self disconnect];
	}
      [localException retain];
      [arp release];
      [localException autorelease];
      [localException raise];
    }
  NS_ENDHANDLER
  if (result != 0)
    {
      PQclear(result);
    }
  [self _checkNotifications];
  [arp release];
  return rowCount;
}

- (void) backendListen: (NSString*)name
{
  [self execute: @"LISTEN ", name, nil];
}

- (void) backendNotify: (NSString*)name payload: (NSString*)more
{
  if (nil == more)
    {
      [self execute: @"NOTIFY ", name, nil];
    }
  else
    {
      [self execute: @"NOTIFY ", name, @",", [self quote: more], nil];
    }
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

- (char*) parseIntoArray: (NSMutableArray *)a type: (int)t from: (char*)p
{
  p++;  /* Step past '{' */
  while (*p && *p != '}')
    {
      id        v = nil;

      /* Ignore leading space before field data.
       */
      while (isspace(*p))
        {
          p++;
        }
      if ('{' == *p)
        {
          /* Found a nested array.
           */
          v = [[NSMutableArray alloc] initWithCapacity: 10];
          p = [self parseIntoArray: v type: t from: p]; 
        }
      else if ('\"' == *p)
        {
          char  *start = ++p;
          int   len = 0;
        
          /* Found something quoted (char, varchar, text, bytea etc)
           */
          while (*p != '\0' && *p != '\"')
            {
              if ('\'' == *p)
                {
                  p++;
                }
              p++;
              len++;
            }
          if ('\"' == *p)
            {
              *p = '\0';
            }
          if (len == (p - start))
            {
              v = [[NSString alloc] initWithUTF8String: start];
            }
          else
            {
              char      *buf;
              char      *ptr;
              int       i;

              buf = malloc(len+1);
              ptr = start;
              i  = 0;
              while (*ptr != '\0')
                {
                  if ('\\' == *ptr)
                    {
                      ptr++;
                    }
                  buf[i++] = *ptr++;
                }
              buf[len] = '\0';
              if ('D' == t)
                {
                  /* This is expected to be bytea data
                   */
                  v = [[self dataFromBLOB: buf] retain];
                }
              else
                {
                  v = [NSString alloc];
                  v = [v initWithBytesNoCopy: buf
                                      length: len
                                    encoding: NSUTF8StringEncoding
                                freeWhenDone: YES];
                }
            }
          *p++ = '\"';
        }
      else
        {
          char  *start = p;
          char  save;
          int   len;

          /* This is an unquoted field ... could be NULL or a boolean,
           * or a numeric field, or a timestamp or just a simple string.
           */
          while (*p != '\0' && *p != ',' && *p != '}')
            {
              p++;
            }
          save = *p;
          *p = '\0'; 
          len = trim(start);
          if (strcmp(start, "NULL") == 0)
            {
              v = null;
            }
          else if ('T' == t)
            {
              v = [[self dbToDateFromBuffer: start length: len] retain];
            }
          else if ('D' == t)
            {
              v = [[self dataFromBLOB: start] retain];
            }
          else if ('B' == t)
            {
              if (*start == 't')
                v = @"YES";
              else
                v = @"NO";
            }
          else
            {
              v = [[NSString alloc] initWithUTF8String: start];
            }
          *p = save;
        }
      if (nil != v)
        {
          [a addObject: v];
          [v release];
        }
      if (',' == *p)
        {
          p++;
        }
    }
  if ('}' == *p)
    {
      p++;
    }
  return p;
}

- (id) parseField: (char *)p type: (int)t
{
  char  arrayType = 0;

  switch (t)
    {
      case 1082:	// Date
        return [self dbToDateFromBuffer: p length: trim(p)];

      case 1083:	// Time (treat as string)
        trim(p);
        return [NSString stringWithUTF8String: p];

      case 1114:	// Timestamp without time zone.
      case 1184:	// Timestamp with time zone.
        return [self dbToDateFromBuffer: p length: trim(p)];

      case 16:		// BOOL
        if (*p == 't')
          {
            return @"YES";
          }
        else
          {
            return @"NO";
          }

      case 17:		// BYTEA
        return [self dataFromBLOB: p];

      case 18:          // "char"
        return [NSString stringWithUTF8String: p];

      case 20:          // INT8
      case 21:          // INT2
      case 23:          // INT4
        trim(p);
        return [NSString stringWithUTF8String: p];
        break;

      case 1182:	// DATE ARRAY
      case 1115:	// TS without TZ ARRAY
      case 1185:	// TS with TZ ARRAY
        if (0 == arrayType) arrayType = 'T';    // Timestamp
      case 1000:        // BOOL ARRAY
        if (0 == arrayType) arrayType = 'B';    // Boolean
      case 1001:        // BYTEA ARRAY
        if (0 == arrayType) arrayType = 'D';    // Data
      case 1005:        // INT2 ARRAY
      case 1007:        // INT4 ARRAY
      case 1016:        // INT8 ARRAY
      case 1021:        // FLOAT ARRAY
      case 1022:        // DOUBLE ARRAY
      case 1002:        // CHAR ARRAY
      case 1009:        // TEXT ARRAY
      case 1015:        // VARCHAR ARRAY
      case 1263:        // CSTRING ARRAY
        if ('{' == *p)
          {
            NSMutableArray      *a;

            a = [NSMutableArray arrayWithCapacity: 10];
            p = [self parseIntoArray: a type: arrayType from: p];
            if ([self debugging] > 2)
              {
                NSLog(@"Parsed array is %@", a);
              }
            return a;
          }

      case 25:          // TEXT
      default:
        if (YES == _shouldTrim)
          {
            trim(p);
          }
        return [NSString stringWithUTF8String: p];
    }
}

- (NSMutableArray*) backendQuery: (NSString*)stmt
		      recordType: (id)rtype
		        listType: (id)ltype
{
  NSAutoreleasePool     *arp = [NSAutoreleasePool new];
  PGresult		*result = 0;
  NSMutableArray	*records = nil;

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
      result = PQexec(connection, statement);
      if (0 == result || PQresultStatus(result) == PGRES_FATAL_ERROR)
	{
	  NSString	*str;
	  const char	*cstr;

	  if (0 == result)
	    {
	      cstr = PQerrorMessage(connection);
	    }
	  else
	    {
	      cstr = PQresultErrorMessage(result);
	    }
	  str = [NSString stringWithUTF8String: cstr];
          if (nil == str)
            {
              str = [NSString stringWithCString: cstr];
            }
	  [NSException raise: SQLException format: @"Error executing %@: %@",
	    stmt, str];
	}
      if (PQresultStatus(result) == PGRES_TUPLES_OK)
	{
	  int		recordCount = PQntuples(result);
	  int		fieldCount = PQnfields(result);
	  NSString	*keys[fieldCount];
	  int		ftype[fieldCount];
	  int		fmod[fieldCount];
	  int		fformat[fieldCount];
	  int		i;

	  for (i = 0; i < fieldCount; i++)
	    {
	      keys[i] = [NSString stringWithUTF8String: PQfname(result, i)];
	      ftype[i] = PQftype(result, i);
	      fmod[i] = PQfmod(result, i);
	      fformat[i] = PQfformat(result, i);
	    }

	  records = [[ltype alloc] initWithCapacity: recordCount];
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
			    keys[j], ftype[j], fmod[j], size];
			}
		      if (fformat[j] == 0)	// Text
			{
                          v = [self parseField: p type: ftype[j]];
			}
		      else			// Binary
			{
			  NSLog(@"Binary data treated as NSNull "
			    @"in %@ type:%d mod:%d size:%d\n",
			    keys[j], ftype[j], fmod[j], size);
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
	  [NSException raise: SQLException format: @"Error executing %@: %s",
	    stmt, PQresultErrorMessage(result)];
	}
    }
  NS_HANDLER
    {
      NSString	*n = [localException name];

      if (result != 0)
	{
	  PQclear(result);
	}
      if (YES == connected && [n isEqual: SQLConnectionException] == YES) 
	{
	  [self disconnect];
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
      PQclear(result);
    }
  [self _checkNotifications];
  return [records autorelease];
}

- (void) backendUnlisten: (NSString*)name
{
  [self execute: @"UNLISTEN ", name, nil];
}

- (unsigned) copyEscapedBLOB: (NSData*)blob into: (void*)buf
{
  const unsigned char	*src = [blob bytes];
  unsigned		sLen = [blob length];
  unsigned char		*ptr = (unsigned char*)buf;
  unsigned		length = 0;
  unsigned		i;

  ptr[length++] = 'E';
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

  length++;         // Allow for leading 'E'
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

  if (sLen > 1 && '\\' == blob[0] && 'x' == blob[1])
    {
      dLen = (sLen - 2) / 2;
      dst = (unsigned char*)NSAllocateCollectable(dLen, 0);
      md = [NSMutableData dataWithBytesNoCopy: dst length: dLen];
      dLen = 0;
      for (i = 2; i < sLen; i += 2)
	{
	  unsigned	hi = blob[i];
	  unsigned	lo = blob[i + 1];

	  hi = (hi > '9') ? (hi - 'a' + 10) : (hi - '0');
	  lo = (lo > '9') ? (lo - 'a' + 10) : (lo - '0');
	  dst[dLen++] = (hi << 4) + lo;
	}
    }
  else
    {
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

      dst = (unsigned char*)NSAllocateCollectable(i, dLen);
      md = [NSMutableData dataWithBytesNoCopy: dst length: dLen];
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
    }
  return md;
}

- (NSDate*) dbToDateFromBuffer: (char*)b length: (int)l
{
  char		        buf[l+32];	/* Allow space to expand buffer. */
  NSCalendarDate	*d;
  BOOL		        milliseconds = NO;
  BOOL                  timezone = NO;
  NSString	        *s;
  int		        i;
  int	                e;

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

  i = l;

  /* Convert +/-HH:SS timezone to +/-HHSS
   */
  if (i > 5 && b[i-3] == ':' && (b[i-6] == '+' || b[i-6] == '-'))
    {
      b[i-3] = b[i-2];
      b[i-2] = b[i-1];
      b[--i] = '\0';
    }

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
      e = l;
    }
  else
    {
      timezone = YES;
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
    }

  /* kludge for timestamps with fractional second information.
   * Force it to 3 digit millisecond */
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

  if (YES == timezone)
    {
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
    }
  else
    {
      if (milliseconds == YES)
        {
          d = [NSCalendarDate dateWithString: s
                              calendarFormat: @"%Y-%m-%d %H:%M:%S.%F"
                                      locale: nil];
        }
      else
        {
          d = [NSCalendarDate dateWithString: s
                              calendarFormat: @"%Y-%m-%d %H:%M:%S"
                                      locale: nil];
        }
    }
  [d setCalendarFormat: @"%Y-%m-%d %H:%M:%S %z"];
  return d;
}

- (void) dealloc
{
  if (extra != 0)
    {
      if (YES == connected)
        {
          [self disconnect];
        }
      NSZoneFree(NSDefaultMallocZone(), extra);
    }
  [super dealloc];
}

- (NSMutableString*) quoteArray: (NSArray *)a
                       toString: (NSMutableString *)s
                 quotingStrings: (BOOL)q
{
  NSUInteger    count;
  NSUInteger    index;

  NSAssert([a isKindOfClass: [NSArray class]], NSInvalidArgumentException);
  if (nil == s)
    {
      s = [NSMutableString stringWithCapacity: 1000];
    }
  [s appendString: @"ARRAY["];
  count = [a count];
  for (index = 0; index < count; index++)
    {
      id        o = [a objectAtIndex: index];

      if (index > 0)
        {
          [s appendString: @","];
        }
      if ([o isKindOfClass: [NSArray class]])
        {
          [self quoteArray: (NSArray *)o toString: s quotingStrings: q];
        }
      else if ([o isKindOfClass: [NSString class]])
        {
          if (YES == q)
            {
              o = [self quoteString: (NSString*)o];
            }
          [s appendString: (NSString*)o];
        }
      else if ([o isKindOfClass: [NSDate class]])
        {
          [s appendString: [self quote: (NSString*)o]];
          [s appendString: @"::timestamp with time zone"];
        }
      else if ([o isKindOfClass: [NSData class]])
        {
          unsigned      len = [self lengthOfEscapedBLOB: o];
          uint8_t       *buf;

          buf = malloc(len+1);
          [self copyEscapedBLOB: o into: buf];
          buf[len] = '\0';
          [s appendFormat: @"%s::bytea", buf];
          free(buf);
        }
      else
        {
          o = [self quote: (NSString*)o];
          [s appendString: (NSString*)o];
        }
    }
  [s appendString: @"]"];
  return s;
}

- (NSString*) quoteString: (NSString *)s
{
  NSData	*d = [s dataUsingEncoding: NSUTF8StringEncoding];
  unsigned	l = [d length];
  unsigned char	*to = NSZoneMalloc(NSDefaultMallocZone(), (l * 2) + 3);

#if 1
  const char    *from = (const char*)[d bytes];
  unsigned      i = 0;
  unsigned      j = 0;

  to[j++] = '\'';
  while (i < l)
    {
      if ('\'' == (to[j++] = from[i++]))
        {
          to[j++] = '\'';
        }
    }
  to[j++] = '\'';
  l = j - 2;
#else
#ifdef	HAVE_PQESCAPESTRINGCONN
  int		err;

  [lock lock];
  NS_DURING
    {
      [self connect];
      l = PQescapeStringConn(connection,
        (char*)(to + 1), [d bytes], l, &err);
    }
  NS_HANDLER
    {
      [lock unlock];
      NSZoneFree(NSDefaultMallocZone(), to);
      [localException raise];
    }
  NS_ENDHANDLER
  [lock unlock];
#else
  l = PQescapeString(to + 1, [d bytes], l);
#endif
  to[0] = '\'';
  to[l + 1] = '\'';
#endif

  s = [[NSString alloc] initWithBytesNoCopy: to
				     length: l + 2
				   encoding: NSUTF8StringEncoding
			       freeWhenDone: YES];
  return [s autorelease];
}

@end

