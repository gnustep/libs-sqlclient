/* -*-objc-*- */

/** Implementation of SQLClientOracle for GNUStep
   Copyright (C) 2004 Free Software Foundation, Inc.
   
   Written by:  Richard Frith-Macdonald <rfm@gnu.org>
   Written by:  Nicola Pero <nicola@brainsrtorm.co.uk>
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

#include	"SQLClient.h"

/*
 * Example configuration for an Oracle database:
 *
 *   oracle-test = {
 *          ServerType = "Oracle"
 *          SQLDatabase = "nicola";
 *          SQLPassword = "mbrand";
 *          SQLUser = "mbrand";
 *        };
 *
 *  Where SQLDatabase is the Unique database Identifier.
 *
 */

@interface SQLClientOracle : SQLClient
@end

@interface	SQLClientOracle(Embedded)
- (const char *) blobFromData: (NSData*)data;
- (NSData *) dataFromBlob: (const char *)blob;
- (BOOL) dbFromDate: (NSDate*)d toBuffer: (char*)b length: (int)l;
- (BOOL) dbFromString: (NSString*)s toBuffer: (char*)b length: (int)l;
- (NSDate*) dbToDateFromBuffer: (char*)b length: (int)l;
- (NSString*) dbToStringFromBuffer: (char*)b length: (int)l;
@end


EXEC SQL INCLUDE sqlca;
EXEC SQL WHENEVER SQLERROR DO SQLClientOracleErrorHandler();

/**
 * Return YES of the last SQL error indicated we are out of data,
 * NO otherwise.
 */
BOOL SQLClientOracleOutOfData()
{
  if (sqlca.sqlcode == 100)
    {
      return YES;
    }
  else
    {
      return NO;
    }
}

/**
 * This error handler is called for most errors ... so we can get it to
 * raise an exception for us.
 */
void SQLClientOracleErrorHandler()
{
  int		code = sqlca.sqlcode;
  const char	*ptr = sqlca.sqlerrm.sqlerrmc;
  const char	*e0 = "'no connection to the server'";
  const char	*e1 = "Error in transaction processing";

  sqlca.sqlcode = 0;	// Reset error code
  NSLog (@"(Oracle) Raising an exception, %ld, %s",
	 code, sqlca.sqlerrm.sqlerrmc);
  
  if (strncmp(ptr, e0, strlen(e0)) == 0
    || strncmp(ptr, e1, strlen(e1)) == 0)
    {
      [NSException raise: SQLConnectionException
		  format: @"(Oracle) SQL Error: SQLCODE=(%ld): %s", code, ptr];
    }
  else
    {
      [NSException raise: SQLException
		  format: @"(Oracle) SQL Error: SQLCODE=(%ld): %s", code, ptr];
    }
}

@implementation	SQLClientOracle

- (BOOL) backendConnect
{
  if (connected == NO)
    {
      if (database != nil  &&  user != nil  &&  password != nil)
	{
	  Class		c = NSClassFromString(@"CmdClient");

	  [[self class] purgeConnections: nil];

	  NS_DURING
	    {
	      EXEC SQL BEGIN DECLARE SECTION;
	      const char *database_c;
	      const char *user_c;
	      const char *password_c;
	      const char *client_c;
	      EXEC SQL END DECLARE SECTION;

	      /* Database is the Oracle Net identifier for the database.  */
	      database_c = [database UTF8String];

	      /* User and password are used to connect to the database.  */
	      user_c = [user UTF8String];
	      password_c = [password UTF8String];

	      /* Client is only used to give this connection a name
	       * and distinguish it from other connections.
	       */
	      client_c = [client UTF8String];
	      
	      if (c != 0)
		{
		  [self debug: @"(Oracle) Connect to database %s user %s as %s", 
			database_c, user_c, client_c];
	        }

	      EXEC SQL CONNECT :user_c IDENTIFIED BY :password_c 
		            AT :client_c USING :database_c;

	      if (c != 0)
		{
		  [self debug: @"(Oracle) Connected (%s)", client_c];
		}
	      connected = YES;
	    }
	  NS_HANDLER
	    {
	      [self error: @"(Oracle) Error connecting to database: %@",
		    localException];
	    }
	  NS_ENDHANDLER
	}
      else
	{
	  [self error:
	    @"(Oracle) Connect with no user/password/database configured"];
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
	  EXEC SQL BEGIN DECLARE SECTION;
	  const char	*client_c;
	  EXEC SQL END DECLARE SECTION;

	  if (inTransaction == YES)
	    {
	      [self rollback];
	    }

	  client_c = [client UTF8String];
	  
	  [self debug: @"(Oracle) Disconnecting client %@", client];
	  
	  /* To disconnect from the database, we issuse a COMMIT
	   * statement with the RELEASE option.  The RELEASE option
	   * causes it to disconnect after the COMMIT.
	   */
	  EXEC SQL AT :client_c COMMIT WORK RELEASE;

	  [self debug: @"(Oracle) Disconnected client %@", client];
	}
      NS_HANDLER
	{
	  [self error: @"(Oracle) Error disconnecting from database (%@): %@",
	    client, localException];
	}
      NS_ENDHANDLER
      connected = NO;
    }
}

- (void) backendExecute: (NSArray*)info
{
  EXEC SQL BEGIN DECLARE SECTION;
  char	*statement;
  char	*handle;
  EXEC SQL END DECLARE SECTION;
  CREATE_AUTORELEASE_POOL(arp);
  NSString		*stmt = [info objectAtIndex: 0];
  unsigned int		length;
  BOOL                  manuallyAutoCommit = NO;

  length = [stmt length];
  if (length == 0)
    {
      [NSException raise: NSInternalInconsistencyException
		  format: @"(Oracle) Statement produced null string"];
    }

  statement = (char*)[stmt UTF8String];
  handle = (char*)[[self clientName] UTF8String];

  /*
   * Ensure we have a working connection.
   */
  if ([self backendConnect] == NO)
    {
      [NSException raise: SQLException
		  format: @"(Oracle) Unable to connect to database"];
    } 

  NS_DURING
    {
      if (inTransaction == NO)
	{
	  manuallyAutoCommit = YES;
	}

      EXEC SQL AT :handle PREPARE command FROM :statement;
      EXEC SQL AT :handle EXECUTE command;

      if (manuallyAutoCommit)
	{
	  EXEC SQL AT :handle COMMIT; 
	}
    }
  NS_HANDLER
    {
      NSString	*n = [localException name];
      NSString	*msg = [localException reason];

      if (manuallyAutoCommit)
	{
	  EXEC SQL AT :handle ROLLBACK; 
	}

      if ([n isEqual: SQLConnectionException] == YES) 
	{
	  [self backendDisconnect];
	}
      /*
       * remove line number information from database exception message
       * since it's meaningless to the developer as it's the line number
       * in this file rather than the code which is calling us.
       */
      if ([n isEqual: SQLException] == YES
	|| [n isEqual: SQLConnectionException] == YES) 
	{
	  NSRange	r;

	  r = [msg rangeOfString: @" in line " options: NSBackwardsSearch];
	  if (r.length > 0)
	    {
	      msg = [msg substringToIndex: r.location];
	      localException = [NSException exceptionWithName: n
						       reason: msg
						     userInfo: nil];
	    }
	}
      [self error: @"(Oracle) Error executing statement:\n%@\n%@",
	    stmt, localException];
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
  EXEC SQL BEGIN DECLARE SECTION;
  int	count;
  int	index;

  short int indicator;
  int	type;
  int	length;
  int	octetLength;
  short int	returnedOctetLength;
  char	fieldName[120];

  char	*aString;
  
  /* This holds a string representation of numbers returned by Oracle.
   * 128 seems a safe bound - else they'll be truncated. */
  char aNumber[128];

  char	*query;
  char  *handle;
  EXEC SQL END DECLARE SECTION;
  CREATE_AUTORELEASE_POOL(arp);
  NSMutableArray	*records;
  BOOL			isOpen = NO;
  BOOL			wasInTransaction = inTransaction;
  BOOL                  allocatedDescriptor = NO;

  length = [stmt length];
  if (length == 0)
    {
      [NSException raise: NSInternalInconsistencyException
		  format: @"(Oracle) Statement produced null string"];
    }

  handle = (char*)[[self clientName] UTF8String];
  query = (char*)[stmt UTF8String];
  records = [[NSMutableArray alloc] initWithCapacity: 32];

  /*
   * Ensure we have a working connection.
   */
  if ([self backendConnect] == NO)
    {
      [NSException raise: SQLException
		  format: @"(Oracle) Unable to connect to database"];
    } 

  NS_DURING
    {
      /* This is really the output descriptor.  We do not use input
       * descriptors; all the input is in the SQL statement.
       */
      EXEC SQL ALLOCATE DESCRIPTOR 'myDesc';
      allocatedDescriptor = YES;

      EXEC SQL AT :handle PREPARE myQuery from :query;
      if (inTransaction == NO)
	{
	  /* EXEC SQL AT :handle BEGIN; */
	  inTransaction = YES;
	}
      EXEC SQL AT :handle DECLARE myCursor CURSOR FOR myQuery;
      EXEC SQL AT :handle OPEN myCursor;
      isOpen = YES;
      
      EXEC SQL AT :handle DESCRIBE OUTPUT myQuery USING DESCRIPTOR 'myDesc';

      EXEC SQL GET DESCRIPTOR 'myDesc' :count = COUNT;

      if (count > 0)
	{
	  /* Now we do what the Oracle examples do, which is we forcefully
	   * require to the library to convert everything into types
	   * chosen by us (mostly strings).  The reason we do it is that
	   * managing the 'internal' Oracle datatypes is a daunting task
	   * (for example numbers are returned in a 22 byte representation
	   * used internally by Oracle ...) and apparently it's now how
	   * they expect you to use it - they provide no examples or
	   * explanations of how to do it btw!  They expect you to choose
	   * which 'external' Oracle representation you want, by using SET
	   * DESCRIPTOR as we do here, and then FETCH comfortably data
	   * which is returned in the representation you chose.  So we do
	   * that way.
	   */
	  int originalType[count];
	  
	  for (index = 1; index <= count; index++)
	    {
	      EXEC SQL GET DESCRIPTOR 'myDesc' VALUE :index 
		:length = LENGTH,
		:octetLength = OCTET_LENGTH,
		:type = TYPE;

	      /* Save the original type so that we know later what's
	       * inside each returned value.  */
	      originalType[index - 1] = type;
	      
	      switch (type)
		{
		  /* Negative values of 'type' are used for Oracle
		   * proprietary extensions; positive values for ANSI
		   * types.  */

		  /* We get character types as they are.  */
		case 1 /* CHARACTER */:
		case 12 /* CHARACTER_VARYING */:
		case -1 /* Oracle VARCHAR2 */:
		  type = -1; /* Oracle VARCHAR2 */
		  EXEC SQL SET DESCRIPTOR 'myDesc' VALUE :index 
		    TYPE = :type;
		  break;

		  /* We get a string representation (128 bytes long)
		   * of any number.  */
		case 2 /* NUMERIC */:
		case 3 /* DECIMAL */:
		case 4 /* INTEGER */:
		case 5 /* SMALLINT*/: 
		case 6 /* FLOAT */:
		case 7 /* REAL */:
		case 8 /* DOUBLE_PRECISION */:
		  type = 12; /* ANSI CHARACTER_VARYING */
		  octetLength = 128;
		  
		  EXEC SQL SET DESCRIPTOR 'myDesc' VALUE :index 
		    LENGTH = :octetLength,
		    TYPE = :type;

		  break;
		}
	    }
	  
	  while (1)
	    {
	      SQLRecord	*record;
	      id	keys[count];
	      id	values[count];
	      
	      EXEC SQL AT :handle FETCH myCursor INTO SQL DESCRIPTOR 'myDesc';
	   
	      if (sqlca.sqlcode)
		{
		  break;
		} 
	      
	      for (index = 1; index <= count; ++index)
		{
		  id	v;
		  
		  EXEC SQL GET DESCRIPTOR 'myDesc' VALUE :index 
		    :indicator = INDICATOR,
		    :length = LENGTH,
		    :fieldName = NAME,
		    :octetLength = OCTET_LENGTH,
		    :returnedOctetLength = RETURNED_OCTET_LENGTH,
		    :type = TYPE;
		  
		  if (indicator == -1)
		    {
		      v = [NSNull null];
		    }
		  else
		    {
		      switch (originalType[index - 1])
			{
			case 3 /* DECIMAL */:
			case 4 /* INTEGER */:
			case 5 /* SMALLINT*/: 
			  {
			    int aInt;
			    
			    EXEC SQL GET DESCRIPTOR 'myDesc' VALUE :index
			      :aNumber = DATA;
			    
			    aInt = [[NSString stringWithUTF8String: aNumber] intValue];
			    v = [NSNumber numberWithInt: aInt];
			    
			    break;
			  }
			  
			case 2 /* NUMERIC */:
			case 6 /* FLOAT */:
			case 7 /* REAL */:
			case 8 /* DOUBLE_PRECISION */:
			  {
			    float aFloat;
			    
			    EXEC SQL GET DESCRIPTOR 'myDesc' VALUE :index
			      :aNumber = DATA;
			    
			    aFloat = [[NSString stringWithUTF8String: aNumber] floatValue];
			    v = [NSNumber numberWithFloat: aFloat];
			    
			    break;
			  }
			  
			case 1 /* CHARACTER */:
			case 12 /* CHARACTER_VARYING */:
			case -1 /* Oracle VARCHAR2 */:
			  /* For unclear reasons, returnedOctetLength is always 0.  */

			  /* This code (patchy and experimentally
			   * determined) really works if the database
			   * field contains something like UTF-8,
			   * returned as UTF-8 (such as for CHAR(20)
			   * fields).  If UNICODE stuff is returned,
			   * then it's not the right way.  We might
			   * need to make a different depending on the
			   * originalField type.
			   */

			  /* Add 1 byte to \0-pad the string.  */
			  aString = malloc (octetLength + 1);
			  if (aString == NULL)
			    {
			      [NSException 
				raise: @"OutOfMemoryException"
				format: @"(Oracle) could not malloc %d bytes", 
				octetLength];
			    }
			  
			  EXEC SQL GET DESCRIPTOR 'myDesc' VALUE :index
			    :aString = DATA;
			  
			  /* \0-pad the string.  */
			  aString[octetLength] = '\0'; 
			  trim (aString);
			  v = [NSString stringWithUTF8String: aString];
			  free(aString);
			  break;
			  
			  /* TODO: DATES */
			  
			  /*
			    TODO TODO
			    
			    case BLOB:
			    EXEC SQL GET DESCRIPTOR 'myDesc' VALUE :index
			    :aString = DATA;
			    v = [self dataFromBlob: aString];
			    free(aString);
			    break;
			  */
			    
			default:
			  aString = malloc (octetLength + 1);
			  if (aString == NULL)
			    {
			      [NSException 
				  raise: @"OutOfMemoryException"
				format: @"(Oracle) could not malloc %d bytes", 
				octetLength];
			    }
			  
			  EXEC SQL GET DESCRIPTOR 'myDesc' VALUE :index
			    :aString = DATA;
			  aString[octetLength] = '\0';
			  trim (aString);
			  v = [NSString stringWithUTF8String: aString];
			  free (aString);
			  NSLog(@"(Oracle) Unknown data type (%d) for '%s': '%@'", 
				type, fieldName, v);
			  break;
			}
		    }
		  
		  values[index - 1] = v;
		  keys[index - 1] = [NSString stringWithUTF8String:
						fieldName];
		}
	      record = [SQLRecord newWithValues: values
				  keys: keys
				  count: count];
	      [records addObject: record];
	      RELEASE(record);
	    }
	}
      
      isOpen = NO;
      EXEC SQL AT :handle CLOSE myCursor;
      if (wasInTransaction == NO && inTransaction == YES)
	{
	  EXEC SQL AT :handle COMMIT;
	  inTransaction = NO;
	}
      EXEC SQL DEALLOCATE DESCRIPTOR 'myDesc';
      allocatedDescriptor = NO;
    }
  NS_HANDLER
    {
      NSString	*n = [localException name];
      NSString	*msg = [localException reason];
      
      DESTROY(records);

      NS_DURING
	{
	  if (isOpen == YES)
	    {
	      EXEC SQL AT :handle CLOSE myCursor;
	    }
	  if (wasInTransaction == NO && inTransaction == YES)
	    {
	      EXEC SQL AT :handle ROLLBACK;
	      inTransaction = NO;
	    }
	}
      NS_HANDLER
	{
	  NSString	*e = [localException name];

	  if (wasInTransaction == NO && inTransaction == YES)
	    {
	      inTransaction = NO;
	    }
	  if ([e isEqual: SQLConnectionException] == YES) 
	    {
	      [self backendDisconnect];
	    }
	}
      NS_ENDHANDLER

      NS_DURING
	{
	  if (allocatedDescriptor)
	    {
	      EXEC SQL DEALLOCATE DESCRIPTOR 'myDesc';
	      allocatedDescriptor = NO;
	    }
	}
      NS_HANDLER
	{
	  NSLog (@"Can't deallocate descriptor ... serious problem.");
	}
      NS_ENDHANDLER


      if ([n isEqual: SQLConnectionException] == YES) 
	{
	  inTransaction = NO;
	  [self backendDisconnect];
	}

      /*
       * remove line number information from database exception message
       * since it's meaningless to the developer as it's the line number
       * in this file rather than the code which is calling us.
       */
      if ([n isEqual: SQLException] == YES
	|| [n isEqual: SQLConnectionException] == YES) 
	{
	  NSRange	r;

	  r = [msg rangeOfString: @" in line " options: NSBackwardsSearch];
	  if (r.length > 0)
	    {
	      msg = [msg substringToIndex: r.location];
	      localException = [NSException exceptionWithName: n
					    reason: msg
					    userInfo: nil];
	    }
	}
      RETAIN(localException);
      RELEASE(arp);
      AUTORELEASE(localException);
      [localException raise];
    }
  NS_ENDHANDLER
  DESTROY(arp);

  return AUTORELEASE(records);
}

/**
 * Convert NSData object with raw binary data into escaped sequence
 */
- (const char *) blobFromData: (NSData*)data
{
  NSMutableData	*md;
  unsigned	sLen = [data length];
  unsigned char	*src = (unsigned char*)[data bytes];
  unsigned	dLen = 0;
  unsigned char	*dst;
  unsigned	i;

  for (i = 0; i < sLen; i++)
    {
      unsigned char	c = src[i];

      if (c < 32 || c > 126)
	{
	  dLen += 4;
	}
      else if (c == 92)
	{
	  dLen += 2;
	}
      else
	{
	  dLen += 1;
	}
    }
  md = [NSMutableData dataWithLength: dLen + 1];
  dst = (unsigned char*)[md mutableBytes];

  dLen = 0;
  for (i = 0; i < sLen; i++)
    {
      unsigned char	c = src[i];

      if (c < 32 || c > 126)
	{
	  dst[dLen] = '\\';
	  dst[dLen + 3] = (c & 7) + '0';
	  c >>= 3;
	  dst[dLen + 2] = (c & 7) + '0';
	  c >>= 3;
	  dst[dLen + 1] = (c & 7) + '0';
	  dLen += 4;
	}
      else if (c == 92)
	{
	  dst[dLen++] = '\\';
	  dst[dLen++] = '\\';
	}
      else
	{
	  dst[dLen++] = c;
	}
    }
  dst[dLen] = '\0';
  return dst;		// Owned by autoreleased NSMutableData
}

/**
 * Convert escaped sequence to raw binary data in NSData object
 */
- (NSData *) dataFromBlob: (const char *)blob
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

/**
 * Convert an NSdate into a buffer for sending to the database.
 * Return YES if the conversion fitted, NO if it was truncated.
 * The value of l is expected to be one less than the size of the buffer.
 * A nul character is appended to the bytes in the buffer.
 */
- (BOOL) dbFromDate: (NSDate*)d toBuffer: (char*)b length: (int)l
{
  NSString	*s;

  s = [d descriptionWithCalendarFormat: @"%Y-%m-%d %H:%M:%S %z"
			      timeZone: nil
				locale: nil];
  return [self dbFromString: s toBuffer: b length: l];
}

/**
 * Convert an NSString into a buffer for sending to the database.<br />
 * Return YES if the conversion fitted, NO if it was truncated.<br />
 * If s is nil, it is treated as an empty string.<br />
 * The value of l is expected to be one less than the size of the buffer
 * and must be at least 1.<br />
 * The pointer b must not be null.<br />
 * A nul character is appended to the bytes in the buffer.<br />
 * Raises an exception when passed invalid arguments.
 */
- (BOOL) dbFromString: (NSString*)s toBuffer: (char*)b length: (int)l
{
  NSData	*d;
  BOOL		ok = YES;
  unsigned	size = l;

  if (l <= 0)
    {
      [NSException raise: NSInvalidArgumentException
		  format: @"(Oracle) -%@: length too small (%d)",
	NSStringFromSelector(_cmd), l];
    }
  if (b == 0)
    {
      [NSException raise: NSInvalidArgumentException
		  format: @"(Oracle) -%@: buffer is null",
	NSStringFromSelector(_cmd)];
    }
  if (s == nil)
    {
      s = @"";
    }
  d = [s dataUsingEncoding: NSUTF8StringEncoding];
  if (l < (int)[d length])
    {
      /*
       * As the data is UTF8, we need to avoid truncating in the
       * middle of a multibyte character, so we shorten the
       * original string and reconvert to UTF8 until we find a
       * string that fits.
       */
      if ((int)[s length] > l)
	{
	  s = [s substringToIndex: l];
	  d = [s dataUsingEncoding: NSUTF8StringEncoding];
	}
      while ((int)[d length] > l)
	{
	  s = [s substringToIndex: [s length] - 1];
	  d = [s dataUsingEncoding: NSUTF8StringEncoding];
	}
      ok = NO;
    }
  size = [d length];
  memcpy(b, (const char*)[d bytes], size);
  /*
   * Pad with nuls and ensure there is a nul terminator.
   */
  while ((int)size <= l)
    {
      b[size++] = '\0';
    }
  return ok;
}

/**
 * Convert from a database character buffer to an NSDate.
 */
- (NSDate*) dbToDateFromBuffer: (char*)b length: (int)l
{
  char		buf[l+32];	/* Allow space to expend buffer. */
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
      s = [self dbToStringFromBuffer: b length: l];
      return [NSCalendarDate dateWithString: s
			     calendarFormat: @"%Y-%m-%d"
				     locale: nil];
    }
  else
    {
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
      if (i > 0)
	{
	  int	e = i;

	  if (isdigit(b[i-1]))
	    {
	      /*
	       * Make space between seconds and timezone.
	       */
	      memmove(&b[i+1], &b[i], l - i);
	      b[i++] = ' ';
	      b[++l] = '\0';
	    }

	  if (isdigit(b[i+1]) && isdigit(b[i+2]))
	    {
	      if (b[i+3] == '\0')
		{
		  // Two digit time zone
		  b[l++] = '0';
		  b[l++] = '0';
		  b[l] = '\0';
		}
	      else if (b[i+3] == ':')
		{
		  // Zone with colon before minutes
		  b[i+3] = b[i+4];
		  b[i+5] = b[i+5];
		  b[--l] = '\0';
		}
	    }

	  /* FIXME ... horrible kludge for postgres returning timestamps with
	     fractional second information. */
	  while (i-- > 0)
	    {
	      if (b[i] == '.')
		{
		  break;
		}
	    }
	  if (i > 0)
	    {
	      memmove(&b[i], &b[e], l - e);
	      l -= (e - i);
	      b[l] = '\0';
	    }
	}
      else if (l == 19)
	{
	  /* A date and time without a timezone ... assume gmt */
	  strcat(b, " +0000");
	  l += 6;
	}

      
      /* If it's a simple date (YYYY-MM-DD) append time for start of day. */
      if (l == 10)
	{
	  strcat(b, " 00:00:00 +0000");
	  l += 15;
	}
      if (l == 0)
	{
	  return nil;
	}

      s = [self dbToStringFromBuffer: b length: l];
      return [NSCalendarDate dateWithString: s
			     calendarFormat: @"%Y-%m-%d %H:%M:%S %z"
				     locale: nil];
    }
}

/**
 * Convert from a database character buffer to an NSString.
 */
- (NSString*) dbToStringFromBuffer: (char*)b length: (int)l
{
  NSData	*d;
  NSString	*s;

  /*
   * Database fields are padded to the full field size with spaces or nuls ...
   * we need to remove that padding before placing in a string.
   */
  while (l > 0 && b[l-1] <= ' ')
    {
      l--;
    }
  d = [[NSData alloc] initWithBytes: b length: l];
  s = [[NSString alloc] initWithData: d encoding: NSUTF8StringEncoding];
  RELEASE(d);
  return AUTORELEASE(s);
}

@end

