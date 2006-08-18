/* -*-objc-*- */

/** Implementation of SQLClientJDBC for GNUStep
   Copyright (C) 2006 Free Software Foundation, Inc.
   
   Written by:  Richard Frith-Macdonald <rfm@gnu.org>
   Date:	August 2006
   
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

   $Date: 2006-06-04 10:19:28 +0100 (Sun, 04 Jun 2006) $ $Revision: 23028 $
   */ 

#include	<Foundation/NSAutoreleasePool.h>
#include	<Foundation/NSCalendarDate.h>
#include	<Foundation/NSCharacterSet.h>
#include	<Foundation/NSData.h>
#include	<Foundation/NSDate.h>
#include	<Foundation/NSException.h>
#include	<Foundation/NSLock.h>
#include	<Foundation/NSMapTable.h>
#include	<Foundation/NSNotification.h>
#include	<Foundation/NSNotification.h>
#include	<Foundation/NSNull.h>
#include	<Foundation/NSProcessInfo.h>
#include	<Foundation/NSString.h>
#include	<Foundation/NSThread.h>
#include	<Foundation/NSUserDefaults.h>
#include	<Foundation/NSValue.h>

#include	"config.h"
#include	"SQLClient.h"

#include	<jni.h>

/* SQLClientJVM shamelessly stolen from JIGS ... written by Nicola Pero
 * and copyright the Free Software Foundation.
 */

@interface SQLClientJVM : NSObject
{
}
+ (void) startVirtualMachineWithClassPath: (NSString *)classPath
			      libraryPath: (NSString *)libraryPath;
+ (void) destroyVirtualMachine;
+ (BOOL) isVirtualMachineRunning;
+ (NSString *) defaultClassPath;
+ (NSString *) defaultLibraryPath;
+ (void) attachCurrentThread;
+ (void) detachCurrentThread;
+ (void) registerJavaVM: (JavaVM *)javaVMHandle;
@end

/*
 * A fast function to get the (JNIEnv *) variable.
 */
static JNIEnv *SQLClientJNIEnv ();

static JavaVM *SQLClientJavaVM = NULL;

/*
 * Return the (JNIEnv *) associated with the current thread,
 * or NULL if no java virtual machine is running (or if the thread 
 * is not attached to the JVM).
 *
 * NB: This function performs a call.  Better use your (JNIEnv *) if 
 * you already have it.
 *
 */
JNIEnv *SQLClientJNIEnv ()
{
  JNIEnv *penv;

  if ((*SQLClientJavaVM)->GetEnv (SQLClientJavaVM, (void **)&penv, 
			     JNI_VERSION_1_2) == JNI_OK)
    {
      return penv;
    }
  else
    {
      return NULL;
    }
}

@implementation SQLClientJVM (GNUstepInternals)
+ (void) _attachCurrentThread: (NSNotification *)not
{
  [self attachCurrentThread];
}

+ (void) _detachCurrentThread: (NSNotification *)not
{
  [self detachCurrentThread];
}
@end

@implementation SQLClientJVM

+ (void) startVirtualMachineWithClassPath: (NSString *)classPath
			      libraryPath: (NSString *)libraryPath
{
  JavaVMInitArgs jvm_args;
  JavaVMOption options[2];
  jint result;
  JNIEnv *env;
  NSDictionary *environment = [[NSProcessInfo processInfo] environment];
  NSString *path;
  NSNotificationCenter *nc = [NSNotificationCenter defaultCenter];

  if (SQLClientJavaVM != NULL)
    {
      [NSException raise: NSGenericException
		   format: @"Only one Java Virtual Machine "
		   @"can be running at each time"];
    }  

  // If we don't pass these options, it assumes they are really @""
  if (classPath == nil)
    {
      classPath = [environment objectForKey: @"CLASSPATH"];
      if (classPath == nil)
	{
	  classPath = @"";
	}
    }
  if (libraryPath == nil)
    {
      libraryPath = [environment objectForKey: @"LD_LIBRARY_PATH"];
      if (libraryPath == nil)
	{
	  libraryPath = @"";
	}
    }

  jvm_args.nOptions = 2;
  
  path = [NSString stringWithFormat: @"-Djava.class.path=%@", 
		   classPath];
  options[0].optionString = (char *)[path cString];

  path = [NSString stringWithFormat: @"-Djava.library.path=%@", 
		   libraryPath];
  options[1].optionString = (char *)[path cString];
  
  jvm_args.version = 0x00010002;
  jvm_args.options = options;
  jvm_args.ignoreUnrecognized = JNI_TRUE;
  
  result = JNI_CreateJavaVM (&SQLClientJavaVM, (void **)&env, &jvm_args);
  
  if (result < 0)
    {
      [NSException raise: NSGenericException
		   format: @"Could not start Java Virtual Machine"];
    }

  /* Whenever a thread start or ends, we want to automatically attach
     or detach it to/from the JVM */
  [nc addObserver: self  selector: @selector (_attachCurrentThread:)
      name: NSThreadDidStartNotification  object: nil];

  [nc addObserver: self selector: @selector (_detachCurrentThread:)
      name: NSThreadWillExitNotification  object: nil];

  return;
}

+ (void) destroyVirtualMachine
{
  jint result;
  
  if (SQLClientJavaVM == NULL)
    {
      [NSException raise: NSGenericException
		   format: @"destroyJVM called without a JVM running"];
    }
  result = (*SQLClientJavaVM)->DestroyJavaVM (SQLClientJavaVM);
  if (result < 0)
    {
      [NSException raise: NSGenericException
		   format: @"Could not destroy Java Virtual Machine"];
    }
  else
    {
      SQLClientJavaVM = NULL;
    }
}

+ (BOOL) isVirtualMachineRunning
{
  if (SQLClientJavaVM == NULL)
    {
      return NO;
    }
  else
    {
      return YES;
    }
}

+ (NSString *) defaultClassPath
{
  NSDictionary *environment = [[NSProcessInfo processInfo] environment];

  return [environment objectForKey: @"CLASSPATH"];
}

+ (NSString *) defaultLibraryPath
{
  NSDictionary *environment = [[NSProcessInfo processInfo] environment];

  return [environment objectForKey: @"LD_LIBRARY_PATH"];
}

+ (void) attachCurrentThread
{
  static int count = 0;
  JNIEnv *env;
  JavaVMAttachArgs args;
  jint result;

  if (SQLClientJavaVM == NULL)
    {
      /* No JVM - nothing to do */
      return;
    }  
                    
  if (SQLClientJNIEnv () != NULL)
    {
      /* The thread is already attached */
      return;
    }

  {
    CREATE_AUTORELEASE_POOL (pool);
    
    args.version = JNI_VERSION_1_2;
    args.name = (char *)[[NSString stringWithFormat:
      @"GNUstepThread-%d", count] cString];
    args.group = NULL;
    
    result = (*SQLClientJavaVM)->AttachCurrentThread
      (SQLClientJavaVM, (void **)&env, &args);
    
    RELEASE (pool);
  }

  if (result < 0)
    {
      [NSException raise: NSGenericException
		   format: @"Could not attach thread to the Java VM"];
    }
  
  count++;
  if (count > 100000)
    {
      /* Duplicated names shouldn't cause any problem */
      count = 0;
    }
  return;
}

+ (void) detachCurrentThread 
{
  jint result;

  if (SQLClientJavaVM == NULL)
    {
      /* No JVM - nothing to do */
      return;
    }  
                    
  if (SQLClientJNIEnv () == NULL)
    {
      /* The thread is not attached */
      return;
    }

  result = (*SQLClientJavaVM)->DetachCurrentThread (SQLClientJavaVM);
  
  if (result < 0)
    {
      [NSException raise: NSGenericException
		   format: @"Could not detach thread from the Java VM"];
    }

  return;
}

+ (void) registerJavaVM: (JavaVM *)javaVMHandle
{
  if (javaVMHandle == NULL)
    {
      [NSException raise: NSInvalidArgumentException
		   format: @"Trying to register a NULL Java VM"];
    }
  
  if (SQLClientJavaVM != NULL)
    {
      if (javaVMHandle == SQLClientJavaVM)
	{
	  return;
	}
      else
	{
	  [NSException raise: NSGenericException
		       format: @"Trying to register a Java VM "
		       @"while one is already running"];
	}
    }  
  
  SQLClientJavaVM = javaVMHandle;
  
  // Safety check.  If javaVMHandle is invalid, the following will crash 
  // your app.  The app would crash anyway later on, so it's better to crash 
  // it here, where it is easier to debug.
  SQLClientJNIEnv ();
  
  return;
}

@end

static jstring
JStringFromNSString (JNIEnv *env, NSString *string)
{
  jstring javaString;
  int length = [string length];

  /* We allocate strings of up to 10k on the stack - others using
     malloc.  */
  if (length < 10000)
    {
      unichar uniString[length];
      
      // Get a unicode representation of the string in the buffer
      [string getCharacters: uniString];
      
      // Create a java string using the buffer
      javaString = (*env)->NewString (env, uniString, length);
      // NB: if javaString is NULL, an exception has been thrown.
    }
  else
    {
      unichar *uniString;

      uniString = malloc (sizeof (unichar) * length);
      [string getCharacters: uniString];
      javaString = (*env)->NewString (env, uniString, length);
      free (uniString);
    }

  return javaString;
}

static NSString*
NSStringFromJString (JNIEnv *env, jstring string)
{
  unichar *uniString;
  jsize length;
  NSString *gnustepString;

  // Get a Unicode string from the jstring
  uniString = (unichar *)(*env)->GetStringChars (env, string, NULL);
  if (uniString == NULL)
    {
      // OutOfMemoryError thrown
      return NULL;
    }
  
  // Get the Unicode string length
  length = (*env)->GetStringLength (env, string);
  
  // Create a GNUstep string from the Unicode string
  gnustepString = [NSString stringWithCharacters: uniString length: length];
  
  // Release the temporary string
  (*env)->ReleaseStringChars (env, string, uniString);
  
  return gnustepString;
}


@interface SQLClientJDBC : SQLClient
@end

@interface	SQLClientJDBC(Embedded)
- (NSData*) dataFromBLOB: (const char *)blob;
- (NSDate*) dbToDateFromBuffer: (char*)b length: (int)l;
@end

static NSDate	*future = nil;
static NSNull	*null = nil;

@implementation	SQLClientJDBC

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

      [SQLClientJVM startVirtualMachineWithClassPath: nil libraryPath: nil];
    }
}

- (BOOL) backendConnect
{
  if (extra == 0)
    {
      connected = NO;
      if ([self database] != nil)
	{
	  NSString		*dbase = [self database];
	  NSRange		r;

	  [[self class] purgeConnections: nil];

	  r = [dbase rangeOfString: @":"];
	  if (r.length > 0)
	    {
	      NSString		*url;
	      NSString		*cname;
	      JNIEnv		*env;
	      jclass		jc;
	      jmethodID		jm;
	      jobject		jo;

	      url = [dbase substringFromIndex: NSMaxRange(r)];
	      cname = [dbase substringToIndex: r.location];

	      env = SQLClientJNIEnv();
	      if (env == 0)
	        {
		  [self debug: @"Connect to '%@' failed to set up Java runtime",
		    [self name]];
		  return NO;
		}

	      /* Ensure the driver for the database is loaded.
	       */
	      jc = (*env)->FindClass(env, [cname UTF8String]);
	      if (jc == 0)
	        {
		  [self debug: @"Connect to '%@' failed to load driver '%@'",
		    [self name], cname];
		  return NO;
		}

	      /* Get the driver manager class.
	       */
	      jc = (*env)->FindClass(env, "java.sql.DriverManager");
	      if (jc == 0)
	        {
		  [self debug: @"Connect to '%@' failed to load DriverManager",
		    [self name]];
		  return NO;
		}

	      /* Get the method to get a connection.
	       */
	      jm = (*env)->GetStaticMethodID(env, jc, "getConnection",
		"(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)"
		"Ljava/sql/Connection");
	      if (jm == 0)
	        {
		  [self debug: @"Connect to '%@' failed to get connect method",
		    [self name]];
		  return NO;
		}

	      /* Get the new connection object
	       */
	      jo = (void*)(*env)->CallStaticObjectMethod(env, jc, jm, 
	        JStringFromNSString(env, url),
	        JStringFromNSString(env, [self user]),
	        JStringFromNSString(env, [self password]));
	      if (jo == 0)
	        {
		  [self debug: @"Connect to '%@' failed to get connection",
		    [self name]];
		  return NO;
		}

	      /* Make a reference so it can't be garbage collected.
	       */
	      extra = (void*)(*env)->NewGlobalRef(env, jo)
	      if (extra == 0)
		{
		  [self debug: @"Connect to '%@' failed to get global ref",
		    [self name]];
		  return NO;
		}
	      else
		{
		  connected = YES;
		}
	    }
	  else
	    {
	      [self debug: @"Connect to '%@' using '%@' has no class",
		[self name], [self database]];
	      return NO;
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
#if 0
  if (extra != 0)
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
	  PQfinish(extra);
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
#endif
}

- (void) backendExecute: (NSArray*)info
{
#if 0
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

      result = PQexec(extra, statement);
      if (result == 0 || PQresultStatus(result) == PGRES_FATAL_ERROR)
	{
	  NSString	*str;
	  const char	*cstr;

	  if (result == 0)
	    {
	      cstr = PQerrorMessage(extra);
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
  #endif
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
  NSMutableArray	*records = nil;
#if 0
  CREATE_AUTORELEASE_POOL(arp);
  PGresult		*result = 0;

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
      result = PQexec(extra, statement);
      if (result == 0 || PQresultStatus(result) == PGRES_FATAL_ERROR)
	{
	  NSString	*str;
	  const char	*cstr;

	  if (result == 0)
	    {
	      cstr = PQerrorMessage(extra);
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
#endif
  return AUTORELEASE(records);
}

- (unsigned) copyEscapedBLOB: (NSData*)blob into: (void*)buf
{
  unsigned		length = 0;
#if 0
  const unsigned char	*src = [blob bytes];
  unsigned		sLen = [blob length];
  unsigned char		*ptr = (unsigned char*)buf;
  unsigned		i;

  ptr[length++] = '\'';
  for (i = 0; i < sLen; i++)
    {
      unsigned char	c = src[i];

      if (c < 32 || c > 126)
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
	  if (standardEscaping == NO)
	    {
	      ptr[length++] = '\\';
	      ptr[length++] = '\\';
	    }
	  ptr[length++] = '\\';
	  ptr[length++] = '\\';
	}
      else if (c == '\'')
	{
	  ptr[length++] = '\'';
	  ptr[length++] = '\'';
	}
      else
	{
	  ptr[length++] = c;
	}
    }
  ptr[length++] = '\'';
#endif
  return length;
}

- (unsigned) lengthOfEscapedBLOB: (NSData*)blob
{
  unsigned int	sLen = [blob length];
  unsigned int	length = sLen + 2;
#if 0
  unsigned char	*src = (unsigned char*)[blob bytes];
  unsigned int	i;

  for (i = 0; i < sLen; i++)
    {
      unsigned char	c = src[i];

      if (c < 32 || c > 126)
	{
	  length += 4;
	}
      else if (c == '\\')
	{
	  if (standardEscaping == NO)
	    {
	      length += 2;
	    }
	  length += 1;
	}
      else if (c == '\'')
	{
	  length += 1;
	}
    }
#endif
  return length;
}

- (NSData *) dataFromBLOB: (const char *)blob
{
  NSMutableData	*md = nil;
#if 0
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
#endif
  return md;
}

- (NSDate*) dbToDateFromBuffer: (char*)b length: (int)l
{
  NSDate	*d = nil;
#if 0
  char		buf[l+32];	/* Allow space to expand buffer. */
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
      d = [NSCalendarDate dateWithString: s
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
    }
#endif
  return d;
}

- (void) dealloc
{
  [self backendDisconnect];
  [super dealloc];
}

- (NSString*) quoteString: (NSString *)s
{
#if 0
  NSData	*d = [s dataUsingEncoding: NSUTF8StringEncoding];
  unsigned	l = [d length];
  unsigned char	*to = NSZoneMalloc(NSDefaultMallocZone(), (l * 2) + 3);

#ifdef	HAVE_PQESCAPESTRINGCONN
  int		err;

  [self backendConnect];
  l = PQescapeStringConn(extra, (char*)(to + 1), [d bytes], l, &err);
#else
  l = PQescapeString(to + 1, [d bytes], l);
#endif
  to[0] = '\'';
  to[l + 1] = '\'';
  s = [[NSString alloc] initWithBytesNoCopy: to
				     length: l + 2
				   encoding: NSUTF8StringEncoding
			       freeWhenDone: YES];
  s = AUTORELEASE(s);
#endif
  return s;
}

@end

