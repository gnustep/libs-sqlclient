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

static NSString	*JDBCException = @"SQLClientJDBCException";

/*
 * Cache connection information
 */
typedef struct {
  jobject	connection;
  jmethodID	commit;
  jmethodID	rollback;
  jmethodID	prepare;
  jobject	statement;
  jmethodID	executeUpdate;
  jmethodID	executeQuery;
} JInfo;


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
  JavaVMOption options[32];
  int	args = 0;
  jint result;
  JNIEnv *env;
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
      classPath = [self defaultClassPath];
      if (classPath == nil)
	{
	  classPath = @"";
	}
    }
  if (libraryPath == nil)
    {
      libraryPath = [self defaultLibraryPath];
      if (libraryPath == nil)
	{
	  libraryPath = @"";
	}
    }

  path = [NSString stringWithFormat: @"-Djava.library.path=%@", libraryPath];
  options[args].optionString = strdup([path UTF8String]);
  options[args++].extraInfo = 0;

  path = [NSString stringWithFormat: @"-Djava.class.path=%@", classPath];
  options[args].optionString = strdup([path UTF8String]);
  options[args++].extraInfo = 0;

  path = [NSString stringWithFormat: @"-Xbootclasspath/a:%@", classPath];
  options[args].optionString = strdup([path UTF8String]);
  options[args++].extraInfo = 0;

  options[args].optionString = "-verbose:class,jni";
  options[args++].extraInfo = 0;
  
  jvm_args.nOptions = args;
  jvm_args.version = JNI_VERSION_1_2;
  jvm_args.options = options;
  jvm_args.ignoreUnrecognized = JNI_FALSE;
  
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

static NSData *
NSDataFromByteArray (JNIEnv *env, jbyteArray array)
{
  NSData *returnData;
  jbyte *bytes;
  unsigned length;

  length = (*env)->GetArrayLength (env, array);

  bytes = (*env)->GetByteArrayElements (env, array, NULL);
  if (bytes == NULL)
    {
      /* OutOfMemoryError */
      return nil;
    }

  returnData = [NSData dataWithBytes: bytes  length: length];
  
  (*env)->ReleaseByteArrayElements (env, array, bytes, 0);

  return returnData;
}

static jbyteArray
ByteArrayFromNSData (JNIEnv *env, NSData *data)
{
  const jbyte *bytes;
  unsigned length;
  jbyteArray javaArray;
  
  length = [data length];
  bytes = [data bytes];

  javaArray = (*env)->NewByteArray (env, length);
  if (javaArray == NULL)
    {
      /* OutOfMemory exception thrown */
      return NULL;
    }
  
  (*env)->SetByteArrayRegion (env, javaArray, 0, length, (jbyte *)bytes);
  if ((*env)->ExceptionCheck (env))
    {
      /* No reason for this to happen - except a bug in NSData */
      return NULL;
    }
  
  return javaArray;
}



static NSString *JExceptionClear (JNIEnv *env)
{
  NSString *desc = nil;
  jthrowable exc = (*env)->ExceptionOccurred (env);

  if (exc != NULL)
    {
      static jclass java_lang_Exception = NULL;
      jmethodID jid = NULL;
      jstring jstr = NULL;

// (*env)->ExceptionDescribe (env);

      // We need to clear the exception before doing anything else. 
      (*env)->ExceptionClear (env);

      java_lang_Exception = (*env)->FindClass(env, "java/lang/Exception");
      
      if (java_lang_Exception == NULL)
	{
	  (*env)->DeleteLocalRef (env, exc);
	  (*env)->ExceptionDescribe (env); 
	  desc = @"Could not get global reference to "
	    @"java/lang/Exception to describe exception";
	  goto done;
	}

      jid = (*env)->GetMethodID (env, java_lang_Exception, "getMessage", 
				 "()Ljava/lang/String;");
      if (jid == NULL)
	{
	  (*env)->DeleteLocalRef (env, exc);
	  desc = @"Could not get the jmethodID of getMessage"
	    @"of java/lang/Exception to describe exception";
	  goto done;
	}
     
      if ((*env)->PushLocalFrame (env, 1) < 0)
	{
	  (*env)->DeleteLocalRef (env, exc);
	  (*env)->ExceptionDescribe (env);
	  desc = @"Could not create enough JNI local references "
	    @"to get a description of the exception";
	  goto done;
	}      
      
      // Get the message
      jstr = (*env)->CallObjectMethod (env, exc, jid);
      if ((*env)->ExceptionOccurred (env))
	{
	  (*env)->ExceptionDescribe (env);
	  (*env)->PopLocalFrame (env, NULL);
	  desc = @"Exception occurred while getting a description of exception";
	  goto done;
	}

      (*env)->DeleteLocalRef (env, exc);
      if ((*env)->ExceptionOccurred (env))
	{
	  (*env)->ExceptionDescribe (env);
	  (*env)->PopLocalFrame (env, NULL);
	  desc = @"Exception occurred while getting a description of exception";
	  goto done;
	}
      
      if (jstr == NULL) // Oh oh - something really wrong here
	{
	  (*env)->PopLocalFrame (env, NULL);
	  desc = @"NULL description of exception";
	  goto done;
	}
      
      desc = NSStringFromJString (env, jstr);
      if (desc == nil)
	{
	  (*env)->PopLocalFrame (env, NULL);
	  desc = @"Exception while converting string of exception";
	}

      (*env)->PopLocalFrame (env, NULL);
    }
done:
  return desc;
}

// Throw an exception if one occurred
static void JException (JNIEnv *env)
{
  NSString	*text = JExceptionClear (env);

  if (text != nil)
    {
      [NSException raise: JDBCException format:  @"%@", text];
    }
}


static NSDate* NSDateFromNSString (NSString *s)
{
  NSDate	*d;
  char		b[32];
  BOOL		milliseconds = NO;
  int		l;
  int		i;

  strcpy(b, [s UTF8String]);
  l = strlen(b);
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

      /* FIXME ... horrible kludge for timestamps with fractional
       * second information. Force it to 3 digit millisecond */
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

@interface SQLClientJDBC : SQLClient
@end

static NSDate	*future = nil;
static NSNull	*null = nil;

@implementation	SQLClientJDBC

static	int	JDBCDATE = 0;
static	int	JDBCTIME = 0;
static	int	JDBCTIMESTAMP = 0;
static	int	JDBCBOOLEAN = 0;
static	int	JDBCBLOB = 0;
static	int	JDBCBINARY = 0;
static	int	JDBCVARBINARY = 0;
static	int	JDBCLONGVARBINARY = 0;
static	int	JDBCVARCHAR = 0;

+ (void) initialize
{
  if (future == nil)
    {
      JNIEnv	*env;
      jclass	jc;
      jfieldID	jf;

      future = [NSCalendarDate dateWithString: @"9999-01-01 00:00:00 +0000"
			       calendarFormat: @"%Y-%m-%d %H:%M:%S %z"
				       locale: nil];
      RETAIN(future);
      null = [NSNull null];
      RETAIN(null);

      [SQLClientJVM startVirtualMachineWithClassPath: nil libraryPath: nil];
      env = SQLClientJNIEnv();
      jc = (*env)->FindClass(env, "java/sql/Types");
      JException (env);

      jf = (*env)->GetStaticFieldID(env, jc, "DATE", "I");
      JException (env);
      JDBCDATE = (*env)->GetStaticIntField(env, jc, jf);
      JException (env);

      jf = (*env)->GetStaticFieldID(env, jc, "TIME", "I");
      JException (env);
      JDBCTIME = (*env)->GetStaticIntField(env, jc, jf);
      JException (env);

      jf = (*env)->GetStaticFieldID(env, jc, "TIMESTAMP", "I");
      JException (env);
      JDBCTIMESTAMP = (*env)->GetStaticIntField(env, jc, jf);
      JException (env);

      jf = (*env)->GetStaticFieldID(env, jc, "BOOLEAN", "I");
      JException (env);
      JDBCBOOLEAN = (*env)->GetStaticIntField(env, jc, jf);
      JException (env);

      jf = (*env)->GetStaticFieldID(env, jc, "BLOB", "I");
      JException (env);
      JDBCBLOB = (*env)->GetStaticIntField(env, jc, jf);
      JException (env);

      jf = (*env)->GetStaticFieldID(env, jc, "BINARY", "I");
      JException (env);
      JDBCBINARY = (*env)->GetStaticIntField(env, jc, jf);
      JException (env);

      jf = (*env)->GetStaticFieldID(env, jc, "VARBINARY", "I");
      JException (env);
      JDBCVARBINARY = (*env)->GetStaticIntField(env, jc, jf);
      JException (env);

      jf = (*env)->GetStaticFieldID(env, jc, "LONGVARBINARY", "I");
      JException (env);
      JDBCLONGVARBINARY = (*env)->GetStaticIntField(env, jc, jf);
      JException (env);

      jf = (*env)->GetStaticFieldID(env, jc, "VARCHAR", "I");
      JException (env);
      JDBCVARCHAR = (*env)->GetStaticIntField(env, jc, jf);
      JException (env);

    }
}

/* Disconnect and deallocate all resources used.
 * Do NOT raise an exception.
 */
- (void) _backendDisconnect
{
  if (extra != 0)
    {
      JNIEnv	*env = SQLClientJNIEnv();
      JInfo	*ji = (JInfo*)extra;
      jclass	jc;
      jmethodID	jm;

      if ((*env)->PushLocalFrame (env, 16) >= 0)
	{
	  if (ji->statement != 0)
	    {
	      jc = (*env)->GetObjectClass(env, ji->statement);
	      jm = (*env)->GetMethodID (env, jc, "close", "()V");
	      if (jm == 0) JExceptionClear(env);
	      else (*env)->CallVoidMethod (env, ji->statement, jm);
	      if (jm == 0) JExceptionClear(env);
	      (*env)->DeleteGlobalRef (env, ji->statement);
	      if (jm == 0) JExceptionClear(env);
	    }
	  if (ji->connection != 0)
	    {
	      jc = (*env)->GetObjectClass(env, ji->connection);
	      jm = (*env)->GetMethodID (env, jc, "close", "()V");
	      if (jm == 0) JExceptionClear(env);
	      else (*env)->CallVoidMethod (env, ji->connection, jm);
	      if (jm == 0) JExceptionClear(env);
	      (*env)->DeleteGlobalRef (env, ji->connection);
	      if (jm == 0) JExceptionClear(env);
	    }
	  (*env)->PopLocalFrame (env, NULL);
	}
      NSZoneFree(NSDefaultMallocZone(), extra);
      extra = 0;
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
	      cname = [cname stringByReplacingString: @"." withString: @"/"];
	      if ((*env)->FindClass(env, [cname UTF8String]) == 0)
	        {
		  JExceptionClear (env);
		  [self debug: @"Connect to '%@' failed to load driver '%@'",
		    [self name], cname];
		  return NO;
		}

	      if ((*env)->PushLocalFrame (env, 32) < 0)
	        {
		  JExceptionClear (env);
		  [self debug: @"Connect to '%@' failed memory allocation '%@'",
		    [self name], cname];
		  return NO;
		}

	      /* Get the driver manager class.
	       */
	      jc = (*env)->FindClass(env, "java/sql/DriverManager");
	      if (jc == 0)
	        {
		  JExceptionClear (env);
		  (*env)->PopLocalFrame (env, NULL);
		  [self debug: @"Connect to '%@' failed to load DriverManager",
		    [self name]];
		  return NO;
		}

	      /* Get the method to get a connection.
	       */
	      jm = (*env)->GetStaticMethodID(env, jc, "getConnection",
		"(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)"
		"Ljava/sql/Connection;");
	      if (jm == 0)
	        {
		  JExceptionClear (env);
		  (*env)->PopLocalFrame (env, NULL);
		  [self debug: @"Connect to '%@' failed to get connect method",
		    [self name]];
		  return NO;
		}

	      /* Get the new connection object
	       */
#if 0
	      jo = (*env)->CallStaticObjectMethod(env, jc, jm, 
	        JStringFromNSString(env, url),
	        JStringFromNSString(env, [self user]),
	        JStringFromNSString(env, [self password]));
#else
jobject js1 = JStringFromNSString(env, url);
jobject js2 = JStringFromNSString(env, [self user]);
jobject js3 = JStringFromNSString(env, [self password]);
NSLog(@"CONNECT '%@', '%@', '%@'",
  NSStringFromJString(env, js1),
  NSStringFromJString(env, js2),
  NSStringFromJString(env, js3));
	      jo = (*env)->CallStaticObjectMethod(env, jc, jm, js1, js2, js3);
#endif
	      if (jo == 0)
	        {
		  JExceptionClear (env);
		  (*env)->PopLocalFrame (env, NULL);
		  [self debug: @"Connect to '%@' failed to get connection",
		    [self name]];
		  return NO;
		}

	      /* Make a reference so it can't be garbage collected.
	       */
	      jo = (*env)->NewGlobalRef(env, jo);
	      if (jo == 0)
		{
		  JExceptionClear (env);
		  (*env)->PopLocalFrame (env, NULL);
		  [self debug: @"Connect to '%@' failed to get global ref",
		    [self name]];
		  return NO;
		}
	      else
		{
		  JInfo	*ji;

		  ji = NSZoneMalloc(NSDefaultMallocZone(), sizeof(JInfo));
		  memset(ji, '\0', sizeof(*ji));
		  extra = ji;

		  NS_DURING
		    {
		      ji->connection = jo;
		      jc = (*env)->GetObjectClass(env, ji->connection);

		      /* Get the method to set autocommit.
		       */
		      jm = (*env)->GetMethodID(env, jc,
			"setAutoCommit", "(Z)V");
		      JException (env);

		      /* Turn off autocommit
		       */
		      (*env)->CallVoidMethod (env, ji->connection,
			jm, JNI_FALSE);
		      JException (env);

		      ji->commit = (*env)->GetMethodID (env, jc,
			"commit", "()V");
		      JException(env);

		      ji->rollback = (*env)->GetMethodID (env, jc,
			"rollback", "()V");
		      JException(env);

		      ji->prepare = (*env)->GetMethodID (env, jc,
			"prepareStatement",
			"(Ljava/lang/String;)Ljava/sql/PreparedStatement;");
		      JException(env);

		      jm = (*env)->GetMethodID (env, jc,
		        "createStatement",
			"()Ljava/sql/Statement;");
		      JException(env);

		      jo = (*env)->CallObjectMethod (env, ji->connection, jm);
		      JException(env);
		      ji->statement = (*env)->NewGlobalRef(env, jo);
		      JException(env);
		      jc = (*env)->GetObjectClass(env, ji->statement);

		      ji->executeUpdate = (*env)->GetMethodID (env, jc,
			"executeUpdate",
			"(Ljava/lang/String;)I");
		      JException(env);

		      ji->executeQuery = (*env)->GetMethodID (env, jc,
			"executeQuery",
			"(Ljava/lang/String;)Ljava/sql/ResultSet;");
		      JException(env);
		      (*env)->PopLocalFrame (env, NULL);
		    }
		  NS_HANDLER
		    {
		      (*env)->PopLocalFrame (env, NULL);
		      [self _backendDisconnect];
		      [self debug: @"Connect to '%@' using '%@' problem: %@",
			[self name], [self database], localException];
		      return NO;
		    }
		  NS_ENDHANDLER
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

          [self _backendDisconnect];

	  if ([self debugging] > 0)
	    {
	      [self debug: @"Disconnected client %@", [self clientName]];
	    }
	}
      NS_HANDLER
	{
          [self _backendDisconnect];
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
  NSString	*stmt = [info objectAtIndex: 0];
  JNIEnv	*env = SQLClientJNIEnv();
  JInfo		*ji = (JInfo*)extra;

  if ([stmt length] == 0)
    {
      RELEASE (arp);
      [NSException raise: NSInternalInconsistencyException
		  format: @"Statement produced null string"];
    }

  if ((*env)->PushLocalFrame (env, 32) < 0)
    {
      JExceptionClear(env);
      RELEASE (arp);
      [NSException raise: NSInternalInconsistencyException
		  format: @"No java memory for execute"];
    }

  NS_DURING
    {
      jmethodID	jm;
      jobject	js;

      /*
       * Ensure we have a working connection.
       */
      if ([self backendConnect] == NO)
	{
	  [NSException raise: SQLException
	    format: @"Unable to connect to '%@' to execute statement %@",
	    [self name], stmt];
	} 

      if ([info count] > 1)
        {
	  unsigned	i;
	  jclass	jc;

	  stmt = [stmt stringByReplacingString: @"'?'''?'" withString: @"?"];

	  js = (*env)->CallObjectMethod (env, ji->connection, ji->prepare,
	    JStringFromNSString(env, stmt));
	  JException(env);

          jc = (*env)->GetObjectClass(env, js);
	  JException(env);
	  jm = (*env)->GetMethodID (env, jc, "setBytes", "(I[B)V");
	  JException(env);

	  for (i = 1; i < [info count]; i++)
	    {
	      (*env)->CallIntMethod (env, js, jm, i,
	        ByteArrayFromNSData(env, [info objectAtIndex: i]));
	      JException(env);
	    }

	  jm = (*env)->GetMethodID (env, jc, "executeUpdate", "()I");
	  JException(env);
	  (*env)->CallIntMethod (env, js, jm);
	}
      else
	{
	  (*env)->CallIntMethod (env, ji->statement,
	    ji->executeUpdate, JStringFromNSString(env, stmt));
	}
      JException(env);
      if (_inTransaction == NO)
        {
	  // Not in a transaction ... commit at once.
	  (*env)->CallVoidMethod (env, ji->connection, ji->commit);
          JException (env);
	}
      (*env)->PopLocalFrame (env, NULL);
    }
  NS_HANDLER
    {
      if (connected == YES)
	{
	  if (_inTransaction == NO)
	    {
	      // Not in a transaction ... rollback to clear error state
	      (*env)->CallVoidMethod (env, ji->connection, ji->rollback);
	      JExceptionClear (env);
	    }
	  (*env)->PopLocalFrame (env, NULL);
	  if ([self debugging] > 0)
	    {
	      [self debug: @"Error executing statement:\n%@\n%@",
		stmt, localException];
	    }
	}
      RETAIN (localException);
      RELEASE (arp);
      AUTORELEASE (localException);
      [localException raise];
    }
  NS_ENDHANDLER
  DESTROY(arp);
}

- (NSMutableArray*) backendQuery: (NSString*)stmt
{
  NSMutableArray	*records = nil;
  CREATE_AUTORELEASE_POOL(arp);
  JNIEnv		*env = SQLClientJNIEnv();
  JInfo			*ji = (JInfo*)extra;

  if ([stmt length] == 0)
    {
      RELEASE (arp);
      [NSException raise: NSInternalInconsistencyException
		  format: @"Statement produced null string"];
    }

  if ((*env)->PushLocalFrame (env, 32) < 0)
    {
      JExceptionClear(env);
      RELEASE (arp);
      [NSException raise: NSInternalInconsistencyException
		  format: @"No java memory for query"];
    }

  NS_DURING
    {
      int	fieldCount;
      jclass	resultClass;
      jobject	result;
      jclass	metaDataClass;
      jobject	metaData;
      jmethodID	jm;

      /*
       * Ensure we have a working connection.
       */
      if ([self backendConnect] == NO)
	{
	  [NSException raise: SQLException
	    format: @"Unable to connect to '%@' to run query %@",
	    [self name], stmt];
	} 

      result = (*env)->CallObjectMethod (env, ji->statement, ji->executeQuery,
        JStringFromNSString(env, stmt));
      JException (env);
      resultClass = (*env)->GetObjectClass(env, result);
      JException (env);
      jm = (*env)->GetMethodID (env, resultClass,
        "getMetaData", "()Ljava/sql/ResultSetMetaData;");
      JException (env);
      metaData = (*env)->CallObjectMethod (env, result, jm);
      JException (env);
      metaDataClass = (*env)->GetObjectClass(env, metaData);
      JException (env);
      jm = (*env)->GetMethodID (env, metaDataClass,
        "getColumnCount", "()I");
      JException (env);
      fieldCount = (*env)->CallIntMethod (env, metaData, jm);
      JException (env);

      if (fieldCount > 0)
	{
	  NSString	*keys[fieldCount];
	  int		types[fieldCount];
	  unsigned	i;
	  jmethodID	next;
	  jmethodID	wasNull;
	  jmethodID	getBinaryStream;
	  jmethodID	getBoolean;
	  jmethodID	getBytes;
	  jmethodID	getString;

	  /* Get the names of each field
	   */
	  jm = (*env)->GetMethodID (env, metaDataClass,
	    "getColumnName", "(I)Ljava/lang/String;");
	  JException (env);
	  for (i = 0; i < fieldCount; i++)
	    {
	      jstring	js = (*env)->CallObjectMethod (env, metaData, jm, i+1);

	      JException (env);
	      keys[i] = NSStringFromJString (env, js);
	    }

	  /* Get the types of each field.
	   * We treat most as strings.
	   */
	  jm = (*env)->GetMethodID (env, metaDataClass,
	    "getColumnType", "(I)I");
	  JException (env);
	  for (i = 0; i < fieldCount; i++)
	    {
	      int	v = (*env)->CallIntMethod (env, metaData, jm, i+1);

	      if (v == JDBCDATE || v == JDBCTIME || v == JDBCTIMESTAMP)
	        {
		  types[i] = JDBCTIMESTAMP;
		}
	      else if (v == JDBCBOOLEAN)
	        {
		  types[i] = JDBCBOOLEAN;
		}
	      else if (v == JDBCBLOB || v == JDBCBINARY || v == JDBCVARBINARY
	        || v == JDBCLONGVARBINARY)
	        {
		  types[i] = JDBCBLOB;
		}
	      else
	        {
		  types[i] = JDBCVARCHAR;
		}
	    }

          /* Iterate through the result set
	   */
	  wasNull = (*env)->GetMethodID (env, resultClass,
	    "wasNull", "()Z");
	  JException (env);
	  getBinaryStream = (*env)->GetMethodID (env, resultClass,
	    "getBinaryStream", "(I)Ljava/io/InputStream;");
	  JException (env);
	  getBoolean = (*env)->GetMethodID (env, resultClass,
	    "getBoolean", "(I)Z");
	  JException (env);
	  getBytes = (*env)->GetMethodID (env, resultClass,
	    "getBytes", "(I)[B");
	  JException (env);
	  getString = (*env)->GetMethodID (env, resultClass,
	    "getString", "(I)Ljava/lang/String;");
	  JException (env);

	  next = (*env)->GetMethodID (env, resultClass,
	    "next", "()Z");
	  JException (env);
	  records = [[NSMutableArray alloc] initWithCapacity: 100];
	  while ((*env)->CallBooleanMethod (env, result, next) == JNI_TRUE)
	    {
	      SQLRecord	*record;
	      id	values[fieldCount];
	      int	j;

	      if ((*env)->PushLocalFrame (env, fieldCount * 2) < 0)
		{
		  JExceptionClear(env);
		  RELEASE (arp);
		  [NSException raise: NSInternalInconsistencyException
			      format: @"No java memory for query"];
		}
	      NS_DURING
		{
		  for (j = 0; j < fieldCount; j++)
		    {
		      id		v = null;

		      if (types[j] == JDBCBOOLEAN)
			{
			  BOOL	b = NO;

			  if ((*env)->CallBooleanMethod (env, result,
			    getBoolean, j+1) == JNI_TRUE)
			    {
			      b = YES;
			    }
			  JException (env);
			  if ((*env)->CallBooleanMethod (env, result,
			    wasNull) == JNI_FALSE)
			    {
			      if (b == YES)
				{
				  v = @"Y";
				}
			      else
				{
				  v = @"N";
				}
			    }
			  JException (env);
			}
		      else if (types[j] == JDBCTIMESTAMP)
			{
			  jobject	jo;

			  jo = (*env)->CallObjectMethod (env, result,
			    getString, j+1);
			  JException (env);
			  if ((*env)->CallBooleanMethod (env, result,
			    wasNull) == JNI_FALSE)
			    {
			      v = NSStringFromJString(env, jo);
			      v = NSDateFromNSString(v);
			    }
			  JException (env);
			}
		      else if (types[j] == JDBCBLOB)
			{
			  jbyteArray	jo;

			  jo = (*env)->CallObjectMethod (env, result,
			    getBytes, j+1);
			  JException (env);
			  if ((*env)->CallBooleanMethod (env, result,
			    wasNull) == JNI_FALSE)
			    {
			      v = NSDataFromByteArray(env, jo);
			    }
			  JException (env);
			}
		      else
			{
			  jobject	jo;

			  jo = (*env)->CallObjectMethod (env, result,
			    getString, j+1);
			  JException (env);
			  if ((*env)->CallBooleanMethod (env, result,
			    wasNull) == JNI_FALSE)
			    {
			      v = NSStringFromJString(env, jo);
			    }
			  JException (env);
			}
		      values[j] = v;
		    }
		  (*env)->PopLocalFrame (env, NULL);
		}
	      NS_HANDLER
	        {
		  (*env)->PopLocalFrame (env, NULL);
		  [localException raise];
		}
	      NS_ENDHANDLER
	      record = [SQLRecord newWithValues: values
					   keys: keys
					  count: fieldCount];
	      [records addObject: record];
	      RELEASE(record);
	    }
	}
      else
	{
	  records = [NSMutableArray array];
	}
      (*env)->PopLocalFrame (env, NULL);
    }
  NS_HANDLER
    {
      NSString	*n = [localException name];

      if (connected == YES)
	{
	  (*env)->PopLocalFrame (env, NULL);
	  if ([n isEqual: SQLConnectionException] == YES) 
	    {
	      [self backendDisconnect];
	    }
	  if ([self debugging] > 0)
	    {
	      [self debug: @"Error executing statement:\n%@\n%@",
		stmt, localException];
	    }
	}
      DESTROY(records);
      RETAIN (localException);
      RELEASE (arp);
      AUTORELEASE (localException);
      [localException raise];
    }
  NS_ENDHANDLER
  DESTROY(arp);
  return AUTORELEASE(records);
}

- (void) begin
{
  [lock lock];
  if (_inTransaction == NO)
    {
      _inTransaction = YES;
      // Leave us locked so the transaction can't be interfered with
    }
  else
    {
      [lock unlock];
      [NSException raise: NSInternalInconsistencyException
		  format: @"begin used inside transaction"];
    }
}

- (void) commit
{
  [lock lock];
  if (_inTransaction == NO)
    {
      [lock unlock];
      [NSException raise: NSInternalInconsistencyException
		  format: @"commit used outside transaction"];
    }
  NS_DURING
    {
      JNIEnv	*env = SQLClientJNIEnv();
      JInfo	*ji = (JInfo*)extra;

      (*env)->CallVoidMethod (env, ji->connection, ji->commit);
      JException(env);
      _inTransaction = NO;
      [lock unlock];		// Locked at start of -commit
      [lock unlock];		// Locked by -begin
    }
  NS_HANDLER
    {
      _inTransaction = NO;
      [lock unlock];		// Locked at start of -commit
      [lock unlock];		// Locked by -begin
      [localException raise];
    }
  NS_ENDHANDLER
}

- (NSString*) quoteString: (NSString *)s
{
  static NSCharacterSet	*special = nil;
  NSMutableString	*m;
  NSRange		r;
  unsigned		l;

  if (special == nil)
    {
      NSString	*stemp;

      /*
       * NB. length of C string is 3, so we include a nul character as a
       * special.
       */
      stemp = [[NSString alloc] initWithBytes: "'\\"
	 			       length: 3
		 		     encoding: NSASCIIStringEncoding];
      special = [NSCharacterSet characterSetWithCharactersInString: stemp];
      RELEASE(stemp);
      RETAIN(special);
    }

  /*
   * Step through string removing nul characters
   * and escaping quote characters as required.
   */
  m = AUTORELEASE([s mutableCopy]);
  l = [m length];
  r = NSMakeRange(0, l);
  r = [m rangeOfCharacterFromSet: special options: NSLiteralSearch range: r];
  while (r.length > 0)
    {
      unichar	c = [m characterAtIndex: r.location];

      if (c == 0)
	{
	  r.length = 1;
	  [m replaceCharactersInRange: r withString: @""];
	  l--;
	}
      else if (c == '\\')
        {
	  r.length = 0;
	  [m replaceCharactersInRange: r withString: @"\\"];
	  l++;
	  r.location += 2;
	}
      else
        {
	  r.length = 0;
	  [m replaceCharactersInRange: r withString: @"'"];
	  l++;
	  r.location += 2;
        }
      r = NSMakeRange(r.location, l - r.location);
      r = [m rangeOfCharacterFromSet: special
			     options: NSLiteralSearch
			       range: r];
    }

  /* Add quoting around it.  */
  [m replaceCharactersInRange: NSMakeRange(0, 0) withString: @"'"];
  [m appendString: @"'"];
  return m;
}

- (void) rollback
{
  [lock lock];
  if (_inTransaction == YES)
    {
      _inTransaction = NO;
      NS_DURING
	{
	  JNIEnv	*env = SQLClientJNIEnv();
	  JInfo		*ji = (JInfo*)extra;

	  (*env)->CallVoidMethod (env, ji->connection, ji->rollback);
	  JException(env);
	  [lock unlock];		// Locked at start of -rollback
	  [lock unlock];		// Locked by -begin
	}
      NS_HANDLER
	{
	  [lock unlock];		// Locked at start of -rollback
	  [lock unlock];		// Locked by -begin
	  [localException raise];
	}
      NS_ENDHANDLER
    }
}

- (void) dealloc
{
  [self backendDisconnect];
  [super dealloc];
}

@end

