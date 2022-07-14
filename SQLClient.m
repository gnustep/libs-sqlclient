/* -*-objc-*- */

/** Implementation of SQLClient for GNUStep
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

#import	<Foundation/NSArray.h>
#import	<Foundation/NSAutoreleasePool.h>
#import	<Foundation/NSBundle.h>
#import	<Foundation/NSCalendarDate.h>
#import	<Foundation/NSCharacterSet.h>
#import	<Foundation/NSData.h>
#import	<Foundation/NSDate.h>
#import	<Foundation/NSDebug.h>
#import	<Foundation/NSDictionary.h>
#import	<Foundation/NSEnumerator.h>
#import	<Foundation/NSException.h>
#import	<Foundation/NSKeyValueCoding.h>
#import	<Foundation/NSLock.h>
#import	<Foundation/NSHashTable.h>
#import	<Foundation/NSMapTable.h>
#import	<Foundation/NSNotification.h>
#import	<Foundation/NSNull.h>
#import	<Foundation/NSObjCRuntime.h>
#import	<Foundation/NSPathUtilities.h>
#import	<Foundation/NSProcessInfo.h>
#import	<Foundation/NSRunLoop.h>
#import	<Foundation/NSSet.h>
#import	<Foundation/NSString.h>
#import	<Foundation/NSThread.h>
#import	<Foundation/NSTimer.h>
#import	<Foundation/NSUserDefaults.h>
#import	<Foundation/NSValue.h>

#import	<Performance/GSCache.h>
#import	<Performance/GSTicker.h>

#define SQLCLIENT_PRIVATE                       @public
#define SQLCLIENT_COMPILE_TIME_QUOTE_CHECK      1

#include	<memory.h>

#include	"SQLClient.h"

#if	defined(GNUSTEP_BASE_LIBRARY)
#define	SUBCLASS_RESPONSIBILITY	[self subclassResponsibility: _cmd];
#else
#define	SUBCLASS_RESPONSIBILITY	
#endif

NSString * const SQLClientDidConnectNotification
 = @"SQLClientDidConnectNotification";

NSString * const SQLClientDidDisconnectNotification
 = @"SQLClientDidDisconnectNotification";

static unsigned int	classDebugging = 0;
static NSTimeInterval	classDuration = -1;

static NSNull	*null = nil;
static NSArray	*queryModes = nil;
static NSThread	*mainThread = nil;
static Class	NSStringClass = Nil;
static Class	NSArrayClass = Nil;
static Class	NSDateClass = Nil;
static Class	NSSetClass = Nil;
static Class	SQLClientClass = Nil;
static Class	LitProxyClass = Nil;
static Class	LitStringClass = Nil;
static Class	TinyStringClass = Nil;
static Class	SQLStringClass = Nil;
static unsigned SQLStringSize = 0;

static BOOL     autoquote = NO;
static BOOL     autoquoteWarning = NO;

static BOOL
isByteCoding(NSStringEncoding encoding)
{
  if (NSASCIIStringEncoding == encoding
    || NSNEXTSTEPStringEncoding == encoding
    || NSISOLatin1StringEncoding == encoding
    || NSISOLatin2StringEncoding == encoding
    || NSISOThaiStringEncoding == encoding
    || NSISOLatin9StringEncoding == encoding)
    {
      return YES;
    }
  return NO;
}

/* Determine the length of the UTF-8 string as a unicode (UTF-16) string.
 * sets the ascii flag according to the content found.
 */
static NSUInteger
lengthUTF8(const uint8_t *p, unsigned l, BOOL *ascii, BOOL *latin1)
{
  const uint8_t	*e = p + l;
  BOOL		a = YES;
  BOOL		l1 = YES;

  l = 0;
  while (p < e)
    {
      uint8_t	c = *p;
      uint32_t	u = c;

      if (c > 0x7f)
	{
	  int i, sle = 0;

	  a = NO;
	  /* calculated the expected sequence length */
	  while (c & 0x80)
	    {
	      c = c << 1;
	      sle++;
	    }

	  /* legal ? */
	  if ((sle < 2) || (sle > 6))
	    {
	      [NSException raise: NSInternalInconsistencyException
			  format: @"Bad sequence length in constant string"];
	    }

	  if (p + sle > e)
	    {
	      [NSException raise: NSInternalInconsistencyException
			  format: @"Short data in constant string"];
	    }

	  /* get the codepoint */
	  for (i = 1; i < sle; i++)
	    {
	      if (p[i] < 0x80 || p[i] >= 0xc0)
		break;
	      u = (u << 6) | (p[i] & 0x3f);
	    }

	  if (i < sle)
	    {
	      [NSException raise: NSInternalInconsistencyException
			  format: @"Codepoint out of range in constant string"];
	    }
	  u = u & ~(0xffffffff << ((5 * sle) + 1));
	  p += sle;

	  /*
	   * We check for invalid codepoints here.
	   */
	  if (u > 0x10ffff)
	    {
	      [NSException raise: NSInternalInconsistencyException
			  format: @"Codepoint invalid in constant string"];
	    }

	  if ((u >= 0xd800) && (u <= 0xdfff))
	    {
	      [NSException raise: NSInternalInconsistencyException
			  format: @"Bad surrogate pair in constant string"];
	    }
	}
      else
	{
	  p++;
	}

      /*
       * Add codepoint as either a single unichar for BMP
       * or as a pair of surrogates for codepoints over 16 bits.
       */
      if (u < 0x10000)
	{
	  l++;
	  if (u > 255)
	    {
	      l1 = NO;
	    }
	}
      else
	{
	  l += 2;
          l1 = NO;
	}
    }
  if (0 != ascii)
    {
      *ascii = a;
    }
  if (0 != latin1)
    {
      *latin1 = l1;
    }
  return l;
}

/*
 */
@interface SQLString: SQLLiteral
{
@public
  NSUInteger    hash;
  BOOL          hasHash;
  BOOL          ascii;
  BOOL          latin1;
  const uint8_t *utf8Bytes;
  NSUInteger    byteLen;
  NSUInteger    charLen;
}
@end

@interface SQLLiteralProxy: SQLLiteral
{
@public
  NSString      *content;
}
@end

BOOL
SQLClientIsLiteral(NSString *aString)
{
  if (nil != aString)
    {
      Class c = object_getClass(aString);

      if (c == LitStringClass || c == TinyStringClass
        || c == SQLStringClass || c == LitProxyClass)
        {
          return YES;
        }
    }
  return NO;
}

SQLLiteral *
SQLClientNewLiteral(const char *bytes, unsigned count)
{
  SQLString     *s;
  uint8_t       *p;

  s = NSAllocateObject(SQLStringClass, count+1, NSDefaultMallocZone());
  s->utf8Bytes = p = ((uint8_t*)(void*)s) + SQLStringSize;
  s->byteLen = count;
  memcpy(p, bytes, count);
  p[count] = '\0';
  s->charLen = lengthUTF8(s->utf8Bytes, s->byteLen, &s->ascii, &s->latin1);
  return s;
}

SQLLiteral *
SQLClientCopyLiteral(NSString *aString)
{
  if (nil != aString)
    {
      Class c = object_getClass(aString);

      if (c == LitProxyClass)
        {
          aString = ((SQLLiteralProxy*)aString)->content;
          c = object_getClass(aString);
        }
      if (c != LitStringClass && c != TinyStringClass && c != SQLStringClass)
        {
	  /* The SQLString class uses utf8 and can be very inefficient
	   * if it's too long.  For long strings we use a proxy instead.
	   */
	  if ([aString length] > 64)
	    {
	      SQLLiteralProxy  *l;

	      l = (SQLLiteralProxy*)
		NSAllocateObject(LitProxyClass, 0, NSDefaultMallocZone());
	      l->content = [aString copy];
	      aString = l;
	    }
	  else
	    {
	      const char    *p = [aString UTF8String];
	      int           l = strlen(p);

	      aString = SQLClientNewLiteral(p, l);
	    }
        }
      else
        {
          aString = [aString copy];
        }
    }
  return (SQLLiteral*)aString;
}

SQLLiteral *
SQLClientMakeLiteral(NSString *aString)
{
  if (nil != aString)
    {
      Class c = object_getClass(aString);

      if (c == LitProxyClass)
        {
          aString = ((SQLLiteralProxy*)aString)->content;
          c = object_getClass(aString);
        }
      if (c != LitStringClass && c != TinyStringClass && c != SQLStringClass)
        {
	  /* The SQLString class uses utf8 and can be very inefficient
	   * if it's too long.  For long strings we use a proxy instead.
	   */
	  if ([aString length] > 64)
	    {
	      SQLLiteralProxy  *l;

	      l = (SQLLiteralProxy*)
		NSAllocateObject(LitProxyClass, 0, NSDefaultMallocZone());
	      l->content = [aString copy];
	      return (SQLLiteral*)[l autorelease];  
	    }
	  else
	    {
	      const char    *p = [aString UTF8String];
	      int           l = strlen(p);
	      NSString      *s = SQLClientNewLiteral(p, l);

	      aString = [s autorelease];
	    }
        }
    }
  return (SQLLiteral*)aString;
}

SQLLiteral *
SQLClientProxyLiteral(NSString *aString)
{
  if (nil == aString)
    {
      return nil;
    }
  else if (SQLClientIsLiteral(aString))
    {
      return (SQLLiteral*)[[aString retain] autorelease];
    }
  else if (NO == [aString isKindOfClass: NSStringClass])
    {
      [NSException raise: NSInvalidArgumentException
                  format: @"Attempt to cast non-string to SQLLiteral"];
      return nil;
    }
  else
    {
      SQLLiteralProxy  *l;

      l = (SQLLiteralProxy*)
        NSAllocateObject(LitProxyClass, 0, NSDefaultMallocZone());
      l->content = [aString retain];
      return (SQLLiteral*)[l autorelease];  
    }
}

NSString *
SQLClientUnProxyLiteral(id aString)
{
  if (nil != aString)
    {
      Class	c = object_getClass(aString);

      if (c == LitProxyClass)
	{
	  aString = ((SQLLiteralProxy*)aString)->content;
	}
      else if (c != LitStringClass
        && c != TinyStringClass
        && c != SQLStringClass)
	{
	  aString = [aString description];
	  if (YES == autoquoteWarning)
	    {
	      NSLog(@"SQLClient expected SQLLiteral type for %@ (%@)",
		aString, NSStringFromClass(c));
	    }
	}
    }
  return (NSString*)aString;
}

static SQLLiteral *
quoteBigInteger(int64_t i)
{
  char          buf[32];
  unsigned      len;
  SQLLiteral    *s;

  len = sprintf(buf, "%"PRId64, i);
  s = SQLClientNewLiteral(buf, len);
  return [s autorelease];
}

@interface      SQLClientPool (Swallow)
- (BOOL) _swallowClient: (SQLClient*)client explicit: (BOOL)swallowed;
@end
@interface      SQLTransaction (Creation)
+ (SQLTransaction*) _transactionUsing: (id)clientOrPool
                                batch: (BOOL)isBatched
                                 stop: (BOOL)stopOnFailure;
@end

@implementation SQLLiteral
+ (id) allocWithZone: (NSZone*)z
{
  [NSException raise: NSInternalInconsistencyException
              format: @"Illegal attempt to allocate instance of SQLLiteral"];
  return nil;
}
- (id) copy
{
  return RETAIN(self);
}
- (id) copyWithZone: (NSZone*)z
{
  return RETAIN(self);
}
@end

@implementation SQLLiteralProxy
- (unichar) characterAtIndex: (NSUInteger)i
{
  return [content characterAtIndex: i];
}
- (void) dealloc
{
  [content release];
  [super dealloc];
}
- (void) getCharacters: (unichar*)buffer
{
  [content getCharacters: buffer];
}
- (void) getCharacters: (unichar*)buffer
                 range: (NSRange)aRange
{
  return [content getCharacters: buffer range: aRange];
}
- (NSUInteger) hash
{
  return [content hash];
}
- (BOOL) isEqual: (id)other
{
  return [content isEqual: other];
}
- (NSUInteger) length
{
  return [content length];
}
- (const char *) UTF8String
{
  return [content UTF8String];
}
@end

@implementation SQLRecordKeys

- (NSUInteger) count
{
  return count;
}

- (void) dealloc
{
  if (nil != order) [order release];
  if (nil != map) [map release];
  if (nil != low) [low release];
  [super dealloc];
}

- (NSUInteger) indexForKey: (NSString*)key
{
  NSUInteger    c;

  c = (NSUInteger)NSMapGet(map, key);
  if (c > 0)
    {
      return c - 1;
    }
  key = [key lowercaseString];
  c = (NSUInteger)NSMapGet(low, key);
  if (c > 0)
    {
      if (classDebugging > 0)
        {
          NSLog(@"[SQLRecordKeys-indexForKey:] lowercase '%@'", key);
        }
      return c - 1;
    }
  return NSNotFound;
}

- (id) initWithKeys: (NSString**)keys count: (NSUInteger)c
{
  if (nil != (self = [super init]))
    {
      count = c;
      order = [[NSArray alloc] initWithObjects: keys count: c];
      map = NSCreateMapTable(NSObjectMapKeyCallBacks,
        NSIntegerMapValueCallBacks, count);
      low = NSCreateMapTable(NSObjectMapKeyCallBacks,
        NSIntegerMapValueCallBacks, count);
      for (c = 1; c <= count; c++)
        {
          NSString      *k = keys[c-1];

          NSMapInsert(map, (void*)k, (void*)c);
          k = [k lowercaseString];
          NSMapInsert(low, (void*)k, (void*)c);
        }
    }
  return self;
}

- (NSArray*) order
{
  return order;
}

- (NSUInteger) sizeInBytesExcluding: (NSHashTable*)exclude
{
  NSUInteger    size = [super sizeInBytesExcluding: exclude];

  if (size > 0)
    {
      if (0 == bytes)
        {
          bytes = size;
          bytes += [order sizeInBytesExcluding: exclude];
          bytes += [map sizeInBytesExcluding: exclude];
          bytes += [low sizeInBytesExcluding: exclude];
        }
      size = bytes;
    }
  return size;
}

@end


@interface	_ConcreteSQLRecord : SQLRecord
{
  SQLRecordKeys *keys;
  NSUInteger	count;  // Must be last
}
@end

@interface	CacheQuery : NSObject
{
@public
  SQLLiteral	*query;
  id		recordType;
  id		listType;
  unsigned	lifetime;
}
@end

@implementation	CacheQuery
- (void) dealloc
{
  [query release];
  [super dealloc];
}
@end

static Class aClass = 0;
static Class rClass = 0;

@implementation	SQLRecord
+ (id) allocWithZone: (NSZone*)aZone
{
  NSLog(@"Illegal attempt to allocate an SQLRecord");
  return nil;
}

+ (void) initialize
{
  GSTickerTimeNow();
  if (nil == null)
    {
      null = [NSNull new];
    }
  if (Nil == aClass)
    {
      aClass = [NSMutableArray class];
    }
  if (Nil == rClass)
    {
      rClass = [_ConcreteSQLRecord class];
    }
}

+ (id) newWithValues: (id*)v keys: (NSString**)k count: (unsigned int)c
{
  return [rClass newWithValues: v keys: k count: c];
}

+ (id) newWithValues: (id*)v keys: (SQLRecordKeys*)k
{
  return [rClass newWithValues: v keys: k];
}

- (NSArray*) allKeys
{
  NSUInteger	count = [self count];

  if (count > 0)
    {
      id	buf[count];

      while (count-- > 0)
        {
          buf[count] = [self keyAtIndex: count];
        }
      return [NSArray arrayWithObjects: buf count: count];
    }
  else
    {
      return [NSArray array];
    }
}

- (id) copyWithZone: (NSZone*)z
{
  return [self retain];
}

- (NSUInteger) count
{
  SUBCLASS_RESPONSIBILITY
  return 0;
}

- (NSMutableDictionary*) dictionary
{
  NSUInteger		count = [self count];

  if (count > 0)
    {
      id	keys[count];
      id        vals[count];

      [self getKeys: keys];
      [self getObjects: vals];
      return [NSMutableDictionary dictionaryWithObjects: vals
                                                forKeys: keys
                                                  count: count];
    }
  else
    {
      return [NSMutableDictionary dictionary];
    }
}

- (void) getKeys: (id*)buf
{
  NSUInteger	i = [self count];

  while (i-- > 0)
    {
      buf[i] = [self keyAtIndex: i];
    }
}

- (void) getObjects: (id*)buf
{
  NSUInteger	i = [self count];

  while (i-- > 0)
    {
      buf[i] = [self objectAtIndex: i];
    }
}

- (id) init
{
  NSLog(@"Illegal attempt to -init an SQLRecord");
  [self release];
  return nil;
}

- (NSString*) keyAtIndex: (NSUInteger)index
{
  SUBCLASS_RESPONSIBILITY
  return nil;
}

- (SQLRecordKeys*) keys
{
  return nil;
}

- (id) objectAtIndex: (NSUInteger)index
{
  SUBCLASS_RESPONSIBILITY
  return nil;
}

- (id) objectForKey: (NSString*)key
{
  NSUInteger    count = [self count];

  if (count > 0)
    {
      NSUInteger        pos;
      id	        keys[count];

      [self getKeys: keys];
      for (pos = 0; pos < count; pos++)
        {
          if ([key isEqualToString: keys[pos]] == YES)
            {
              break;
            }
        }
      if (pos == count)
        {
          for (pos = 0; pos < count; pos++)
            {
              if ([key caseInsensitiveCompare: keys[pos]] == NSOrderedSame)
                {
                  break;
                }
            }
        }

      if (pos != count)
        {
          return [self objectAtIndex: pos];
        }
    }
  return nil;
}

- (void) replaceObjectAtIndex: (NSUInteger)index withObject: (id)anObject
{
  SUBCLASS_RESPONSIBILITY
}

- (void) setObject: (id)anObject forKey: (NSString*)aKey
{
  NSUInteger	count = [self count];

  if (count > 0)
    {
      NSUInteger	pos;
      id		keys[count];

      if (anObject == nil)
        {
          anObject = null;
        }
      [self getKeys: keys];
      for (pos = 0; pos < count; pos++)
        {
          if ([aKey isEqualToString: keys[pos]] == YES)
            {
              break;
            }
        }
      if (pos == count)
        {
          for (pos = 0; pos < count; pos++)
            {
              if ([aKey caseInsensitiveCompare: keys[pos]] == NSOrderedSame)
                {
                  break;
                }
            }
        }

      if (pos == count)
        {
          [NSException raise: NSInvalidArgumentException
                      format: @"Bad key (%@) in -setObject:forKey:", aKey];
        }
      else
        {
          [self replaceObjectAtIndex: pos withObject: anObject];
        }
    }
  else
    {
      [NSException raise: NSInvalidArgumentException
		  format: @"Bad key (%@) in -setObject:forKey:", aKey];
    }
}

- (NSUInteger) sizeInBytes: (NSMutableSet*)exclude
{
  NSUInteger	size = [super sizeInBytes: exclude];
  NSUInteger	count = [self count];

  if (size > 0 && count > 0)
    {
      NSUInteger	pos;
      id	        vals[count];

      [self getObjects: vals];
      for (pos = 0; pos < count; pos++)
	{
	  size += [vals[pos] sizeInBytes: exclude];
	}
    }
  return size;
}

@end

@implementation	SQLRecord (KVC)
- (void) setValue: (id)aValue forKey: (NSString*)aKey
{
  [self setObject: aValue forKey: aKey];
}
- (id) valueForKey: (NSString*)aKey
{
  id	v = [self objectForKey: aKey];

  if (v == nil)
    {
      v = [super valueForKey: aKey];
    }
  return v;
}
@end


@implementation	_ConcreteSQLRecord

+ (id) newWithValues: (id*)v keys: (SQLRecordKeys*)k
{
  id		        *ptr;
  _ConcreteSQLRecord	*r;
  NSUInteger	        c;

  c = [k count];
  r = (_ConcreteSQLRecord*)NSAllocateObject(self,
    c*sizeof(id), NSDefaultMallocZone());
  r->count = c;
  r->keys = [k retain];
  ptr = (id*)(((void*)&(r->count)) + sizeof(r->count));
  while (c-- > 0)
    {
      if (nil == v[c])
	{
	  ptr[c] = [null retain];
	}
      else
	{
	  ptr[c] = [v[c] retain];
	}
    }
  return r;
}

+ (id) newWithValues: (id*)v keys: (NSString**)k count: (unsigned int)c
{
  SQLRecordKeys         *o;
  _ConcreteSQLRecord	*r;

  o = [[SQLRecordKeys alloc] initWithKeys: k count: c];
  r = [self newWithValues: v keys: o];
  [o release];
  return r;
}

- (NSArray*) allKeys
{
  return [keys order];
}

- (id) copyWithZone: (NSZone*)z
{
  return [self retain];
}

- (NSUInteger) count
{
  return count;
}

- (void) dealloc
{
  id		*ptr;
  NSUInteger	pos;

  [keys release];
  ptr = (id*)(((void*)&count) + sizeof(count));
  for (pos = 0; pos < count; pos++)
    {
      [ptr[pos] release]; ptr[pos] = nil;
    }
  [super dealloc];
}

- (NSMutableDictionary*) dictionary
{
  NSMutableDictionary	*d;
  NSUInteger		pos;
  NSArray               *k = [keys order];
  id			*ptr;

  ptr = (id*)(((void*)&count) + sizeof(count));
  d = [NSMutableDictionary dictionaryWithCapacity: count];
  for (pos = 0; pos < count; pos++)
    {
      [d setObject: ptr[pos]
            forKey: [[k objectAtIndex: pos] lowercaseString]];
    }
  return d;
}

- (void) getKeys: (id*)buf
{
  [[keys order] getObjects: buf];
}

- (void) getObjects: (id*)buf
{
  id		*ptr;
  NSUInteger	pos;

  ptr = (id*)(((void*)&count) + sizeof(count));
  for (pos = 0; pos < count; pos++)
    {
      buf[pos] = ptr[pos];
    }
}

- (id) init
{
  NSLog(@"Illegal attempt to -init an SQLRecord");
  [self release];
  return nil;
}

- (NSString*) keyAtIndex: (NSUInteger)pos
{
  return [[keys order] objectAtIndex: pos];
}

- (SQLRecordKeys*) keys
{
  return keys;
}

- (id) objectAtIndex: (NSUInteger)pos
{
  id	*ptr;

  if (pos >= count)
    {
      [NSException raise: NSRangeException
		  format: @"Array index too large"];
    }
  ptr = (id*)(((void*)&count) + sizeof(count));
  return ptr[pos];
}

- (id) objectForKey: (NSString*)key
{
  NSUInteger    pos = [keys indexForKey: key];

  if (NSNotFound == pos)
    {
      return nil;
    }
  else
    {
      id        *ptr;

      ptr = (id*)(((void*)&count) + sizeof(count));
      return ptr[pos];
    }
}

- (void) replaceObjectAtIndex: (NSUInteger)index withObject: (id)anObject
{
  id		*ptr;

  if (index >= count)
    {
      [NSException raise: NSRangeException
		  format: @"Array index too large"];
    }
  if (anObject == nil)
    {
      anObject = null;
    }
  ptr = (id*)(((void*)&count) + sizeof(count));
  ptr += index;
  [anObject retain];
  [*ptr release];
  *ptr = anObject;
}

- (void) setObject: (id)anObject forKey: (NSString*)aKey
{
  id		*ptr;
  NSUInteger 	pos;

  if (anObject == nil)
    {
      anObject = null;
    }
  ptr = (id*)(((void*)&count) + sizeof(count));
  pos = [keys indexForKey: aKey];
  if (NSNotFound == pos)
    {
      [NSException raise: NSInvalidArgumentException
                  format: @"Bad key (%@) in -setObject:forKey:", aKey];
    }
  else
    {
      [anObject retain];
      [ptr[pos] release];
      ptr[pos] = anObject;
    }
}

- (NSUInteger) sizeInBytes: (NSMutableSet*)exclude
{
  if ([exclude member: self] != nil)
    {
      return 0;
    }
  else
    {
      NSUInteger	size = [super sizeInBytes: exclude];
      NSUInteger	pos;
      id		*ptr;

      ptr = (id*)(((void*)&count) + sizeof(count));
      for (pos = 0; pos < count; pos++)
	{
	  size += [ptr[pos] sizeInBytes: exclude];
	}
      return size;
    }
}

- (NSUInteger) sizeInBytesExcluding: (NSHashTable*)exclude
{
  static NSUInteger     (*imp)(id,SEL,id) = 0;
  NSUInteger            size;

  /* We use the NSObject implementation to get the memory used,
   * and then add in the fields within the record.
   */
  if (0 == imp)
    {
      imp = (NSUInteger(*)(id,SEL,id))
        [NSObject instanceMethodForSelector: _cmd];
    }
  size = (*imp)(self, _cmd, exclude);
  if (size > 0)
    {
      NSUInteger	pos;
      id		*ptr;

      size += [keys sizeInBytesExcluding: exclude];
      size += sizeof(void*) * count;
      ptr = (id*)(((void*)&count) + sizeof(count));
      for (pos = 0; pos < count; pos++)
	{
	  size += [ptr[pos] sizeInBytesExcluding: exclude];
	}
    }
  return size;
}

@end

/**
 * Exception raised when an error with the remote database server occurs.
 */
NSString	*SQLException = @"SQLException";
/**
 * Exception for when a connection to the server is lost.
 */
NSString	*SQLConnectionException = @"SQLConnectionException";
/**
 * Exception for when a query is supposed to return data and doesn't.
 */
NSString	*SQLEmptyException = @"SQLEmptyException";
/**
 * Exception for when an insert/update would break the uniqueness of a
 * field or index.
 */
NSString	*SQLUniqueException = @"SQLUniqueException";

@implementation	SQLClient (Logging)

+ (unsigned int) debugging
{
  return classDebugging;
}

+ (NSTimeInterval) durationLogging
{
  return classDuration;
}

+ (void) setDebugging: (unsigned int)level
{
  classDebugging = level;
}

+ (void) setDurationLogging: (NSTimeInterval)threshold
{
  classDuration = threshold;
}

- (void) debug: (NSString*)fmt, ...
{
  va_list	ap;

  va_start(ap, fmt);
  NSLogv(fmt, ap);
  va_end(ap);
}

- (unsigned int) debugging
{
  return _debugging;
}

- (NSTimeInterval) durationLogging
{
  return _duration;
}

- (void) setDebugging: (unsigned int)level
{
  _debugging = level;
}

- (void) setDurationLogging: (NSTimeInterval)threshold
{
  _duration = threshold;
}

@end

/* Containers for all instances.
 * Access to and update of these containers is protected by classLock.
 * Most other operations involving an instance are protected by a lock
 * in that  instance.
 * To avoid deadlocks, we always obtain the instance lock *before* we
 * obtain the class lock, so we don't get the situation where one thread
 * has locked the instance and another has locked the class lock, and
 * each thread then tries to get the other lock.
 */
static NSHashTable	*clientsHash = 0;
static NSMapTable	*clientsMap = 0;
static NSRecursiveLock	*clientsLock = nil;

/* Protect changes to the cache used for queries by any individual client.
 */
static NSRecursiveLock	*cacheLock = nil;

static NSString		*beginString = @"begin";
static NSArray		*beginStatement = nil;
static NSString		*commitString = @"commit";
static NSArray		*commitStatement = nil;
static NSString		*rollbackString = @"rollback";
static NSArray		*rollbackStatement = nil;


@interface	SQLClient (Private)

/**
 * Internal method to handle configuration using the notification object.
 * This object may be either a configuration front end or a user defaults
 * object ... so we have to be careful that we work with both.
 */
- (void) _configure: (NSNotification*)n;

/** Internal method to make the client instance lock available to
 * an associated SQLTransaction
 */
- (NSRecursiveLock*) _lock;

/** Internal method to populate the cache with the result of a query.
 */
- (void) _populateCache: (CacheQuery*)a;

/** Internal method called to record the 'main' thread in which automated
 * cache updates are to be performed.
 */
- (void) _recordMainThread;

/*
 * Called at one second intervals to ensure that our current timestamp
 * is reasonably accurate.
 */
+ (void) _tick: (NSTimer*)t;

@end

@interface	SQLClient (GSCacheDelegate)
- (BOOL) shouldKeepItem: (id)anObject
		withKey: (id)aKey
	       lifetime: (unsigned)lifetime
		  after: (unsigned)delay;
@end

@implementation	SQLClient

static NSTimeInterval   abandonAfter = 0.0;
static unsigned int	maxConnections = 8;
static int	        poolConnections = 0;

+ (NSArray*) allClients
{
  NSMutableArray	*a;
  NSHashEnumerator	e;
  id                    o;

  [clientsLock lock];
  a = [NSMutableArray arrayWithCapacity: NSCountHashTable(clientsHash)];
  e = NSEnumerateHashTable(clientsHash);
  while (nil != (o = (id)NSNextHashEnumeratorItem(&e)))
    {
      [a addObject: o];
    }
  NSEndHashTableEnumeration(&e);
  [clientsLock unlock];
  return a;
}

+ (SQLClient*) clientWithConfiguration: (NSDictionary*)config
				  name: (NSString*)reference
{
  SQLClient	*o;

  if ([reference isKindOfClass: NSStringClass] == NO)
    {
      if (nil == config)
	{
	  config = (NSDictionary*)[NSUserDefaults standardUserDefaults];
	}
      reference = [config objectForKey: @"SQLClientName"];
      if ([reference isKindOfClass: NSStringClass] == NO)
	{
	  reference = @"Database";
	}
    }

  o = [self existingClient: reference];
  if (nil == o)
    {
      o = [[[SQLClient alloc] initWithConfiguration: config name: reference]
        autorelease];
    }
  else if ([config isKindOfClass: [NSUserDefaults class]] == NO)
    {
      /* If the configuration object is not the user defaults
       * make sure to update the existing connnection's configuration.
       */
      NSNotification *n = [NSNotification
	notificationWithName: NSUserDefaultsDidChangeNotification
        object: config
        userInfo: nil];
      [o _configure: n];
    }

  return o;
}

+ (SQLClient*) existingClient: (NSString*)reference
{
  SQLClient	*existing;

  if ([reference isKindOfClass: NSStringClass] == NO)
    {
      reference = [[NSUserDefaults standardUserDefaults] stringForKey:
	@"SQLClientName"];
      if (reference == nil)
	{
	  reference = @"Database";
	}
    }

  [clientsLock lock];
  existing = (SQLClient*)NSMapGet(clientsMap, reference);
  [[existing retain] autorelease];
  [clientsLock unlock];
  return existing;
}

+ (void) initialize
{
  if (Nil == SQLClientClass && [SQLClient class] == self)
    {
      static id	modes[1];
      
      if (Nil == LitStringClass)
        {
          /* Find the literal string class used by the foundation library.
           * We may have two varieties.
           */
          LitStringClass = object_getClass(@"test string");
          TinyStringClass = object_getClass(@"test");

          SQLStringClass = [SQLString class];
          SQLStringSize = class_getInstanceSize(SQLStringClass);
          LitProxyClass = [SQLLiteralProxy class];
        }

      if (nil == null)
        {
          null = [NSNull new];
        }
      SQLClientClass = self;
      modes[0] = NSDefaultRunLoopMode;
      queryModes = [[NSArray alloc] initWithObjects: modes count: 1];
      GSTickerTimeNow();
      [SQLRecord class];	// Force initialisatio
      if (0 == clientsHash)
        {
          cacheLock = [NSRecursiveLock new];
          clientsHash = NSCreateHashTable(NSNonOwnedPointerHashCallBacks, 0);
          clientsMap = NSCreateMapTable(NSObjectMapKeyCallBacks,
            NSNonRetainedObjectMapValueCallBacks, 0);
          clientsLock = [NSRecursiveLock new];
          beginStatement = [[NSArray arrayWithObject: beginString] retain];
          commitStatement = [[NSArray arrayWithObject: commitString] retain];
          rollbackStatement
            = [[NSArray arrayWithObject: rollbackString] retain];
          NSStringClass = [NSString class];
          NSDateClass = [NSDate class];
          NSArrayClass = [NSArray class];
          NSSetClass = [NSSet class];
          [NSTimer scheduledTimerWithTimeInterval: 1.0
                                           target: self
                                         selector: @selector(_tick:)
                                         userInfo: 0
                                          repeats: YES];
        }
    }
}

+ (unsigned int) maxConnections
{
  return maxConnections;
}

+ (void) purgeConnections: (NSDate*)since
{
  NSHashEnumerator	e;
  NSMutableArray        *a = nil;
  SQLClient		*o;
  unsigned int		connectionCount = 0;
  NSTimeInterval	t;

  t = (nil == since) ? 0.0 : [since timeIntervalSinceReferenceDate];

  /* Find clients we may want to disconnect.
   */
  [clientsLock lock];
  e = NSEnumerateHashTable(clientsHash);
  while (nil != (o = (SQLClient*)NSNextHashEnumeratorItem(&e)))
    {
      if (YES == o->connected)
        {
          connectionCount++;
          if (since != nil)
            {
              NSTimeInterval	when = o->_lastOperation;

              if (when < o->_lastStart)
                {
                  when = o->_lastStart;
                }
              if (when < t)
                {
                  if (nil == a)
                    {
                      a = [NSMutableArray array];
                    }
                  [a addObject: o];
                }
            }
        }
    }
  NSEndHashTableEnumeration(&e);
  [clientsLock unlock];

  /* Disconnect any clients idle too long
   */
  while ([a count] > 0)
    {
      o = [a lastObject];
      if ([o->lock tryLock])
        {
	  if (YES == o->connected)
            {
              NSTimeInterval	when = o->_lastOperation;

              if (when < o->_lastStart)
                {
                  when = o->_lastStart;
                }
              if (when < t)
                {
                  NS_DURING
                    {
                      [o disconnect];
                    }
                  NS_HANDLER
                    {
                      NSLog(@"Problem disconnecting: %@", localException);
                    }
                  NS_ENDHANDLER
                }
            }
          if (NO == o->connected)
            {
              connectionCount--;
            }
          [o->lock unlock];
        }
      [a removeLastObject];
    }

  while (connectionCount >= (maxConnections + poolConnections))
    {
      SQLClient		*other = nil;
      NSTimeInterval	oldest = 0.0;
  
      connectionCount = 0;
      [clientsLock lock];
      e = NSEnumerateHashTable(clientsHash);
      while (nil != (o = (SQLClient*)NSNextHashEnumeratorItem(&e)))
	{
	  if (YES == o->connected)
	    {
	      NSTimeInterval	when = o->_lastOperation;

              if (when < o->_lastStart)
                {
                  when = o->_lastStart;
                }
	      connectionCount++;
	      if (oldest == 0.0 || when < oldest)
		{
		  oldest = when;
		  ASSIGN(other, o);
		}
	    }
	}
      NSEndHashTableEnumeration(&e);
      [clientsLock unlock];
      connectionCount--;
      if ([other debugging] > 0)
	{
	  [other debug:
	    @"Force disconnect of '%@' because max connections (%u) reached",
	    other, maxConnections]; 
	}
      [AUTORELEASE(other) disconnect];
    }
}

+ (void) setAbandonFailedConnectionsAfter: (NSTimeInterval)delay
{
  abandonAfter = delay;
}

+ (void) setMaxConnections: (unsigned int)c
{
  if (c > 0)
    {
      maxConnections = c;
      [self purgeConnections: nil];
    }
}

- (void) begin
{
  [lock lock];
  if (_inTransaction == NO)
    {
      NS_DURING
	{
	  [self simpleExecute: beginStatement];
	  _inTransaction = YES;
	  /* NB.  We leave the lock locked ... until a matching -commit
	   * or -rollback is called.  This prevents other threads from
	   * interfering with this transaction.
	   */
	}
      NS_HANDLER
	{
	  [lock unlock];
	  [localException raise];
	}
      NS_ENDHANDLER
    }
  else
    {
      [lock unlock];
      [NSException raise: NSInternalInconsistencyException
		  format: @"begin used inside transaction"];
    }
}

- (SQLLiteral*) buildQuery: (NSString*)stmt, ...
{
  va_list	ap;
  NSString	*sql = nil;

  /*
   * First check validity and concatenate parts of the query.
   */
  va_start (ap, stmt);
  sql = [[self prepare: stmt args: ap] objectAtIndex: 0];
  va_end (ap);

  if ([sql length] < 1000)
    {
      return SQLClientMakeLiteral(sql);
    }
  return SQLClientProxyLiteral(sql);
}

- (SQLLiteral*) buildQuery: (NSString*)stmt with: (NSDictionary*)values
{
  NSString	*sql = nil;

  sql = [[self prepare: stmt with: values] objectAtIndex: 0];

  if ([sql length] < 1000)
    {
      return SQLClientMakeLiteral(sql);
    }
  return SQLClientProxyLiteral(sql);
}

- (NSString*) clientName
{
  NSString      *s;

  [lock lock];
  if (nil == _client)
    {
      _client = [[[NSProcessInfo processInfo] globallyUniqueString] retain];
    }
  s = [_client retain];
  [lock unlock];
  return [s autorelease];
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

  /* Since we are in a transaction we must be doubly locked right now,
   * so we unlock once, and we still have the lock (which was locked
   * in the earlier call to the -begin method).
   */
  [lock unlock];
  _inTransaction = NO;
  NS_DURING
    {
      [self simpleExecute: commitStatement];
      [_statements removeAllObjects];
      [lock unlock];		// Locked by -begin
    }
  NS_HANDLER
    {
      [_statements removeAllObjects];
      [lock unlock];		// Locked by -begin
      [localException raise];
    }
  NS_ENDHANDLER
}

- (uint64_t) committed
{
  return _committed;
}

- (BOOL) connect
{
  if (NO == [self tryConnect] && abandonAfter > 0.0)
    {
      NSTimeInterval    end;

      end = [NSDate timeIntervalSinceReferenceDate] + abandonAfter;
      while (NO == connected && [NSDate timeIntervalSinceReferenceDate] < end)
	{
          [self tryConnect];
	}
    }
  return connected;
}

- (BOOL) connected
{
  return connected;
}

- (NSString*) database
{
  return _database;
}

- (void) dealloc
{
  NSNotificationCenter	*nc;

  [clientsLock lock];
  NSHashRemove(clientsHash, (void*)self);
  if (_name != nil
    && (SQLClient*)NSMapGet(clientsMap, (void*)_name) == self)
    {
      NSMapRemove(clientsMap, (void*)_name);
    }
  [clientsLock unlock];
  nc = [NSNotificationCenter defaultCenter];
  [nc removeObserver: self];
  if (YES == connected) [self disconnect];
  [lock release]; lock = nil;
  [_client release]; _client = nil;
  [_database release]; _database = nil;
  [_password release]; _password = nil;
  [_user release]; _user = nil;
  [_name release]; _name = nil;
  [_statements release]; _statements = nil;
  [_cache release]; _cache = nil;
  [_cacheThread release]; _cacheThread = nil;
  if (0 != _observers)
    {
      NSNotificationCenter      *nc;
      NSMapEnumerator	        e;
      NSMutableSet              *n;
      id                        o;

      nc = [NSNotificationCenter defaultCenter];
      e = NSEnumerateMapTable(_observers);
      while (NSNextMapEnumeratorPair(&e, (void**)&o, (void**)&n) != 0)
        {
          NSEnumerator  *ne = [n objectEnumerator];
          NSString      *name;

          while (nil != (name = [ne nextObject]))
            {
              [nc removeObserver: o name: name object: nil];
            }
        }
      NSEndMapTableEnumeration(&e);
      NSFreeMapTable(_observers);
      _observers = 0;
    }
  [_names release]; _names = 0;
  [super dealloc];
}

- (NSString*) description
{
  NSMutableString	*s = [[NSMutableString new] autorelease];

  [lock lock];
  NS_DURING
    {
      [s appendFormat: @"Database      - %@\n", [self clientName]];
      [s appendFormat: @"  Name        - %@\n", [self name]];
      [s appendFormat: @"  DBase       - %@\n", [self database]];
      [s appendFormat: @"  DB User     - %@\n", [self user]];
      [s appendFormat: @"  Password    - %@\n",
        [self password] == nil ? @"unknown" : @"known"];
      [s appendFormat: @"  Connected   - %@\n", connected ? @"yes" : @"no"];
      [s appendFormat: @"  Committed   - %"PRIu64"\n", _committed];
      [s appendFormat: @"  Transaction - %@\n",
        _inTransaction ? @"yes" : @"no"];
    }
  NS_HANDLER
    {
      [lock unlock];
      [localException raise];
    }
  NS_ENDHANDLER
  [lock unlock];
  return s;
}

- (void) disconnect
{
  if (YES == connected)
    {
      NSNotificationCenter  *nc;

      [lock lock];
      if (YES == _inTransaction)
        {
          /* If we are inside a transaction we must be doubly locked,
           * so we do an unlock corresponding to the -begin before we
           * disconnect (the disconnect implicitly rolls back the
           * transaction).
           */
          _inTransaction = NO;
          [lock unlock];
        }
      if (YES == connected)
	{
	  NS_DURING
	    {
	      [self backendDisconnect];
	    }
	  NS_HANDLER
	    {
	      [lock unlock];
	      [localException raise];
	    }
	  NS_ENDHANDLER
	}
      [lock unlock];
      nc = [NSNotificationCenter defaultCenter];
      [nc postNotificationName: SQLClientDidDisconnectNotification
                        object: self];
    }
}

- (NSInteger) execute: (NSString*)stmt, ...
{
  NSArray	*info;
  va_list	ap;

  va_start (ap, stmt);
  info = [self prepare: stmt args: ap];
  va_end (ap);
  return [self simpleExecute: info];
}

- (NSInteger) execute: (NSString*)stmt with: (NSDictionary*)values
{
  NSArray	*info;

  info = [self prepare: stmt with: values];
  return [self simpleExecute: info];
}

- (id) init
{
  return [self initWithConfiguration: nil name: nil];
}

- (id) initWithConfiguration: (NSDictionary*)config
{
  return [self initWithConfiguration: config name: nil];
}

- (id) initWithConfiguration: (NSDictionary*)config
			name: (NSString*)reference
{
  return [self initWithConfiguration: config name: reference pool: nil];
}


- (id) initWithConfiguration: (NSDictionary*)config
			name: (NSString*)reference
                        pool: (SQLClientPool*)pool
{
  NSNotification	*n;
  NSDictionary		*conf = config;
  id			existing;

  if (conf == nil)
    {
      // Pretend the defaults object is a dictionary.
      conf = (NSDictionary*)[NSUserDefaults standardUserDefaults];
    }

  if ([reference isKindOfClass: NSStringClass] == NO)
    {
      reference = [conf objectForKey: @"SQLClientName"];
      if ([reference isKindOfClass: NSStringClass] == NO)
	{
	  reference = @"Database";
	}
    }

  [clientsLock lock];
  _pool = pool;
  if (nil != _pool)
    {
      existing = nil;   // Pool, object ... can't already exist
    }
  else
    {
      existing = (SQLClient*)NSMapGet(clientsMap, reference);
    }
  if (nil == existing)
    {
      lock = [NSRecursiveLock new];	// Ensure thread-safety.
      [self setDebugging: [[self class] debugging]];
      [self setDurationLogging: [[self class] durationLogging]];
      [self setName: reference];	// Set name and store in cache.
      _statements = [NSMutableArray new];

      if ([conf isKindOfClass: [NSUserDefaults class]] == YES)
	{
	  NSNotificationCenter	*nc;

	  nc = [NSNotificationCenter defaultCenter];
	  [nc addObserver: self
		 selector: @selector(_configure:)
		     name: NSUserDefaultsDidChangeNotification
		   object: conf];
	}
      n = [NSNotification
	notificationWithName: NSUserDefaultsDidChangeNotification
	object: conf
	userInfo: nil];

      NSHashInsert(clientsHash, (void*)self);
      [self _configure: n];	// Actually set up the configuration.
    }
  else
    {
      [self release];
      self = [existing retain];

      if ([conf isKindOfClass: [NSUserDefaults class]] == NO)
        {
          /* If the configuration object is not the user defaults
           * make sure to update the existing connnection's configuration.
           */
          n = [NSNotification
	    notificationWithName: NSUserDefaultsDidChangeNotification
            object: conf
            userInfo: nil];
          [self _configure: n];
        }
    }
  [clientsLock unlock];

  return self;
}

- (NSUInteger) hash
{
  return [[self database] hash] + [[self user] hash];
}

- (BOOL) isEqual: (id)other
{
  if (self == other)
    {
      return YES;
    }
  if ([self class] != [other class])
    {
      return NO;
    }
  if (NO == [[self database] isEqual: [other database]])
    {
      return NO;
    }
  if (NO == [[self user] isEqual: [other user]])
    {
      return NO;
    }
  return YES;
}

- (BOOL) isInTransaction
{
  return _inTransaction;
}

- (NSDate*) lastConnect
{
  if (_lastConnect > 0.0)
    {
      return [NSDate dateWithTimeIntervalSinceReferenceDate: _lastConnect];
    }
  return nil;
}

- (NSDate*) lastOperation
{
  if (_lastOperation > 0.0 && _connectFails == 0)
    {
      return [NSDate dateWithTimeIntervalSinceReferenceDate: _lastOperation];
    }
  return nil;
}

- (BOOL) lockBeforeDate: (NSDate*)limit
{
  if (nil == limit)
    {
      return [lock tryLock];
    }
  return [lock lockBeforeDate: limit];
}

- (SQLClient*) longestIdle: (SQLClient*)other
{
  NSTimeInterval        t0;
  NSTimeInterval        t1;

  t0 = _lastOperation;
  if (t0 < _lastStart)
    {
      t0 = _lastStart;
    }
  if (NO == connected || 0 != _connectFails)
    {
      t0 = 0.0;
    }

  if (NO == [other isKindOfClass: SQLClientClass] || YES == [other isProxy])
    {
      t1 = 0.0;
    }
  else
    {
      t1 = other->_lastOperation;
      if (t1 < other->_lastStart)
        {
          t1 = other->_lastStart;
        }
      if (NO == other->connected || 0 != other->_connectFails)
        {
          t1 = 0.0;
        }
    }

  if (t1 > 0.0 && (t1 <= t0 || 0.0 == t0))
    {
      return other;
    }
  if (t0 > 0.0 && (t0 <= t1 || 0.0 == t1))
    {
      return self;
    }
  return nil;
}

- (NSString*) name
{
  return _name;
}

- (NSString*) password
{
  return _password;
}

- (NSMutableArray*) prepare: (NSString*)stmt, ...
{
  va_list		ap;
  NSMutableArray	*result;

  va_start (ap, stmt);
  result = [self prepare: stmt args: ap];
  va_end (ap);

  return result;
}

- (NSMutableArray*) prepare: (NSString*)stmt args: (va_list)args
{
  NSMutableArray	*ma = [NSMutableArray arrayWithCapacity: 2];
  NSString		*tmp = va_arg(args, NSString*);
  NSAutoreleasePool     *arp = [NSAutoreleasePool new];

  if (tmp != nil)
    {
      NSMutableString	*s = [NSMutableString stringWithCapacity: 1024];
      NSString          *warn = nil;
      unsigned          index = 0;

      [s appendString: stmt];
      /*
       * Append any values from the nil terminated varargs
       */ 
      while (tmp != nil)
        {
          index++;
          if ([tmp isKindOfClass: [NSData class]] == YES)
            {
              [ma addObject: tmp];
              tmp = @"'?'''?'";	// Marker.
            }
          else if ([tmp isKindOfClass: NSStringClass] == NO)
            {
              tmp = [self quote: tmp];
            }
          else
            {
              Class c = object_getClass(tmp);

              if (c == LitProxyClass)
                {
                  tmp = ((SQLLiteralProxy*)tmp)->content;
                }
              else if (c != LitStringClass
                && c != TinyStringClass
                && c != SQLStringClass)
                {
                  if (nil == warn)
                    {
                      warn = [NSString stringWithFormat:
                        @"\"%@\" (argument %u, %@)",
                        tmp, index, NSStringFromClass(c)];
                    }
                  if (YES == autoquote)
                    {
                      tmp = [self quote: tmp];
                    }
                }
            }
          [s appendString: tmp];
          tmp = va_arg(args, NSString*);
        }
      stmt = s;
      if (nil != warn && YES == autoquoteWarning)
        {
          if (YES == autoquote)
            {
              NSLog(@"SQLClient autoquote performed for %@ in \"%@\"",
                warn, stmt);
            }
          else
            {
              NSLog(@"SQLClient autoquote proposed for %@ in \"%@\"",
                warn, stmt);
            }
        }
    }
  [ma insertObject: SQLClientMakeLiteral(stmt) atIndex: 0];
  [arp release];
  return ma;
}

- (NSMutableArray*) prepare: (NSString*)stmt with: (NSDictionary*)values
{
  unsigned int		l = [stmt length];
  NSRange		r;
  NSMutableArray	*ma = [NSMutableArray arrayWithCapacity: 2];
  NSAutoreleasePool     *arp = [NSAutoreleasePool new];

  if (l < 2)
    {
      [ma addObject: SQLClientMakeLiteral(stmt)];	// Can't contain {...}
    }
  else if ((r = [stmt rangeOfString: @"{"]).length == 0)
    {
      [ma addObject: SQLClientMakeLiteral(stmt)];	// No '{' markup
    }
  else if (l - r.location < 2)
    {
      [ma addObject: SQLClientMakeLiteral(stmt)];	// Can't contain {...}
    }
  else if ([stmt rangeOfString: @"}" options: NSLiteralSearch
    range: NSMakeRange(r.location, l - r.location)].length == 0
    && [stmt rangeOfString: @"{{" options: NSLiteralSearch
    range: NSMakeRange(0, l)].length == 0)
    {
      [ma addObject: SQLClientMakeLiteral(stmt)];	// No '}' or '{{'
    }
  else if (r.length == 0)
    {
      [ma addObject: SQLClientMakeLiteral(stmt)];	// Nothing to do.
    }
  else
    {
      NSMutableString	*mtext = [[stmt mutableCopy] autorelease];
      NSString          *warn = nil;

      /*
       * Replace {FieldName} with the value of the field
       */
      while (r.length > 0)
	{
	  unsigned	pos = r.location;
	  unsigned	nxt;
	  unsigned	vLength;
	  NSArray	*a;
	  NSRange	s;
	  NSString	*k;
	  NSString	*v;
	  NSString	*alt;
	  id		o;
	  unsigned	i;

	  r.length = l - pos;

	  /*
	   * If the length of the string from the '{' onwards is less than two,
	   * there is nothing to do and we can end processing.
	   */
	  if (r.length < 2)
	    {
	      break;
	    }

	  if ([mtext characterAtIndex: r.location + 1] == '{')
	    {
	      // Got '{{' ... remove one of them.
	      r.length = 1;
	      [mtext replaceCharactersInRange: r withString: @""];
	      l--;
	      r.location++;
	      r.length = l - r.location;
	      r = [mtext rangeOfString: @"{"
			       options: NSLiteralSearch
				 range: r];
	      continue;
	    }

	  r = [mtext rangeOfString: @"}"
			   options: NSLiteralSearch
			     range: r];
	  if (r.length == 0)
	    {
	      break;	// No closing bracket
	    }
	  nxt = NSMaxRange(r);
	  r = NSMakeRange(pos, nxt - pos);
	  s.location = r.location + 1;
	  s.length = r.length - 2;
	  k = v = [mtext substringWithRange: s];

	  /*
	   * If the value contains a '?', it is actually in two parts,
	   * the first part is the field name, and the second part is
	   * an alternative text to be used if the value from the
	   * dictionary is empty.
	   */
	  s = [v rangeOfString: @"?"];
	  if (s.length == 0)
	    {
	      alt = @"";	// No alternative value.
	    }
	  else
	    {
	      alt = [v substringFromIndex: NSMaxRange(s)];
	      v = [v substringToIndex: s.location];
	    }
       
	  /*
	   * If the value we are substituting contains dots, we split it apart.
	   * We use the value to make a reference into the dictionary we are
	   * given.
	   */
	  a = [v componentsSeparatedByString: @"."];
	  o = values;
	  for (i = 0; i < [a count]; i++)
	    {
	      NSString	*k = [a objectAtIndex: i];

	      if ([k length] > 0)
		{
		  o = [(NSDictionary*)o objectForKey: k];
		}
	    }
	  if (o == values || o == nil)
	    {
	      v = nil;		// Mo match found.
	    }
	  else
            {
              if ([o isKindOfClass: [NSData class]] == YES)
                {
                  [ma addObject: o];
                  v = @"'?'''?'";
                }
              else if ([o isKindOfClass: NSStringClass] == NO)
                {
                  v = [self quote: o];
                }
              else
                {
                  Class c = object_getClass(o);

                  v = o;
                  if (c == LitProxyClass)
                    {
                      v = ((SQLLiteralProxy*)o)->content;
                    }
                  else if (c != LitStringClass
                    && c != TinyStringClass
                    && c != SQLStringClass)
                    {
                      if (nil == warn)
                        {
                          warn = [NSString stringWithFormat:
                            @"\"%@\" (value for \"%@\", %@)",
                            o, k, NSStringFromClass(c)];
                        }
                      if (YES == autoquote)
                        {
                          v = [self quote: o];
                        }
                    }
                }
            }

	  if ([v length] == 0)
	    {
	      v = alt;
	      if (v == nil)
	        {
		  v = @"";
	        }
	    }
	  vLength = [v length];

	  [mtext replaceCharactersInRange: r withString: v];
	  l += vLength;			// Add length of string inserted
	  l -= r.length;		// Remove length of string replaced
	  r.location += vLength;

	  if (r.location >= l)
	    {
	      break;
	    }
	  r.length = l - r.location;
	  r = [mtext rangeOfString: @"{"
			   options: NSLiteralSearch
			     range: r];
	}
      [ma insertObject: SQLClientProxyLiteral(mtext) atIndex: 0];
      if (nil != warn && YES == autoquoteWarning)
        {
          if (YES == autoquote)
            {
              NSLog(@"SQLClient autoquote performed for %@ in \"%@\"",
                warn, mtext);
            }
          else
            {
              NSLog(@"SQLClient autoquote proposed for %@ in \"%@\"",
                warn, mtext);
            }
        }
    }
  [arp release];
  return ma;
}

- (NSMutableArray*) query: (NSString*)stmt, ...
{
  va_list		ap;
  NSMutableArray	*result = nil;
  SQLLiteral            *query;

  /*
   * First check validity and concatenate parts of the query.
   */
  va_start (ap, stmt);
  query = [[self prepare: stmt args: ap] objectAtIndex: 0];
  va_end (ap);

  result = [self simpleQuery: query];

  return result;
}

- (NSMutableArray*) query: (NSString*)stmt with: (NSDictionary*)values
{
  NSMutableArray	*result = nil;
  SQLLiteral            *query;

  query = [[self prepare: stmt with: values] objectAtIndex: 0];

  result = [self simpleQuery: query];

  return result;
}

- (SQLLiteral*) quote: (id)obj
{
  /* For a nil object, we return NULL.
   */
  if (obj == nil || obj == null)
    {
      return (SQLLiteral*)@"NULL";
    }
  else if ([obj isKindOfClass: NSStringClass] == NO)
    {
      /* For a data object, we don't quote ... the other parts of the code
       * need to know they have an NSData object and pass it on unchanged
       * to the -backendExecute: method.
       */
      if ([obj isKindOfClass: [NSData class]] == YES)
	{
	  return obj;
	}

      SQLLiteral        *tmp = [obj quoteForSQLClient: self];
      if (nil == tmp)
        {
          [NSException raise: NSInvalidArgumentException
            format: @"Attempt to SQL quote instance of unsupported class: %@",
            obj];
        }
      return tmp;
    }

  /* Get a string description of the object.  */
  obj = [self quoteString: obj];

  return obj;
}

- (SQLLiteral*) quotef: (NSString*)fmt, ...
{
  va_list	ap;
  NSString	*str;
  SQLLiteral	*quoted;

  va_start(ap, fmt);
  str = [[NSString allocWithZone: NSDefaultMallocZone()]
    initWithFormat: fmt arguments: ap];
  va_end(ap);

  quoted = [self quoteString: str];
  [str release];
  return quoted;
}

- (SQLLiteral*) quoteArray: (NSArray *)a
{
  NSMutableString	*s;

  s = [self quoteArray: a 
	      toString: nil 
	quotingStrings: NO];
  return SQLClientProxyLiteral(s);
}

- (SQLLiteral*) quoteArraySafe: (NSArray *)a
{
  NSMutableString	*s;

  s = [self quoteArray: a 
	      toString: nil 
	quotingStrings: YES];
  return SQLClientProxyLiteral(s);
}

- (NSMutableString*) quoteArray: (NSArray *)a
                       toString: (NSMutableString *)s
                 quotingStrings: (BOOL)q
{
  [NSException raise: NSGenericException
    format: @"%@ not supported for this database", NSStringFromSelector(_cmd)]; 
  return nil;
}

- (SQLLiteral*) quoteBigInteger: (int64_t)i
{
  char          buf[32];
  unsigned      len;
  SQLLiteral    *s;

  len = sprintf(buf, "%"PRId64, i);
  s = SQLClientNewLiteral(buf, len);
  return [s autorelease];
}

- (SQLLiteral*) quoteBoolean: (int)i
{
  return (SQLLiteral*)(i ? @"true" : @"false");
}

- (SQLLiteral*) quoteCString: (const char *)s
{
  NSString	*str;
  SQLLiteral	*quoted;

  if (s == 0)
    {
      s = "";
    }
  str = [[NSString alloc] initWithCString: s];
  quoted = [self quoteString: str];
  [str release];
  return quoted;
}

- (SQLLiteral*) quoteChar: (char)c
{
  NSString	*str;
  SQLLiteral	*quoted;

  if (c == 0)
    {
      [NSException raise: NSInvalidArgumentException
		  format: @"Attempt to quote a nul character in -quoteChar:"];
    }
  str = [[NSString alloc] initWithFormat: @"%c", c];
  quoted = [self quoteString: str];
  [str release];
  return quoted;
}

- (SQLLiteral*) quoteFloat: (double)f
{
  char          buf[32];
  unsigned      len;
  SQLLiteral    *s;

  len = sprintf(buf, "%.17g", f);
  s = SQLClientNewLiteral(buf, len);
  return [s autorelease];
}

- (SQLLiteral*) quoteInteger: (int)i
{
  char          buf[32];
  unsigned      len;
  SQLLiteral    *s;

  len = sprintf(buf, "%i", i);
  s = SQLClientNewLiteral(buf, len);
  return [s autorelease];
}

- (SQLLiteral*) quoteName: (NSString *)s
{
  NSData        *d = [s dataUsingEncoding: NSUTF8StringEncoding];
  const char    *src = (const char*)[d bytes];
  char          *dst;
  unsigned      len = [d length];
  unsigned      count = 2;
  unsigned      i;
  SQLString     *q;

  for (i = 0; i < len; i++)
    {
      char      c = src[i];

      if ('\"' == c)
        {
          count++;      // A quote needs to be doubled
        }
      if ('\0' != c)
        {
          count++;      // A nul needs to be ignored
        }
    }
  q = NSAllocateObject(SQLStringClass, count + 1, NSDefaultMallocZone());
  dst = ((char*)(void*)q) + SQLStringSize;
  q->utf8Bytes = (uint8_t*)dst;
  q->byteLen = count;
  *dst++ = '\"';
  for (i = 0; i < len; i++)
    {
      char      c = src[i];

      if ('\"' == c)
        {
          *dst++ = '\"';
        }
      if ('\0' != c)
        {
          *dst++ = c;
        }
     }   
  *dst++ = '\"';
  *dst = '\0';
  q->charLen = lengthUTF8(q->utf8Bytes, q->byteLen, &q->ascii, &q->latin1);
  return [q autorelease];
}

- (SQLLiteral*) quoteSet: (id)obj
{
  NSEnumerator          *enumerator = [obj objectEnumerator];
  NSMutableString	*ms = [NSMutableString stringWithCapacity: 100];
  id			value = [enumerator nextObject];

  [ms appendString: @"("];
  if (value != nil)
    {
      [ms appendString: [self quote: value]];
    }
  while ((value = [enumerator nextObject]) != nil)
    {
      [ms appendString: @","];
      [ms appendString: [self quote: value]];
    }
  [ms appendString: @")"];
  return SQLClientMakeLiteral(ms);
}

- (SQLLiteral*) quoteString: (NSString *)s
{
  NSData        *d = [s dataUsingEncoding: NSUTF8StringEncoding];
  const char    *src = (const char*)[d bytes];
  char          *dst;
  unsigned      len = [d length];
  unsigned      count = 2;
  unsigned      i;
  SQLString     *q;

  for (i = 0; i < len; i++)
    {
      char      c = src[i];

      if ('\'' == c)
        {
          count++;      // A quote needs to be doubled
        }
      if ('\0' != c)
        {
          count++;      // A nul needs to be ignored
        }
    }
  q = NSAllocateObject(SQLStringClass, count + 1, NSDefaultMallocZone());
  dst = ((char*)(void*)q) + SQLStringSize;
  q->utf8Bytes = (uint8_t*)dst;
  q->byteLen = count;
  *dst++ = '\'';
  for (i = 0; i < len; i++)
    {
      char      c = src[i];

      if ('\'' == c)
        {
          *dst++ = '\'';
        }
      if ('\0' != c)
        {
          *dst++ = c;
        }
     }   
  *dst++ = '\'';
  *dst = '\0';
  q->charLen = lengthUTF8(q->utf8Bytes, q->byteLen, &q->ascii, &q->latin1);
  return [q autorelease];
}

- (oneway void) release
{
  /* We lock the table while checking, to prevent another thread
   * from grabbing this object while we are checking it.
   */
  [clientsLock lock];
  if (nil != _pool && [self retainCount] == 1)
    {
      /* This is the only reference to a client associated with
       * a connection pool we put this client back to the pool.
       *
       * That being the case, we know that this thread 'owns'
       * the client and it's not going to be deallocated and not
       * going to have the _pool iinstance variable changed, so it
       * is safe to unlock clientsLock before returning the client
       * to the pool.  This avoids a possible deadlock when a pool
       * is being purged.
       *
       * wl 2019-05-01: The original implementation was calling this code
       * when NSDecrementExtraRefCountWasZero returns YES, but
       * this does not work with newer versions of the GNUstep
       * Objective-C runtime nor with recent versions of Apple's
       * Objective-C runtime, which both don't handle resurrection
       * gracefully for objects whose retain count has become zero.
       */
      [clientsLock unlock];
      [_pool _swallowClient: self explicit: NO];
    }
  else
    {
      [super release];
      [clientsLock unlock];
    }
}

- (SQLClientPool*) pool
{
  return _pool;
}

- (void) rollback
{
  [lock lock];
  if (NO == _inTransaction)
    {
      [lock unlock];	// Not in a transaction ... nothing to do.
      return;
    }

  /* Since we are in a transaction we must be doubly locked right now,
   * so we unlock once, and we still have the lock (which was locked
   * in the earlier call to the -begin method).
   */
  [lock unlock];
  _inTransaction = NO;
  NS_DURING
    {
      /* If the connection was lost, the transaction has implicitly
       * rolled back anyway, so we should skip the rollback statement.
       */
      if (YES == [self connected])
        {
          [self simpleExecute: rollbackStatement];
        }
      [_statements removeAllObjects];
      [lock unlock];		// Locked by -begin
    }
  NS_HANDLER
    {
      [_statements removeAllObjects];
      [lock unlock];		// Locked by -begin
      [localException raise];
    }
  NS_ENDHANDLER
}

- (void) setClientName: (NSString*)s
{
  [lock lock];
  ASSIGNCOPY(_client, s);
  [lock unlock];
}

- (void) setDatabase: (NSString*)s
{
  [lock lock];
  NS_DURING
    {
      if ([s isEqual: _database] == NO)
        {
          if (YES == connected)
            {
              [self disconnect];
            }
          s = [s copy];
          [_database release];
          _database = s;
        }
    }
  NS_HANDLER
    {
      [lock unlock];
      [localException raise];
    }
  NS_ENDHANDLER
  [lock unlock];
}

- (void) setName: (NSString*)s
{
  [lock lock];
  NS_DURING
    {
      if ([s isEqual: _name] == NO)
        {
          [clientsLock lock];
          if (nil == _pool)
            {
              if (NSMapGet(clientsMap, s) != 0)
                {
                  [clientsLock unlock];
                  [lock unlock];
                  if ([self debugging] > 0)
                    {
                      [self
                        debug: @"Error attempt to re-use client name %@", s];
                    }
                  NS_VOIDRETURN;
                }
            }
          if (YES == connected)
            {
              [self disconnect];
            }
          if (_name != nil
            && (SQLClient*)NSMapGet(clientsMap, (void*)_name) == self)
            {
              NSMapRemove(clientsMap, (void*)_name);
            }
          s = [s copy];
          [_name release];
          _name = s;
          if (nil == _client)
            {
              _client
                = [[[NSProcessInfo processInfo] globallyUniqueString] retain];
            }
          if (nil == _pool && _name != nil)
            {
              NSMapInsert(clientsMap, (void*)_name, (void*)self);
            }
          [clientsLock unlock];
        }
    }
  NS_HANDLER
    {
      [lock unlock];
      [localException raise];
    }
  NS_ENDHANDLER
  [lock unlock];
}

- (void) setOptions: (NSDictionary*)o
{
  return;	// Abstract class does not use options
}

- (void) setPassword: (NSString*)s
{
  [lock lock];
  NS_DURING
    {
      if ([s isEqual: _password] == NO)
        {
          if (YES == connected)
            {
              [self disconnect];
            }
          s = [s copy];
          [_password release];
          _password = s;
        }
    }
  NS_HANDLER
    {
      [lock unlock];
      [localException raise];
    }
  NS_ENDHANDLER
  [lock unlock];
}

- (void) setShouldTrim: (BOOL)aFlag
{
  _shouldTrim = (YES == aFlag) ? YES : NO;
}

- (void) setUser: (NSString*)s
{
  [lock lock];
  NS_DURING
    {
      if ([s isEqual: _user] == NO)
        {
          if (YES == connected)
            {
              [self disconnect];
            }
          s = [s copy];
          [_user release];
          _user = s;
        }
    }
  NS_HANDLER
    {
      [lock unlock];
      [localException raise];
    }
  NS_ENDHANDLER
  [lock unlock];
}

- (NSInteger) simpleExecute: (id)info
{
  NSInteger     result;
  NSString      *debug = nil;
  BOOL          done = NO;
  BOOL          isCommit = NO;
  BOOL          isRollback = NO;
  NSString      *statement;

  if ([info isKindOfClass: NSArrayClass] == NO)
    {
      if ([info isKindOfClass: NSStringClass] == NO)
        {
          [NSException raise: NSInvalidArgumentException
                      format: @"[%@ -simpleExecute: %@ (class %@)]",
            NSStringFromClass([self class]),
            info,
            NSStringFromClass([info class])];
        }
      info = [NSMutableArray arrayWithObject: info];
    }

  [lock lock];

  statement = [info objectAtIndex: 0];

  /* Ensure we have a working connection.
   */
  if ([self connect] == NO)
    {
      [lock unlock];
      [NSException raise: SQLConnectionException
	format: @"Unable to connect to '%@' to run statement %@",
	[self name], statement];
    }

  if ([statement isEqualToString: commitString])
    {
      isCommit = YES;
    }
  if ([statement isEqualToString: rollbackString])
    {
      isRollback = YES;
    }

  while (NO == done)
    {
      debug = nil;
      done = YES;
      NS_DURING
        {
	  _lastStart = GSTickerTimeNow();
          result = [self backendExecute: info];
          _lastOperation = GSTickerTimeNow();
          [_statements addObject: statement];
          if (_duration >= 0)
            {
              NSTimeInterval	d;

              d = _lastOperation - _lastStart;
              if (d >= _duration)
                {
		  NSMutableString	*m;

                  if (isCommit || isRollback)
                    {
                      NSEnumerator      *e = [_statements objectEnumerator];

                      if (isCommit)
                        {
                          m = [NSMutableString stringWithFormat:
                            @"Duration %g for transaction commit ...\n", d];
                        }
                      else 
                        {
                          m = [NSMutableString stringWithFormat:
                            @"Duration %g for transaction rollback ...\n", d];
                        }
                      while ((statement = [e nextObject]) != nil)
                        {
                          [m appendFormat: @"  %@;\n", statement];
                        }
		      [m appendFormat: @"  affected %"PRIdPTR" record%s\n",
			result, ((1 == result) ? "" : "s")];
                    }
                  else if ([self debugging] > 1)
                    {
                      /*
                       * For higher debug levels, we log data objects as well
                       * as the query string, otherwise we omit them.
                       */
                      m = [NSMutableString stringWithFormat:
                        @"Duration %g for statement %@;", d, info];
		      [m appendFormat: @" affected %"PRIdPTR" record%s",
			result, ((1 == result) ? "" : "s")];
                    }
                  else
                    {
                      m = [NSMutableString stringWithFormat:
                        @"Duration %g for statement %@;", d, statement];
		      [m appendFormat: @" affected %"PRIdPTR" record%s",
			result, ((1 == result) ? "" : "s")];
                    }
		  debug = m;
                }
            }
          if (_inTransaction == NO)
            {
              [_statements removeAllObjects];
	      _committed++;
            }
        }
      NS_HANDLER
        {
          result = -1;
          if (NO == _inTransaction)
            {
              [_statements removeAllObjects];
              if ([[localException name] isEqual: SQLConnectionException])
                {
                  /* A connection failure while not in a transaction ...
                   * we can and should retry.
                   */
                  done = NO;
                  if (nil != debug)
                    {
                      NSLog(@"Will retry after: %@", localException);
                    }
		  [self connect];
                }
            }
          if (done)
            {
              [lock unlock];
              [localException raise];
            }
        }
      NS_ENDHANDLER
    }
  [lock unlock];
  if (nil != debug)
    {
      [self debug: @"%@", debug];
    }
  return result;
}

- (NSMutableArray*) simpleQuery: (SQLLitArg*)stmt
{
  return [self simpleQuery: stmt recordType: rClass listType: aClass];
}

- (NSMutableArray*) simpleQuery: (SQLLitArg*)stmt
		     recordType: (id)rtype
		       listType: (id)ltype
{
  NSMutableArray	*result = nil;
  NSString              *debug = nil;
  BOOL                  done = NO;

  if (rtype == 0) rtype = rClass;
  if (ltype == 0) ltype = aClass;
  [lock lock];
  if ([self connect] == NO)
    {
      [lock unlock];
      [NSException raise: SQLConnectionException
	format: @"Unable to connect to '%@' to run query %@",
	[self name], stmt];
    }
  while (NO == done)
    {
      done = YES;
      NS_DURING
        {
          _lastStart = GSTickerTimeNow();
          result = [self backendQuery: stmt recordType: rtype listType: ltype];
          _lastOperation = GSTickerTimeNow();
          if (_duration >= 0)
            {
              NSTimeInterval	d;

              d = _lastOperation - _lastStart;
              if (d >= _duration)
                {
		  NSUInteger	count = [result count];

                  debug = [NSString stringWithFormat:
                    @"Duration %g for query %@;  produced %"PRIuPTR" record%s",
		    d, stmt, count, ((1 == count) ? "" : "s")];
                }
            }
          if (_inTransaction == NO)
            {
	      _committed++;
            }
        }
      NS_HANDLER
        {
          if (NO == _inTransaction)
            {
              if ([[localException name] isEqual: SQLConnectionException])
                {
                  /* A connection failure while not in a transaction ...
                   * we can and should retry.
                   */
                  done = NO;
                  if (nil != debug)
                    {
                      NSLog(@"Will retry after: %@", localException);
                    }
		  [self connect];
                }
            }
          if (done)
            {
              [lock unlock];
              [localException raise];
            }
        }
      NS_ENDHANDLER
    }
  [lock unlock];
  if (nil != debug)
    {
      [self debug: @"%@", debug];
    }
  return result;
}

- (BOOL) tryConnect
{
  if (NO == connected)
    {
      [lock lock];
      if (NO == connected)
	{
	  NS_DURING
	    {
	      NSTimeInterval	_lastListen = 0.0;

              if (_connectFails > 1)
                {
                  NSTimeInterval	delay;
                  NSTimeInterval	elapsed;

                  /* If we have repeated connection failures, we enforce a
                   * delay of up to 30 seconds between connection attempts
                   * to avoid overloading the system with too frequent
                   * connection attempts.
                   */
                  delay = (_connectFails < 30) ? _connectFails : 30;
                  elapsed = GSTickerTimeNow() - _lastOperation;
                  if (elapsed < delay)
                    {
                      [NSThread sleepForTimeInterval: delay - elapsed];
                    }
                }

	      _lastStart = GSTickerTimeNow();
	      if (YES == [self backendConnect])
                {
                  /* On establishing a new connection, we must restore any
                   * listen instructions in the backend.
                   */
                  if (nil != _names)
                    {
                      NSEnumerator  *e;
                      NSString      *n;

		      _lastListen = GSTickerTimeNow();
                      e = [_names objectEnumerator];
                      while (nil != (n = [e nextObject]))
                        {
                          [self backendListen: [self quoteName: n]];
                        }
                    }
                  _lastConnect = GSTickerTimeNow();
                  _connectFails = 0;
                }
              else
                {
                  _lastOperation = GSTickerTimeNow();
                  _connectFails++;
                }

	      if (_duration >= 0)
		{
		  NSTimeInterval	d;
		  NSString		*s;

		  if (0 == _connectFails)
		    {
		      s = @"success";
		      d = _lastConnect - _lastStart;
		    }
		  else
		    {
		      s = @"failure";
		      d = _lastOperation - _lastStart;
		    }

		  if (d >= _duration)
		    {
		      if (_lastListen > 0.0)
			{
			  [self debug: @"Duration %g for connection (%@)"
			    @", of which %g adding observers.",
			    d, s, _lastOperation - _lastListen];
			}
		      else
			{
			  [self debug: @"Duration %g for connection (%@).",
			    d, s];
			}
		    }
		}
	    }
	  NS_HANDLER
	    {
	      _lastOperation = GSTickerTimeNow();
	      _connectFails++;
	      [lock unlock];
	      [localException raise];
	    }
	  NS_ENDHANDLER
	}
      [lock unlock];
      if (YES == connected)
        {
          NSNotificationCenter  *nc;

          nc = [NSNotificationCenter defaultCenter];
          [nc postNotificationName: SQLClientDidConnectNotification
                            object: self];
        }
    }
  return connected;
}

- (void) unlock
{
  [lock unlock];
}

- (NSString*) user
{
  return _user;
}

@end

@implementation	SQLClient (Subclass)

- (BOOL) backendConnect
{
  [NSException raise: NSInternalInconsistencyException
	      format: @"Called -%@ without backend bundle loaded",
    NSStringFromSelector(_cmd)];
  return NO;
}

- (void) backendDisconnect
{
  [NSException raise: NSInternalInconsistencyException
	      format: @"Called -%@ without backend bundle loaded",
    NSStringFromSelector(_cmd)];
}

- (NSInteger) backendExecute: (NSArray*)info
{
  [NSException raise: NSInternalInconsistencyException
	      format: @"Called -%@ without backend bundle loaded",
    NSStringFromSelector(_cmd)];
  return -1;
}

- (void) backendListen: (NSString*)name
{
  return;
}

- (void) backendNotify: (NSString*)name payload: (NSString*)more
{
  [NSException raise: NSInternalInconsistencyException
	      format: @"Called -%@ without backend bundle implementation",
    NSStringFromSelector(_cmd)];
  return;
}

- (NSMutableArray*) backendQuery: (NSString*)stmt
		      recordType: (id)rtype
		        listType: (id)ltype
{
  [NSException raise: NSInternalInconsistencyException
	      format: @"Called -%@ without backend bundle loaded",
    NSStringFromSelector(_cmd)];
  return nil;
}

- (void) backendUnlisten: (NSString*)name
{
  return;
}

- (unsigned) copyEscapedBLOB: (NSData*)blob into: (void*)buf
{
  [NSException raise: NSInternalInconsistencyException
	      format: @"Called -%@ without backend bundle loaded",
    NSStringFromSelector(_cmd)];
  return 0;
}

- (const void*) insertBLOBs: (NSArray*)blobs
	      intoStatement: (const void*)statement
		     length: (unsigned)sLength
		 withMarker: (const void*)marker
		     length: (unsigned)mLength
		     giving: (unsigned*)result
{
  unsigned	count = [blobs count];
  unsigned	length = sLength;

  if (count > 1)
    {
      unsigned			i;
      unsigned char		*buf;
      unsigned char		*ptr;
      const unsigned char	*from = (const unsigned char*)statement;

      /*
       * Calculate length of buffer needed.
       */
      for (i = 1; i < count; i++)
	{
	  length += [self lengthOfEscapedBLOB: [blobs objectAtIndex: i]];
	  length -= mLength;
	}

      buf = NSZoneMalloc(NSDefaultMallocZone(), length + 1);
      [NSData dataWithBytesNoCopy: buf length: length + 1];	// autoreleased
      ptr = buf;

      /*
       * Merge quoted data objects into statement.
       */
      i = 1;
      from = (unsigned char*)statement;
      while (*from != 0)
	{
	  if (*from == *(unsigned char*)marker
	    && memcmp(from, marker, mLength) == 0)
	    {
	      NSData	*d = [blobs objectAtIndex: i++];

	      from += mLength;
	      ptr += [self copyEscapedBLOB: d into: ptr];
	    }
	  else
	    {
	      *ptr++ = *from++;
	    }
	}
      *ptr = '\0';
      statement = buf;
    }
  *result = length;
  return statement;
}

- (unsigned) lengthOfEscapedBLOB: (NSData*)blob
{
  [NSException raise: NSInternalInconsistencyException
	      format: @"Called -%@ without backend bundle loaded",
    NSStringFromSelector(_cmd)];
  return 0;
}
@end

@implementation	SQLClient (Private)

- (void) _configure: (NSNotification*)n
{
  NSDictionary	*o;
  NSDictionary	*d;
  NSString	*s;
  Class		c;

  [lock lock];
  NS_DURING
    {
      o = [n object];
      /*
       * get dictionary containing config info for this client by name.
       */
      d = [o objectForKey: @"SQLClientReferences"];
      if ([d isKindOfClass: [NSDictionary class]] == NO)
        {
          [self debug: @"Unable to find SQLClientReferences config dictionary"];
          d = nil;
        }
      d = [d objectForKey: _name];
      if ([d isKindOfClass: [NSDictionary class]] == NO)
        {
          [self debug: @"Unable to find config for client '%@'", _name];
          d = nil;
        }

      s = [d objectForKey: @"ServerType"];
      if ([s isKindOfClass: NSStringClass] == NO)
        {
          s = @"Postgres";
        }

      c = NSClassFromString([@"SQLClient" stringByAppendingString: s]);
      if (c == nil)
        {
          NSString		*path;
          NSBundle		*bundle;
          NSArray		*paths;
          NSMutableArray	*tried;
          unsigned		count;

          paths = NSSearchPathForDirectoriesInDomains(NSLibraryDirectory,
            NSAllDomainsMask, YES);
          count = [paths count];
          tried = [NSMutableArray arrayWithCapacity: count];
          while (count-- > 0)
            {
              path = [paths objectAtIndex: count];
              path = [path stringByAppendingPathComponent: @"Bundles"];
              path = [path stringByAppendingPathComponent:
			     @"SQLClient"SOVERSION""];
              path = [path stringByAppendingPathComponent: s];
              path = [path stringByAppendingPathExtension: @"bundle"];
              bundle = [NSBundle bundleWithPath: path];
              if (bundle != nil)
                {
                  [tried addObject: path];
                  if ((c = [bundle principalClass]) != nil)
                    {
                      break;	// Found it.
                    }
                }
              /* Try alternative version with more libraries linked in.
               * In some systems and situations the dynamic linker needs
               * to haved the SQLClient, gnustep-base, and objc libraries
               * explicitly linked into the bundle, but in others it
               * requires them to not be linked. To handle that, we create
               * two versions of each bundle, the seond version has _libs
               * appended to the bundle name, and has the extra libraries
               * linked.
               */
              path = [path stringByDeletingPathExtension];
              path = [path stringByAppendingString: @"_libs"];
              path = [path stringByAppendingPathExtension: @"bundle"];
              bundle = [NSBundle bundleWithPath: path];
              if (bundle != nil)
                {
                  [tried addObject: path];
                  if ((c = [bundle principalClass]) != nil)
                    {
                      break;	// Found it.
                    }
                }
            }
          if (c == nil)
            {
              if ([tried count] == 0)
                {
                  [self debug: @"unable to load bundle for '%@' server type"
                    @" ... failed to locate bundle in %@", s, paths];
                }
              else
                {
                  [self debug: @"unable to load backend class for '%@' server"
                    @" type ... dynamic library load failed in %@", s, tried];
                }
              [lock unlock];
              NS_VOIDRETURN;
            }
        }
      if (c != [self class])
        {
          if (YES == connected)
            {
              [self disconnect];
            }
#ifdef	GNUSTEP
          GSDebugAllocationRemove(object_getClass(self), self);
#endif
          object_setClass(self, c);
#ifdef	GNUSTEP
          GSDebugAllocationAdd(object_getClass(self), self);
#endif
        }

      s = [d objectForKey: @"Database"];
      if ([s isKindOfClass: NSStringClass] == NO)
        {
          s = [o objectForKey: @"Database"];
          if ([s isKindOfClass: NSStringClass] == NO)
            {
              s = nil;
            }
        }
      [self setDatabase: s];

      s = [d objectForKey: @"User"];
      if ([s isKindOfClass: NSStringClass] == NO)
        {
          s = [o objectForKey: @"User"];
          if ([s isKindOfClass: NSStringClass] == NO)
            {
              s = @"";
            }
        }
      [self setUser: s];

      s = [d objectForKey: @"Password"];
      if ([s isKindOfClass: NSStringClass] == NO)
        {
          s = [o objectForKey: @"Password"];
          if ([s isKindOfClass: NSStringClass] == NO)
            {
              s = @"";
            }
        }
      [self setPassword: s];

      if (nil == d && [o isKindOfClass: [NSDictionary class]])
	{
	  d = (NSDictionary*)o;
	}
      [self setOptions: d];
    }
  NS_HANDLER
    {
      [lock unlock];
      [localException raise];
    }
  NS_ENDHANDLER
  [lock unlock];
}

- (NSRecursiveLock*) _lock
{
  return lock;
}

- (void) _populateCache: (CacheQuery*)a
{
  GSCache	*cache;
  id		result;

  result = [self simpleQuery: a->query
                  recordType: a->recordType
                    listType: a->listType];
  cache = [self cache];
  [cache setObject: result
	    forKey: a->query
	  lifetime: a->lifetime];
}

- (void) _recordMainThread
{
  mainThread = [NSThread currentThread];
}

+ (void) _tick: (NSTimer*)t
{
  (void) GSTickerTimeNow();
}
@end

@implementation	SQLClient (GSCacheDelegate)
- (BOOL) shouldKeepItem: (id)anObject
		withKey: (id)aKey
	       lifetime: (unsigned)lifetime
		  after: (unsigned)delay
{
  CacheQuery	*a;
  NSDictionary	*d;

  a = [CacheQuery new];
  aKey = [aKey copy];
  [a->query release];
  a->query = aKey;
  d = [[NSThread currentThread] threadDictionary];
  a->recordType = [d objectForKey: @"SQLClientRecordType"];
  a->listType = [d objectForKey: @"SQLClientListType"];
  a->lifetime = lifetime;
  [a autorelease];
  if (_cacheThread == nil)
    {
      [self _populateCache: a];
    }
  else
    {
      /* We schedule an asynchronous update if the item is not too old,
       * otherwise (more than lifetime seconds past its expiry) we wait
       * for the update to complete.
       */
      [self performSelectorOnMainThread: @selector(_populateCache:)
			     withObject: a
			  waitUntilDone: (delay > lifetime) ? YES : NO
				  modes: queryModes];
    }
  return YES;	// Always keep items ... 
}
@end



@implementation	SQLClient(Convenience)

+ (NSMutableArray*) columns: (NSMutableArray*)records
{
  SQLRecord		*r = [records lastObject];
  unsigned		rowCount = [records count];
  unsigned		colCount = [r count];
  NSMutableArray	*m;

  if (rowCount == 0 || colCount == 0)
    {
      m = [NSMutableArray array];
    }
  else
    {
      NSMutableArray	*cols[colCount];
      unsigned		i;

      m = [NSMutableArray arrayWithCapacity: colCount];
      for (i = 0; i < colCount; i++)
	{
	  cols[i] = [[NSMutableArray alloc] initWithCapacity: rowCount];
	  [m addObject: cols[i]];
	  [cols[i] release];
	}
      for (i = 0; i < rowCount; i++)
	{
	  unsigned	j;

	  r = [records objectAtIndex: i];
	  for (j = 0; j < colCount; j++)
	    {
	      [cols[j] addObject: [r objectAtIndex: j]];
	    }
	}
    }
  return m;
}

+ (void) singletons: (NSMutableArray*)records
{
  unsigned	c = [records count];

  while (c-- > 0)
    {
      [records replaceObjectAtIndex: c
			 withObject: [[records objectAtIndex: c] lastObject]];
    }
}

- (SQLTransaction*) batch: (BOOL)stopOnFailure
{
  return [SQLTransaction _transactionUsing: self
                                     batch: YES
                                      stop: stopOnFailure];
}

- (NSMutableArray*) columns: (NSMutableArray*)records
{
  return [SQLClient columns: records];
}

- (SQLRecord*) queryRecord: (NSString*)stmt, ...
{
  va_list	ap;
  NSArray	*result = nil;
  SQLRecord	*record;
  SQLLiteral    *query;

  va_start (ap, stmt);
  query = [[self prepare: stmt args: ap] objectAtIndex: 0];
  va_end (ap);

  result = [self simpleQuery: query];

  if ([result count] > 1)
    {
      [NSException raise: NSInvalidArgumentException
		  format: @"Query returns more than one record -\n%@\n", query];
    }
  record = [result lastObject];
  if (record == nil)
    {
      [NSException raise: SQLEmptyException
		  format: @"Query returns no data -\n%@\n", query];
    }
  return record;
}

- (NSString*) queryString: (NSString*)stmt, ...
{
  va_list	ap;
  NSArray	*result = nil;
  SQLRecord	*record;
  SQLLiteral    *query;

  va_start (ap, stmt);
  query = [[self prepare: stmt args: ap] objectAtIndex: 0];
  va_end (ap);

  result = [self simpleQuery: query];

  if ([result count] > 1)
    {
      [NSException raise: NSInvalidArgumentException
		  format: @"Query returns more than one record -\n%@\n", query];
    }
  record = [result lastObject];
  if (record == nil)
    {
      [NSException raise: SQLEmptyException
		  format: @"Query returns no data -\n%@\n", query];
    }
  if ([record count] > 1)
    {
      [NSException raise: NSInvalidArgumentException
		  format: @"Query returns multiple fields -\n%@\n", query];
    }
  return [[record lastObject] description];
}

- (void) singletons: (NSMutableArray*)records
{
  [SQLClient singletons: records];
}

- (SQLTransaction*) transaction
{
  return [SQLTransaction _transactionUsing: self batch: NO stop: NO];
}

@end



@interface	SQLClientCacheInfo : NSObject
{
@public
  NSString		*query;
  NSMutableArray	*result;
  NSTimeInterval	expires;
}
@end

@implementation	SQLClientCacheInfo
- (void) dealloc
{
  [query release]; query = nil;
  [result release]; result = nil;
  [super dealloc];
}
- (NSUInteger) hash
{
  return [query hash];
}
- (BOOL) isEqual: (id)other
{
  return [query isEqual: ((SQLClientCacheInfo*)other)->query];
}
@end

@implementation SQLClient (Caching)

- (GSCache*) cache
{
  GSCache	*c;

  /* NB we use a different lock to protect the cache from the lock
   * used to protect the database query.  That allows multiple
   * connections (a connection pool) to share the same cache.
   */
  [cacheLock lock];
  if (nil == _cache)
    {
      _cache = [GSCache new];
      [_cache setName: [self clientName]];
      if (_cacheThread != nil)
	{
	  [_cache setDelegate: self];
	}
    }
  c = [_cache retain];
  [cacheLock unlock];
  return [c autorelease];
}

- (NSMutableArray*) cacheCheckSimpleQuery: (NSString*)stmt
{
  NSMutableArray        *result = [[self cache] objectForKey: stmt];

  if (result != nil)
    {
      result = [[result mutableCopy] autorelease];
    }
  return result;
}

- (NSMutableArray*) cache: (int)seconds
		    query: (NSString*)stmt,...
{
  va_list	ap;
  SQLLiteral    *query;

  va_start (ap, stmt);
  query = [[self prepare: stmt args: ap] objectAtIndex: 0];
  va_end (ap);

  return [self cache: seconds simpleQuery: query];
}

- (NSMutableArray*) cache: (int)seconds
		    query: (NSString*)stmt
		     with: (NSDictionary*)values
{
  SQLLiteral    *query;

  query = [[self prepare: stmt with: values] objectAtIndex: 0];
  return [self cache: seconds simpleQuery: query];
}

- (NSMutableArray*) cache: (int)seconds
	      simpleQuery: (SQLLitArg*)stmt
{
  return [self cache: seconds
	 simpleQuery: stmt
	  recordType: nil
	    listType: nil];
}

- (NSMutableArray*) cache: (int)seconds
	      simpleQuery: (SQLLitArg*)stmt
	       recordType: (id)rtype
	         listType: (id)ltype
{
  NSMutableArray	*result;
  NSMutableDictionary	*md;
  GSCache		*c;
  id			toCache;
  BOOL			cacheHit;

  if (rtype == 0) rtype = rClass;
  if (ltype == 0) ltype = aClass;

  md = [[NSThread currentThread] threadDictionary];
  [md setObject: rtype forKey: @"SQLClientRecordType"];
  [md setObject: ltype forKey: @"SQLClientListType"];
  _lastStart = GSTickerTimeNow();
  c = [self cache];
  toCache = nil;

  if (seconds < 0)
    {
      seconds = -seconds;
      result = nil;
    }
  else
    {
      result = [c objectForKey: stmt];
    }

  if (result == nil)
    {
      CacheQuery	*a;

      cacheHit = NO;
      a = [CacheQuery new];
      a->query = [stmt copy];
      a->recordType = rtype;
      a->listType = ltype;
      a->lifetime = seconds;
      [a autorelease];

      if (_cacheThread == nil)
	{
          [self _populateCache: a];
	}
      else
	{
	  /* Not really an asynchronous query because we wait until it's
	   * done in order to have a result we can return.
	   */
	  [self performSelectorOnMainThread: @selector(_populateCache:)
				 withObject: a
			      waitUntilDone: YES
				      modes: queryModes];
	}
      result = [c objectForKey: stmt];
    }
  else
    {
      cacheHit = YES;
    }

  if (seconds == 0)
    {
      // We have been told to remove the existing cached item.
      [c setObject: nil forKey: stmt lifetime: seconds];
      toCache = nil;
    }

  if (toCache != nil)
    {
      // We have a newly retrieved object ... cache it.
      [c setObject: toCache forKey: stmt lifetime: seconds];
    }

  if (result != nil)
    {
      /*
       * Return an autoreleased copy ... not the original cached data.
       */
      result = [[result mutableCopy] autorelease];
    }

  _lastOperation = GSTickerTimeNow();
  if (_duration >= 0)
    {
      NSTimeInterval	d;

      d = _lastOperation - _lastStart;
      if (d >= _duration)
	{
	  [self debug: @"Duration %g for cache-%@ query %@",
	    d, (YES == cacheHit) ? @"hit" : @"miss", stmt];
	}
    }
  return result;
}

- (void) setCache: (GSCache*)aCache
{
  /* NB we use a different lock to protect the cache from the lock
   * used to protect the database query.  That allows multiple
   * connections (a connection pool) to share the same cache.
   */
  [cacheLock lock];
  NS_DURING
    {
      if (_cacheThread != nil)
        {
          [_cache setDelegate: nil];
        }
      [aCache retain];
      [_cache release];
      _cache = aCache;
      if (_cacheThread != nil)
        {
          [_cache setDelegate: self];
        }
    }
  NS_HANDLER
    {
      [cacheLock unlock];
      [localException raise];
    }
  NS_ENDHANDLER
  [cacheLock unlock];
}

- (void) setCacheThread: (NSThread*)aThread
{
  if (mainThread == nil)
    {
      [self performSelectorOnMainThread: @selector(_recordMainThread)
			     withObject: nil
			  waitUntilDone: NO
				  modes: queryModes];
    }
  if (aThread != nil && aThread != mainThread)
    {
      NSLog(@"SQLClient: only the main thread is usable as cache thread");
      aThread = mainThread;
    }
  [lock lock];
  NS_DURING
    {
      if (_cacheThread != nil)
        {
          [_cache setDelegate: nil];
        }
      [aThread retain];
      [_cacheThread release];
      _cacheThread = aThread;
      if (_cacheThread != nil)
        {
          [_cache setDelegate: self];
        }
    }
  NS_HANDLER
    {
      [lock unlock];
      [localException raise];
    }
  NS_ENDHANDLER
  [lock unlock];
}
@end

@implementation	SQLTransaction

+ (SQLTransaction*) _transactionUsing: (id)clientOrPool
                                batch: (BOOL)isBatched
                                 stop: (BOOL)stopOnFailure
{
  SQLTransaction	*transaction;

  transaction = (SQLTransaction*)NSAllocateObject(self, 0,
    NSDefaultMallocZone());
 
  transaction->_owner = [clientOrPool retain];
  transaction->_info = [NSMutableArray new];
  transaction->_batch = isBatched;
  transaction->_stop = stopOnFailure;
  transaction->_lock = [NSRecursiveLock new];
  return [transaction autorelease];
}

- (void) _addSQL: (NSMutableString*)sql andArgs: (NSMutableArray*)args
{
  unsigned      count = [_info count];
  unsigned      index;

  for (index = 0; index < count; index++)
    {
      id        o = [_info objectAtIndex: index];

      if ([o isKindOfClass: NSArrayClass] == YES)
        {
          unsigned      c = [(NSArray*)o count];

          if (c > 0)
            {
              unsigned  i;

              [sql appendString: [(NSArray*)o objectAtIndex: 0]];
              [sql appendString: @";"];
              for (i = 1; i < c; i++)
                {
                  [args addObject: [(NSArray*)o objectAtIndex: i]];
                }
            }
        }
      else
        {
          [(SQLTransaction*)o _addSQL: sql andArgs: args];
        }
    }
}

- (void) addPrepared: (NSArray*)statement
{
  [_lock lock];
  [_info addObject: statement];
  _count++;
  [_lock unlock];
}

- (void) _countLength: (unsigned*)length andArgs: (unsigned*)args
{
  unsigned      count = [_info count];
  unsigned      index;

  for (index = 0; index < count; index++)
    {
      id        o = [_info objectAtIndex: index];

      if ([o isKindOfClass: NSArrayClass] == YES)
        {
          unsigned      c = [(NSArray*)o count];

          if (c > 0)
            {
              length += [[(NSArray*)o objectAtIndex: 0] length] + 1;
              args += c - 1;
            }
        }
      else
        {
          [(SQLTransaction*)o _countLength: length andArgs: args];
        }
    }
}

- (void) add: (NSString*)stmt,...
{
  va_list               ap;
  NSMutableArray        *p;

  va_start (ap, stmt);
  p = [_owner prepare: stmt args: ap];
  va_end (ap);
  [self addPrepared: p];
}

- (void) add: (NSString*)stmt with: (NSDictionary*)values
{
  NSMutableArray        *p;

  p = [_owner prepare: stmt with: values];
  [self addPrepared: p];
}

- (void) append: (SQLTransaction*)other
{
  if (nil == other)
    {
      return;
    }
  NSAssert(other != self, NSInvalidArgumentException);
  NSAssert([other isKindOfClass: [SQLTransaction class]],
    NSInvalidArgumentException);
  [other lock];
  [_lock lock];
  NS_DURING
    {
      if (other->_count > 0)
        {
          SQLTransaction        *t;

          /* Owners must the the same client, or the same pool, or members
           * of the same pool or a client and the pool it belongs to.
           */
          if (NO == [_owner isEqual: other->_owner]
           && NO == [[_owner pool] isEqual: [other->_owner pool]])
            {
              [NSException raise: NSInvalidArgumentException
                          format: @"[%@-%@] database owner mismatch",
                NSStringFromClass([self class]), NSStringFromSelector(_cmd)];
            }

          t = [other copy];

          [_info addObject: t];
          _count += t->_count;
          [t release];
        }
    }
  NS_HANDLER
    {
      [_lock unlock];
      [other unlock];
      [localException raise];
    }
  NS_ENDHANDLER
  [_lock unlock];
  [other unlock];
}

- (id) copyWithZone: (NSZone*)z
{
  SQLTransaction        *c;

  [_lock lock];
  c = (SQLTransaction*)NSCopyObject(self, 0, z);
  c->_owner = [c->_owner retain];
  c->_info = [c->_info mutableCopy];
  c->_lock = [NSRecursiveLock new];
  [_lock unlock];
  return c;
}

- (NSUInteger) count
{
  NSUInteger    count;

  [_lock lock];
  count = [_info count];
  [_lock unlock];
  return count;
}

- (id) db
{
  return _owner;
}

- (void) dealloc
{
  [_owner release]; _owner = nil;
  [_info release]; _info = nil;
  [_lock release]; _lock = nil;
  [super dealloc];
}

- (NSString*) description
{
  NSString      *str;

  [_lock lock];
  str = [NSString stringWithFormat: @"%@ with SQL '%@' for %@",
    [super description],
    (_count == 0 ? (id)@"" : (id)_info), _owner];
  [_lock unlock];
  return str;
}

- (void) execute
{
  [_lock lock];
  if (_count > 0)
    {
      NS_DURING
        {
          NSMutableArray    *info = nil;
          SQLClientPool     *pool = nil;
          SQLClient         *db;
          NSRecursiveLock   *dbLock;
          BOOL              wrap;

          if ([_owner isKindOfClass: [SQLClientPool class]])
            {
              pool = (SQLClientPool*)_owner;
              db = [pool provideClient];
            }
          else
            {
              db = _owner;
            }
              
          dbLock = [db _lock];
          [dbLock lock];
          wrap = [db isInTransaction] ? NO : YES;
          NS_DURING
            {
              NSMutableString   *sql;
              unsigned          sqlSize = 0;
              unsigned          argCount = 0;

              [self _countLength: &sqlSize andArgs: &argCount];

              /* Allocate and initialise the transaction statement.
               */
              info = [[NSMutableArray alloc] initWithCapacity: argCount + 1];
              sql = [[NSMutableString alloc] initWithCapacity: sqlSize + 13];
              [info addObject: SQLClientProxyLiteral(sql)];
              [sql release];
              if (YES == wrap)
                {
                  [sql appendString: @"begin;"];
                }

              [self _addSQL: sql andArgs: info];

              if (YES == wrap)
                {
                  [sql appendString: @"commit;"];
                }

              [db simpleExecute: info];
              [info release]; info = nil;
              [dbLock unlock];
              if (nil != pool)
                {
                  [pool swallowClient: db];
                }
            }
          NS_HANDLER
            {
              NSException   *e = localException;

              [info release];
	      if (YES == wrap
		&& NO == [[e name] isEqual: SQLConnectionException])
		{
                  NS_DURING
                    {
                      [db simpleExecute: rollbackStatement];
                    }
                  NS_HANDLER
                    {
                      [db disconnect];
                      NSLog(@"Disconnected due to failed rollback after %@", e);
                    }
                  NS_ENDHANDLER
                }
              [dbLock unlock];
              if (nil != pool)
                {
                  [pool swallowClient: db];
                }
              [e raise];
            }
          NS_ENDHANDLER
        }
      NS_HANDLER
        {
          [_lock unlock];
          [localException raise];
        }
      NS_ENDHANDLER
      if (YES == _reset)
        {
          [self reset];
        }
    }
  [_lock unlock];
}

- (unsigned) executeBatch
{
  return [self executeBatchReturningFailures: nil logExceptions: NO];
}

- (unsigned) executeBatchReturningFailures: (SQLTransaction*)failures
			     logExceptions: (BOOL)log
{
  unsigned      executed = 0;

  [_lock lock];
  if (_count > 0)
    {
      NS_DURING
        {
          NSRecursiveLock   *dbLock;
          SQLClientPool     *pool = nil;
          SQLClient         *db;

          if ([_owner isKindOfClass: [SQLClientPool class]])
            {
              pool = (SQLClientPool*)_owner;
              db = [pool provideClient];
            }
          else
            {
              db = _owner;
            }

          dbLock = [db _lock];
          [dbLock lock];
          NS_DURING
            {
              [self execute];
              executed = _count;
            }
          NS_HANDLER
            {
              if (log == YES || [db debugging] > 0)
                {
                  [db debug: @"Initial failure executing batch %@: %@",
                    self, localException];
                }
              if (_batch == YES)
                {
                  SQLTransaction	*wrapper = nil;
                  NSUInteger  		count = [_info count];
                  NSUInteger  		i;

                  for (i = 0; i < count; i++)
                    {
                      BOOL      success = NO;
                      id        o = [_info objectAtIndex: i];

                      if ([o isKindOfClass: NSArrayClass] == YES)
                        {
                          NS_DURING
                            {
                              /* Wrap the statement inside a transaction so
                               * its context will still be that of a statement
                               * in a transaction rather than a standalone
                               * statement.  This might be important if the
                               * statement is actually a call to a stored
                               * procedure whose code must all be executed
                               * with the visibility rules of a single
                               * transaction.
                               */
                              if (wrapper == nil)
                                {
                                  wrapper = [db transaction];
                                }
                              [wrapper reset];
                              [wrapper addPrepared: o];
                              [wrapper execute];
                              executed++;
                              success = YES;
                            }
                          NS_HANDLER
                            {
                              if (failures != nil)
                                {
                                  [failures addPrepared: o];
                                }
                              if (log == YES || [db debugging] > 0)
                                {
                                  [db debug:
                                    @"Failure of %d executing batch %@: %@",
                                    i, self, localException];
                                }
                              success = NO;
                            }
                          NS_ENDHANDLER
                        }
                      else
                        {
                          unsigned      result;

                          result = [(SQLTransaction*)o
                            executeBatchReturningFailures: failures
                            logExceptions: log];
                          executed += result;
                          if (result == [(SQLTransaction*)o totalCount])
                            {
                              success = YES;
                            }
                        }
                      if (success == NO && _stop == YES)
                        {
                          /* We are configured to stop after a failure,
                           * so we need to add all the subsequent statements
                           * or transactions to the list of those which have
                           * not been done.
                           */
                          i++;
                          while (i < count)
                            {
                              id        o = [_info objectAtIndex: i++];

                              if ([o isKindOfClass: NSArrayClass] == YES)
                                {
                                  [failures addPrepared: o];
                                }
                              else
                                {
                                  [failures append: (SQLTransaction*)o];
                                }
                            }
                          break;
                        }
                    }
                }
            }
          NS_ENDHANDLER
          [dbLock unlock];
          if (nil != pool)
            {
              [pool swallowClient: db];
            }
        }
      NS_HANDLER
        {
          [_lock unlock];
          [localException raise];
        }
      NS_ENDHANDLER
      if (YES == _reset)
        {
          [self reset];
        }
    }
  [_lock unlock];
  return executed;
}

- (void) insertTransaction: (SQLTransaction*)trn atIndex: (unsigned)index
{
  [_lock lock];
  if (index > [_info count])
    {
      [_lock unlock];
      [NSException raise: NSRangeException
		  format: @"[%@-%@] index too large",
	NSStringFromClass([self class]), NSStringFromSelector(_cmd)];
    }
  if (trn == nil || trn->_count == 0)
    {
      [_lock unlock];
      [NSException raise: NSInvalidArgumentException
		  format: @"[%@-%@] attempt to insert nil/empty transaction",
	NSStringFromClass([self class]), NSStringFromSelector(_cmd)];
    }
  if (NO == [_owner isEqual: trn->_owner]
   && NO == [[_owner pool] isEqual: [trn->_owner pool]])
    {
      [_lock unlock];
      [NSException raise: NSInvalidArgumentException
		  format: @"[%@-%@] database owner mismatch",
	NSStringFromClass([self class]), NSStringFromSelector(_cmd)];
    }
  trn = [trn copy];
  [_info insertObject: trn atIndex: index];
  _count += trn->_count;
  [trn release];
  [_lock unlock];
}

- (void) lock
{
  [_lock lock];
}

- (id) owner
{
  return _owner;
}

- (void) removeTransactionAtIndex: (unsigned)index
{
  id	o;

  [_lock lock];
  if (index >= [_info count])
    {
      [_lock unlock];
      [NSException raise: NSRangeException
		  format: @"[%@-%@] index too large",
	NSStringFromClass([self class]), NSStringFromSelector(_cmd)];
    }
  o = [_info objectAtIndex:  index];
  if ([o isKindOfClass: NSArrayClass] == YES)
    {
      _count--;
    }  
  else
    {
      _count -= [(SQLTransaction*)o totalCount];
    }
  [_info removeObjectAtIndex: index];
  [_lock unlock];
}

- (void) reset
{
  [_lock lock];
  [_info removeAllObjects];
  _count = 0;
  [_lock unlock];
}

- (BOOL) setResetOnExecute: (BOOL)aFlag
{
  BOOL  old;

  [_lock lock];
  old = _reset;
  _reset = (aFlag ? YES : NO);
  [_lock unlock];
  return old;
}

- (unsigned) totalCount
{
  return _count;
}

- (SQLTransaction*) transactionAtIndex: (unsigned)index
{
  id	o;

  [_lock lock];
  if (index >= [_info count])
    {
      [_lock unlock];
      [NSException raise: NSRangeException
		  format: @"[%@-%@] index too large",
	NSStringFromClass([self class]), NSStringFromSelector(_cmd)];
    }
  o = [_info objectAtIndex: index];
  if ([o isKindOfClass: NSArrayClass] == YES)
    {
      SQLTransaction	*t = [[self owner] transaction];

      [t addPrepared: o];
      [_lock unlock];
      return t;
    }
  else
    {
      o = [o copy];
      [_lock unlock];
      return [o autorelease];
    }
}

- (void) unlock
{
  [_lock unlock];
}
@end


@implementation SQLClient (Notifications)

static NSString *
validName(NSString *name)
{
  const char    *ptr;

  if (NO == [name isKindOfClass: [NSString class]])
    {
      [NSException raise: NSInvalidArgumentException
        format: @"Notification name must be a string"];
    }
  ptr = [name UTF8String];
  if (!isalpha(*ptr))
    {
      [NSException raise: NSInvalidArgumentException
        format: @"Notification name must begin with letter"];
    }
  ptr++;
  while (0 != *ptr)
    {
      if (!isdigit(*ptr) && !isalpha(*ptr) && *ptr != '_')
        {
          [NSException raise: NSInvalidArgumentException
                      format: @"Notification name must contain only letters,"
            @" digits, and underscores"];
        }
      ptr++;
    }
  return name;
}

- (void) addObserver: (id)anObserver
            selector: (SEL)aSelector
                name: (NSString*)name
{
  if (nil == anObserver)
    {
      [NSException raise: NSInvalidArgumentException
                  format: @"Attempt to add nil observer to SQL client"];
    }
  if (nil != _pool)
    {
      [NSException raise: NSInvalidArgumentException
                  format: @"Attempt to use pool client as observer"];
    }
  name = validName(name);
  [lock lock];
  NS_DURING
    {
      NSMutableSet      *set;

      if (nil == _observers)
        {
          _observers = NSCreateMapTable(NSNonRetainedObjectMapKeyCallBacks,
            NSObjectMapValueCallBacks, 0);
          _names = [NSCountedSet new];
        }
      set = (NSMutableSet*)NSMapGet(_observers, (void*)anObserver);
      if (nil == set)
        {
          set = [NSMutableSet new];
          NSMapInsert(_observers, anObserver, set);
          [set release];
        }
      if (nil == [set member: name])
        {
          [set addObject: name];
          [[NSNotificationCenter defaultCenter] addObserver: anObserver
                                                   selector: aSelector
                                                       name: name
                                                     object: self];
          [_names addObject: name];
          if (YES == connected && 1 == [_names countForObject: name])
            {
              [self backendListen: [self quoteName: name]];
            }
        }
    }
  NS_HANDLER
    {
      [lock unlock];
      [localException raise];
    }
  NS_ENDHANDLER
  [lock unlock];
}

- (void) postNotificationName: (NSString*)name payload: (NSString*)more
{
  name = validName(name);
  if (nil != more)
    {
      if (NO == [more isKindOfClass: [NSString class]])
        {
          [NSException raise: NSInvalidArgumentException
                      format: @"Notification payload is not a string"];
        }
    }
  [lock lock];
  if ([self connect] == NO)
    {
      [lock unlock];
      [NSException raise: SQLConnectionException
	format: @"Unable to connect to '%@' to notify %@ with %@",
	[self name], name, more];
    }
  NS_DURING
    {
      [self backendNotify: [self quoteName: name]
                  payload: more];
    }
  NS_HANDLER
    {
      [lock unlock];
      [localException raise];
    }
  NS_ENDHANDLER
  [lock unlock];
}

- (void) removeObserver: (id)anObserver name: (NSString*)name
{
  if (nil != name)
    {
      name = validName(name);
    }
  [lock lock];
  NS_DURING
    {
      if (_observers != nil)
        {
          NSNotificationCenter  *nc;
          NSEnumerator          *oe = nil;

          nc = [NSNotificationCenter defaultCenter];
          if (nil == anObserver)
            {
              oe = [NSAllMapTableKeys(_observers) objectEnumerator];
              anObserver = [oe nextObject];
            }
          while (anObserver != nil)
            {
              NSMutableSet      *set;
              NSEnumerator      *nameEnumerator = nil;

              set = (NSMutableSet*)NSMapGet(_observers, (void*)anObserver);
              if (nil == name)
                {
                  /* Remove all names for this observer.
                   */
                  nameEnumerator = [[set allObjects] objectEnumerator];
                }
              else
                {
                  nameEnumerator
                    = [[NSArray arrayWithObject: name] objectEnumerator];
                }
              while (nil != (name = [nameEnumerator nextObject]))
                {
                  if (nil != [set member: name])
                    {
                      [[name retain] autorelease];
                      [set removeObject: name];
                      [nc removeObserver: anObserver
                                    name: name
                                  object: self];
                      [_names removeObject: name];
                      if (YES == connected
                        && 0 == [_names countForObject: name])
                        {
                          [self backendUnlisten: [self quoteName: name]];
                        }
                    }
                }
              if ([set count] == 0)
                {
                  NSMapRemove(_observers, (void*)anObserver);
                }
              anObserver = [oe nextObject];
            }
        }
    }
  NS_HANDLER
    {
      [lock unlock];
      [localException raise];
    }
  NS_ENDHANDLER
  [lock unlock];
}
@end



@implementation SQLDictionaryBuilder
- (void) addObject: (id)anObject
{
  return;
}

- (id) alloc
{
  return [self retain];
}

- (NSMutableDictionary*) content
{
  return content;
}

- (NSUInteger) count
{
  return [content count];
}

- (void) dealloc
{
  [content release];
  [super dealloc];
}

- (id) initWithCapacity: (NSUInteger)capacity
{
  if (nil != (self = [super init]))
    {
      DESTROY(content);
      content = [[NSMutableDictionary alloc] initWithCapacity: capacity];
    }
  return self;
}

- (id) mutableCopyWithZone: (NSZone*)aZone
{
  return [content mutableCopyWithZone: aZone];
}

- (id) newWithValues: (id*)values
		keys: (NSString**)keys
	       count: (unsigned int)count
{
  if (count != 2)
    {
      [NSException raise: NSInvalidArgumentException
                  format: @"Query did not return key/value pairs"];
    }
  [content setObject: values[1] forKey: values[0]];
  return nil;
}
@end

@implementation SQLSetBuilder
- (NSUInteger) added
{
  return added;
}

- (void) addObject: (id)anObject
{
  return;
}

- (id) alloc
{
  return [self retain];
}

- (NSCountedSet*) content
{
  return content;
}

- (NSUInteger) count
{
  return [content count];
}

- (void) dealloc
{
  [content release];
  [super dealloc];
}

- (id) initWithCapacity: (NSUInteger)capacity
{
  if (nil != (self = [super init]))
    {
      DESTROY(content);
      content = [[NSCountedSet alloc] initWithCapacity: capacity];
      added = 0;
    }
  return self;
}

- (id) mutableCopyWithZone: (NSZone*)aZone
{
  return [content mutableCopyWithZone: aZone];
}

- (id) newWithValues: (id*)values
		keys: (NSString**)keys
	       count: (unsigned int)count
{
  if (count != 1)
    {
      [NSException raise: NSInvalidArgumentException
                  format: @"Query did not return a single value"];
    }
  added++;
  [content addObject: values[0]];
  return nil;
}
@end

@implementation SQLSingletonBuilder
- (id) newWithValues: (id*)values
		keys: (NSString**)keys
	       count: (unsigned int)count
{
  /* Instead of creating an object to hold the supplied record,
   * we use the field from the record as the value to be used.
   */
  if (count != 1)
    {
      [NSException raise: NSInvalidArgumentException
                  format: @"Query did not return singleton values"];
    }
  return [values[0] retain];
}
@end

@implementation	SQLClientPool (Adjust)

+ (void) _adjustPoolConnections: (int)n
{
  unsigned      err = 0;

  [clientsLock lock];
  poolConnections += n;
  if (poolConnections < 0)
    {
      err -= poolConnections;
      poolConnections = 0;
    }
  [clientsLock unlock];
  NSAssert(0 == err, NSInvalidArgumentException);
}

@end

@implementation NSObject (SQLClient)

- (BOOL) isNull
{
  if (nil == null)
    {
      null = [NSNull new];
    }
  return (self == null ? YES : NO);
}

- (SQLLiteral*) quoteBigInteger
{
  int64_t       v = (int64_t)[[self description] longLongValue];

  return quoteBigInteger(v);
}

- (SQLLiteral*) quoteBigNatural
{
  int64_t       v = (int64_t)[[self description] longLongValue];

  if (v < 0)
    {
      [NSException raise: NSInternalInconsistencyException
                  format: @"Object (%@) is not a natural number", self];
    }
  return quoteBigInteger(v);
}

- (SQLLiteral*) quoteBigPositive
{
  int64_t       v = (int64_t)[[self description] longLongValue];

  if (v <= 0)
    {
      [NSException raise: NSInternalInconsistencyException
                  format: @"Object (%@) is not a positive number", self];
    }
  return quoteBigInteger(v);
}

- (SQLLiteral*) quoteBoolean
{
  return (SQLLiteral*)([[self description] boolValue] ? @"true" : @"false");
}

- (SQLLiteral*) quoteForSQLClient: (SQLClient*)db
{
  return nil;
}

- (SQLLiteral*) quoteInteger
{
  int64_t       v = (int64_t)[[self description] longLongValue];

  if (v > (int64_t)2147483647LL || v < (int64_t)-2147483648LL)
    {
      [NSException raise: NSInternalInconsistencyException
                  format: @"Object (%@) is not a 32bit number", self];
    }
  return quoteBigInteger(v);
}

- (SQLLiteral*) quoteNatural
{
  int64_t       v = (int64_t)[[self description] longLongValue];

  if (v > (int64_t)2147483647LL || v < (int64_t)-2147483648LL)
    {
      [NSException raise: NSInternalInconsistencyException
                  format: @"Object (%@) is not a 32bit number", self];
    }
  if (v < 0)
    {
      [NSException raise: NSInternalInconsistencyException
                  format: @"Object (%@) is not a natural number", self];
    }
  return quoteBigInteger(v);
}

- (SQLLiteral*) quotePositive
{
  int64_t       v = (int64_t)[[self description] longLongValue];

  if (v > (int64_t)2147483647LL || v < (int64_t)-2147483648LL)
    {
      [NSException raise: NSInternalInconsistencyException
                  format: @"Object (%@) is not a 32bit number", self];
    }
  if (v <= 0)
    {
      [NSException raise: NSInternalInconsistencyException
                  format: @"Object (%@) is not a positive number", self];
    }
  return quoteBigInteger(v);
}

@end

@implementation SQLClient (Quote)

+ (BOOL) autoquote
{
  return autoquote;
}

+ (BOOL) autoquoteWarning
{
  return autoquoteWarning;
}

+ (void) setAutoquote: (BOOL)aFlag
{
  autoquote = (aFlag ? YES : NO);
}

+ (void) setAutoquoteWarning: (BOOL)aFlag
{
  autoquoteWarning = (aFlag ? YES : NO);
}

@end

@implementation NSArray (Quote)
- (SQLLiteral*) quoteForSQLClient: (SQLClient*)db
{
  return [db quoteSet: self];
}
@end

@implementation NSDate (Quote)
- (SQLLiteral*) quoteForSQLClient: (SQLClient*)db
{
  return SQLClientMakeLiteral([self descriptionWithCalendarFormat:
    @"'%Y-%m-%d %H:%M:%S.%F %z'" timeZone: nil locale: nil]);
}
@end

@implementation NSNull (Quote)
- (SQLLiteral*) quoteForSQLClient: (SQLClient*)db
{
  return (SQLLiteral*)@"NULL";
}
@end

@implementation NSNumber (Quote)
- (SQLLiteral*) quoteBigInteger
{
  int64_t       v = (int64_t)[self longLongValue];

  return quoteBigInteger(v);
}

- (SQLLiteral*) quoteBigNatural
{
  int64_t       v = (int64_t)[self longLongValue];

  if (v < 0)
    {
      [NSException raise: NSInternalInconsistencyException
                  format: @"Object (%@) is not a natural number", self];
    }
  return quoteBigInteger(v);
}

- (SQLLiteral*) quoteBigPositive
{
  int64_t       v = (int64_t)[self longLongValue];

  if (v <= 0)
    {
      [NSException raise: NSInternalInconsistencyException
                  format: @"Object (%@) is not a positive number", self];
    }
  return quoteBigInteger(v);
}

- (SQLLiteral*) quoteForSQLClient: (SQLClient*)db
{
  return SQLClientMakeLiteral([self description]);
}

- (SQLLiteral*) quoteInteger
{
  int64_t       v = (int64_t)[self longLongValue];

  if (v > (int64_t)2147483647LL || v < (int64_t)-2147483648LL)
    {
      [NSException raise: NSInternalInconsistencyException
                  format: @"Object (%@) is not a 32bit number", self];
    }
  return quoteBigInteger(v);
}

- (SQLLiteral*) quoteNatural
{
  int64_t       v = (int64_t)[self longLongValue];

  if (v > (int64_t)2147483647LL || v < (int64_t)-2147483648LL)
    {
      [NSException raise: NSInternalInconsistencyException
                  format: @"Object (%@) is not a 32bit number", self];
    }
  if (v < 0)
    {
      [NSException raise: NSInternalInconsistencyException
                  format: @"Object (%@) is not a natural number", self];
    }
  return quoteBigInteger(v);
}

- (SQLLiteral*) quotePositive
{
  int64_t       v = (int64_t)[self longLongValue];

  if (v > (int64_t)2147483647LL || v < (int64_t)-2147483648LL)
    {
      [NSException raise: NSInternalInconsistencyException
                  format: @"Object (%@) is not a 32bit number", self];
    }
  if (v <= 0)
    {
      [NSException raise: NSInternalInconsistencyException
                  format: @"Object (%@) is not a positive number", self];
    }
  return quoteBigInteger(v);
}

@end

@implementation NSSet (Quote)
- (SQLLiteral*) quoteForSQLClient: (SQLClient*)db
{
  return [db quoteSet: self];
}
@end


/* Count the number of bytes that make up this UTF-8 code point.
 * This to keep in mind:
 * This macro doesn't return anything larger than '4'
 * Legal UTF-8 cannot be larger than 4 bytes long (0x10FFFF)
 * It will return 0 for anything illegal
 */
#define UTF8_BYTE_COUNT(c) \
  (((c) < 0xf8) ? 1 + ((c) >= 0xc0) + ((c) >= 0xe0) + ((c) >= 0xf0) : 0)

/* Sequentially extracts characters from UTF-8 string
 * p = pointer to the utf-8 data
 * l = length (bytes) of the utf-8 data
 * o = pointer to current offset within the data
 * n = pointer to either zero or the next pre-read part of a surrogate pair.
 * The condition for having read the entire string is that the offset (*o)
 * is the number of bytes in the string, and the unichar pointed to by *n
 * is zero (meaning there is no second part of a surrogate pair remaining).
 */
static inline unichar
nextUTF8(const uint8_t *p, unsigned l, unsigned *o, unichar *n)
{
  unsigned	i;

  /* If we still have the second part of a surrogate pair, return it.
   */
  if (*n > 0)
    {
      unichar	u = *n;

      *n = 0;
      return u;
    }

  if ((i = *o) < l)
    {
      uint8_t	c = p[i];
      uint32_t	u = c;

      if (c > 0x7f)
	{
	  int j, sle = 0;

	  /* calculated the expected sequence length */
	  sle = UTF8_BYTE_COUNT(c);

	  /* legal ? */
	  if (sle < 2)
	    {
	      [NSException raise: NSInvalidArgumentException
			  format: @"bad multibyte character length"];
	    }

	  if (sle + i > l)
	    {
	      [NSException raise: NSInvalidArgumentException
			  format: @"multibyte character extends beyond data"];
	    }

	  /* get the codepoint */
	  for (j = 1; j < sle; j++)
	    {
	      uint8_t	b = p[i + j];

	      if (b < 0x80 || b >= 0xc0)
		break;
	      u = (u << 6) | (b & 0x3f);
	    }

	  if (j < sle)
	    {
	      [NSException raise: NSInvalidArgumentException
			  format: @"bad data in multibyte character"];
	    }
	  u = u & ~(0xffffffff << ((5 * sle) + 1));
	  i += sle;

	  /*
	   * We discard invalid codepoints here.
	   */
	  if (u > 0x10ffff)
	    {
	      [NSException raise: NSInvalidArgumentException
			  format: @"invalid unicode codepoint"];
	    }
	}
      else
	{
	  i++;
	}

      /*
       * Add codepoint as either a single unichar for BMP
       * or as a pair of surrogates for codepoints over 16 bits.
       */
      if (u >= 0x10000)
	{
	  unichar ul, uh;

	  u -= 0x10000;
	  ul = u & 0x3ff;
	  uh = (u >> 10) & 0x3ff;

	  *n = ul + 0xdc00;	// record second part of pair
	  u = uh + 0xd800;	// return first part.
	}
      *o = i;			// Return new index
      return (unichar)u;
    }
  [NSException raise: NSInvalidArgumentException
	      format: @"no more data in UTF-8 string"];
  return 0;
}

@implementation SQLString

- (const char*) UTF8String
{
  return (const char*)utf8Bytes;
}

- (unichar) characterAtIndex: (NSUInteger)index
{
  NSUInteger	l = 0;
  unichar	u;
  unichar	n = 0;
  unsigned	i = 0;

  while (i < byteLen || n > 0)
    {
      u = nextUTF8(utf8Bytes, byteLen, &i, &n);
      if (l++ == index)
	{
	  return u;
	}
    }

  [NSException raise: NSInvalidArgumentException
	      format: @"-characterAtIndex: index out of range"];
  return 0;
}

- (BOOL) canBeConvertedToEncoding: (NSStringEncoding)encoding
{
  if (NSUTF8StringEncoding == encoding
    || NSUnicodeStringEncoding == encoding
    || (NSISOLatin1StringEncoding == encoding && YES == latin1)
    || (isByteCoding(encoding) && YES == ascii))
    {
      return YES;
    }
  NS_DURING
    {
      id d = [self dataUsingEncoding: encoding allowLossyConversion: NO];

      NS_VALRETURN(d != nil ? YES : NO);
    }
  NS_HANDLER
    {
      return NO;
    }
  NS_ENDHANDLER
}

- (NSData*) dataUsingEncoding: (NSStringEncoding)encoding
	 allowLossyConversion: (BOOL)flag
{
  if (0 == byteLen)
    {
      return [NSData data];
    }

  if (NSUTF8StringEncoding == encoding
    || (YES == ascii && YES == isByteCoding(encoding)))
    {
      /* We can just copy the data unmodified
       */
      return [NSData dataWithBytes: (void*)utf8Bytes
                            length: byteLen];
    }
  else if (YES == latin1 && NSISOLatin1StringEncoding == encoding)
    {
      unichar	        n = 0;
      unsigned	        i = 0;
      NSUInteger	index = 0;
      uint8_t           *buf = malloc(charLen); 

      while (index < charLen && (i < byteLen || n > 0))
	{
	  buf[index++] = nextUTF8(utf8Bytes, byteLen, &i, &n);
	}
      return [NSData dataWithBytesNoCopy: buf
                                  length: charLen
                            freeWhenDone: YES];
    }
  else if (NSUnicodeStringEncoding == encoding)
    {
      unichar	        n = 0;
      unsigned	        i = 0;
      NSUInteger	index = 0;
      unichar           *buf = malloc(charLen * sizeof(unichar)); 

      while (index < charLen && (i < byteLen || n > 0))
	{
	  buf[index++] = nextUTF8(utf8Bytes, byteLen, &i, &n);
	}
      return [NSData dataWithBytesNoCopy: buf
                                  length: charLen * sizeof(unichar)
                            freeWhenDone: YES];
    }

  return [super dataUsingEncoding: encoding allowLossyConversion: flag];
}

- (void) getCharacters: (unichar*)buffer
		 range: (NSRange)aRange
{
  unichar	n = 0;
  unsigned	i = 0;
  NSUInteger	max = NSMaxRange(aRange);
  NSUInteger	index = 0;

  if (NSNotFound == aRange.location)
    [NSException raise: NSRangeException format:
      @"in %s, range { %"PRIuPTR", %"PRIuPTR" } extends beyond string",
      GSNameFromSelector(_cmd), aRange.location, aRange.length];

  while (index < aRange.location && (i < byteLen || n > 0))
    {
      nextUTF8(utf8Bytes, byteLen, &i, &n);
      index++;
    }
  if (index == aRange.location)
    {
      while (index < max && (i < byteLen || n > 0))
	{
	  *buffer++ = nextUTF8(utf8Bytes, byteLen, &i, &n);
	  index++;
	}
    }
  if (index != max)
    {
      [NSException raise: NSRangeException format:
	@"in %s, range { %"PRIuPTR", %"PRIuPTR" } extends beyond string",
        GSNameFromSelector(_cmd), aRange.location, aRange.length];
    }
}

- (BOOL) getCString: (char*)buffer
	  maxLength: (NSUInteger)maxLength
	   encoding: (NSStringEncoding)encoding
{
  const uint8_t *ptr = utf8Bytes;
  int           length = byteLen;
  int           index;

  if (0 == maxLength || 0 == buffer)
    {
      return NO;	// Can't fit in here
    }
  if (NSUTF8StringEncoding == encoding
    || (YES == ascii && isByteCoding(encoding)))
    {
      BOOL	result = (length < maxLength) ? YES : NO;

      /* We can just copy directly.
       */
      if (maxLength <= length)
        {
          length = maxLength - 1;
        }
      for (index = 0; index < length; index++)
        {
          buffer[index] = (char)ptr[index];
        }
      /* Step back before any multibyte sequence
       */
      while (index > 0 && (ptr[index - 1] & 0x80))
	{
          index--;
        }
      buffer[index] = '\0';
      return result;
    }
  return [super getCString: buffer maxLength: maxLength encoding: encoding];
}

/* Must match the implementation in NSString
 * To avoid allocating memory, we build the hash incrementally.
 */
- (NSUInteger) hash
{
  if (NO == hasHash)
    {
      hash = [super hash];
      hasHash = YES;
    }
  return hash;
}

- (id) initWithBytes: (const void*)bytes
	      length: (NSUInteger)length
	    encoding: (NSStringEncoding)encoding
{
  RELEASE(self);
  [NSException raise: NSGenericException
	      format: @"Attempt to init an SQL string"];
  return nil;
}

- (id) initWithBytesNoCopy: (void*)bytes
		    length: (NSUInteger)length
		  encoding: (NSStringEncoding)encoding
	      freeWhenDone: (BOOL)flag
{
  RELEASE(self);
  [NSException raise: NSGenericException
	      format: @"Attempt to init an SQL string"];
  return nil;
}

- (int) intValue
{
  return strtol((const char*)utf8Bytes, 0, 10);
}

- (NSInteger) integerValue
{
  return (NSInteger)strtoll((const char*)utf8Bytes, 0, 10);
}

- (NSUInteger) length
{
  return charLen;
}

- (long long) longLongValue
{
  return strtoll((const char*)utf8Bytes, 0, 10);
}

- (NSRange) rangeOfCharacterFromSet: (NSCharacterSet*)aSet
			    options: (NSUInteger)mask
			      range: (NSRange)aRange
{
  NSUInteger	index;
  NSUInteger	start;
  NSUInteger	stop;
  NSRange	range;

  GS_RANGE_CHECK(aRange, charLen);

  start = aRange.location;
  stop = NSMaxRange(aRange);

  range.location = NSNotFound;
  range.length = 0;

  if (stop > start)
    {
      BOOL	(*mImp)(id, SEL, unichar);
      unichar	n = 0;
      unsigned	i = 0;

      mImp = (BOOL(*)(id,SEL,unichar))
	[aSet methodForSelector: @selector(characterIsMember:)];

      for (index = 0; index < start; index++)
	{
	  nextUTF8(utf8Bytes, byteLen, &i, &n);
	}
      if ((mask & NSBackwardsSearch) == NSBackwardsSearch)
	{
	  unichar	buf[stop - start];
	  NSUInteger	pos = 0;
	  
	  for (pos = 0; pos + start < stop; pos++)
	    {
	      buf[pos] = nextUTF8(utf8Bytes, byteLen, &i, &n);
	    }
	  index = stop;
	  while (index-- > start)
	    {
	      if ((*mImp)(aSet, @selector(characterIsMember:), buf[--pos]))
		{
		  range = NSMakeRange(index, 1);
		  break;
		}
	    }
	}
      else
	{
	  while (index < stop)
	    {
	      unichar letter;

	      letter = nextUTF8(utf8Bytes, byteLen, &i, &n);
	      if ((*mImp)(aSet, @selector(characterIsMember:), letter))
		{
		  range = NSMakeRange(index, 1);
		  break;
		}
	      index++;
	    }
	}
    }

  return range;
}

- (id) copyWithZone: (NSZone*)z
{
  return RETAIN(self);
}

- (NSZone*) zone
{
  return NSDefaultMallocZone();
}

- (NSStringEncoding) fastestEncoding
{
  return NSUTF8StringEncoding;
}

- (NSStringEncoding) smallestEncoding
{
  return NSUTF8StringEncoding;
}

- (NSUInteger) sizeOfContentExcluding: (NSHashTable*)exclude
{
  return byteLen + 1;
}

@end
