/** 
   Copyright (C) 2004 Free Software Foundation, Inc.
   
   Written by:  Richard Frith-Macdonald <rfm@gnu.org>
   Date:	June 2004
   
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

#include <Foundation/Foundation.h>
#include "WebServer.h"

@interface	WebServerSession : NSObject
{
  NSString		*address;
  NSFileHandle		*handle;
  GSMimeParser		*parser;
  NSMutableData		*buffer;
  unsigned		byteCount;
  NSTimeInterval	ticked;
  BOOL			processing;
  BOOL			shouldEnd;
}
- (NSString*) address;
- (NSMutableData*) buffer;
- (NSFileHandle*) handle;
- (unsigned) moreBytes: (unsigned)count;
- (GSMimeParser*) parser;
- (BOOL) processing;
- (void) reset;
- (void) setAddress: (NSString*)aString;
- (void) setBuffer: (NSMutableData*)aBuffer;
- (void) setHandle: (NSFileHandle*)aHandle;
- (void) setParser: (GSMimeParser*)aParser;
- (void) setProcessing: (BOOL)aFlag;
- (void) setShouldEnd: (BOOL)aFlag;
- (void) setTicked: (NSTimeInterval)when;
- (BOOL) shouldEnd;
- (NSTimeInterval) ticked;
@end

@implementation	WebServerSession
- (NSString*) address
{
  return address;
}

- (NSMutableData*) buffer
{
  return buffer;
}

- (void) dealloc
{
  [handle closeFile];
  DESTROY(address);
  DESTROY(buffer);
  DESTROY(handle);
  DESTROY(parser);
  [super dealloc];
}

- (NSString*) description
{
  return [NSString stringWithFormat: @"%@ [%@] ",
    [super description], [self address]];
}

- (NSFileHandle*) handle
{
  return handle;
}

- (unsigned) moreBytes: (unsigned)count
{
  byteCount += count;
  return byteCount;
}

- (GSMimeParser*) parser
{
  return parser;
}

- (BOOL) processing
{
  return processing;
}

- (void) reset
{
  [self setBuffer: [NSMutableData dataWithCapacity: 1024]];
  [self setParser: nil];
  [self setProcessing: NO];
}

- (void) setAddress: (NSString*)aString
{
  ASSIGN(address, aString);
}

- (void) setBuffer: (NSMutableData*)aBuffer
{
  ASSIGN(buffer, aBuffer);
}

- (void) setHandle: (NSFileHandle*)aHandle
{
  ASSIGN(handle, aHandle);
}

- (void) setParser: (GSMimeParser*)aParser
{
  ASSIGN(parser, aParser);
}

- (void) setProcessing: (BOOL)aFlag
{
  processing = aFlag;
}

- (void) setShouldEnd: (BOOL)aFlag
{
  shouldEnd = aFlag;
}

- (void) setTicked: (NSTimeInterval)when
{
  ticked = when;
}

- (BOOL) shouldEnd
{
  return shouldEnd;
}

- (NSTimeInterval) ticked
{
  return ticked;
}
@end

@interface	WebServer (Private)
- (void) _alert: (NSString*)fmt, ...;
- (void) _didConnect: (NSNotification*)notification;
- (void) _didRead: (NSNotification*)notification;
- (void) _didWrite: (NSNotification*)notification;
- (void) _endSession: (WebServerSession*)session;
- (void) _process: (WebServerSession*)session;
- (void) _timeout: (NSTimer*)timer;
@end

@implementation	WebServer

- (void) dealloc
{
  if (_ticker != nil)
    {
      [_ticker invalidate];
      _ticker = nil;
    }
  [self setPort: nil secure: nil];
  DESTROY(_nc);
  DESTROY(_root);
  DESTROY(_hosts);
  if (_sessions != 0)
    {
      NSFreeMapTable(_sessions);
      _sessions = 0;
    }
  [super dealloc];
}

static unsigned
unescapeData(const unsigned char* bytes, unsigned length, unsigned char *buf)
{
  unsigned int	to = 0;
  unsigned int	from = 0;

  while (from < length)
    {
      unsigned char	c = bytes[from++];

      if (c == '+')
	{
	  c = ' ';
	}
      else if (c == '%' && from < length - 1)
	{
	  unsigned char	tmp;

	  c = 0;
	  tmp = bytes[from++];
	  if (tmp <= '9' && tmp >= '0')
	    {
	      c = tmp - '0';
	    }
	  else if (tmp <= 'F' && tmp >= 'A')
	    {
	      c = tmp + 10 - 'A';
	    }
	  else if (tmp <= 'f' && tmp >= 'a')
	    {
	      c = tmp + 10 - 'a';
	    }
	  else
	    {
	      c = 0;
	    }
	  c <<= 4;
	  tmp = bytes[from++];
	  if (tmp <= '9' && tmp >= '0')
	    {
	      c += tmp - '0';
	    }
	  else if (tmp <= 'F' && tmp >= 'A')
	    {
	      c += tmp + 10 - 'A';
	    }
	  else if (tmp <= 'f' && tmp >= 'a')
	    {
	      c += tmp + 10 - 'a';
	    }
	  else
	    {
	      c = 0;
	    }
	}
      buf[to++] = c;
    }
  return to;
}

- (unsigned) decodeURLEncodedForm: (NSData*)data
			     into: (NSMutableDictionary*)dict
{
  const unsigned char	*bytes = (const unsigned char*)[data bytes];
  unsigned		length = [data length];
  unsigned		pos = 0;
  unsigned		fields = 0;

  while (pos < length)
    {
      unsigned int	keyStart = pos;
      unsigned int	keyEnd;
      unsigned int	valStart;
      unsigned int	valEnd;
      unsigned char	*buf;
      unsigned int	buflen;
      BOOL		escape = NO;
      NSData		*d;
      NSString		*k;
      NSMutableArray	*a;

      while (pos < length && bytes[pos] != '&')
	{
	  pos++;
	}
      valEnd = pos;
      if (pos < length)
	{
	  pos++;	// Step past '&'
	}

      keyEnd = keyStart;
      while (keyEnd < pos && bytes[keyEnd] != '=')
	{
	  if (bytes[keyEnd] == '%' || bytes[keyEnd] == '+')
	    {
	      escape = YES;
	    }
	  keyEnd++;
	}

      if (escape == YES)
	{
	  buf = NSZoneMalloc(NSDefaultMallocZone(), keyEnd - keyStart);
	  buflen = unescapeData(&bytes[keyStart], keyEnd - keyStart, buf);
	  d = [[NSData alloc] initWithBytesNoCopy: buf
					   length: buflen
				     freeWhenDone: YES];
	}
      else
	{
	  d = [[NSData alloc] initWithBytesNoCopy: (void*)&bytes[keyStart]
					   length: keyEnd - keyStart
				     freeWhenDone: NO];
	}
      k = [[NSString alloc] initWithData: d encoding: NSUTF8StringEncoding];
      if (k == nil)
	{
	  [NSException raise: NSInvalidArgumentException
		      format: @"Bad UTF-8 form data (key of field %d)", fields];
	}
      RELEASE(d);

      valStart = keyEnd;
      if (valStart < pos)
	{
	  valStart++;	// Step past '='
	}
      if (valStart < valEnd)
	{
	  buf = NSZoneMalloc(NSDefaultMallocZone(), valEnd - valStart);
	  buflen = unescapeData(&bytes[valStart], valEnd - valStart, buf);
	  d = [[NSData alloc] initWithBytesNoCopy: buf
					   length: buflen
				     freeWhenDone: YES];
	}
      else
	{
	  d = [NSData new];
	}
      a = [dict objectForKey: k];
      if (a == nil)
	{
	  a = [[NSMutableArray alloc] initWithCapacity: 1];
	  [dict setObject: a forKey: k];
	  RELEASE(a);
	}
      [a addObject: d];
      RELEASE(d);
      RELEASE(k);
      fields++;
    }
  return fields;
}

- (NSString*) description
{
  return [NSString stringWithFormat:
    @"%@ on %@, %u of %u sessions active, %u requests, listening: %@",
    [super description], _port, NSCountMapTable(_sessions), _maxSess, _handled,
    _accepting == YES ? @"yes" : @"no"];
}

- (id) init
{
  _nc = RETAIN([NSNotificationCenter defaultCenter]);
  _sessionTimeout = 30.0;
  _maxSess = 32;
  _maxBodySize = 8*1024;
  _maxRequestSize = 4*1024*1024;
  _substitutionLimit = 4;
  _sessions = NSCreateMapTable(NSNonOwnedPointerMapKeyCallBacks,
    NSObjectMapValueCallBacks, 0);
  _ticker = [NSTimer scheduledTimerWithTimeInterval: 0.8
					     target: self
					   selector: @selector(_timeout:)
					   userInfo: 0
					    repeats: YES];
  return self;
}

- (BOOL) isSecure
{
  if (_sslConfig == nil)
    {
      return NO;
    }
  return YES;
}

- (BOOL) produceResponse: (GSMimeDocument*)aResponse
	    fromTemplate: (NSString*)aPath
		   using: (NSDictionary*)map
{
  CREATE_AUTORELEASE_POOL(arp);
  NSString	*path = (_root == nil) ? (id)@"" : (id)_root;
  NSString	*str;
  NSFileManager	*mgr;
  BOOL		result;

  path = [path stringByAppendingString: @"/"];
  str = [path stringByStandardizingPath];
  path = [path stringByAppendingPathComponent: aPath];
  path = [path stringByStandardizingPath];
  mgr = [NSFileManager defaultManager];
  if ([path hasPrefix: str] == NO)
    {
      [self _alert: @"Illegal template '%@' ('%@')", aPath, path];
      result = NO;
    }
  else if ([mgr isReadableFileAtPath: path] == NO)
    {
      [self _alert: @"Can't read template '%@' ('%@')", aPath, path];
      result = NO;
    }
  else if ((str = [NSString stringWithContentsOfFile: path]) == nil)
    {
      [self _alert: @"Failed to load template '%@' ('%@')", aPath, path];
      result = NO;
    }
  else
    {
      NSMutableString	*m = [NSMutableString stringWithCapacity: [str length]];

      result = [self substituteFrom: str
			      using: map
			       into: m
			      depth: 0];
      if (result == YES)
	{
	  [aResponse setContent: m type: @"text/html" name: nil];
	  [[aResponse headerNamed: @"content-type"] setParameter: @"utf-8"
							  forKey: @"charset"];
	}
    }
  DESTROY(arp);
  return result;
}

- (NSMutableDictionary*) parameters: (GSMimeDocument*)request
{
  NSMutableDictionary	*params;
  NSString		*str = [[request headerNamed: @"x-http-query"] value];
  NSData		*data;

  params = [NSMutableDictionary dictionaryWithCapacity: 32];
  if ([str length] > 0)
    {
      data = [str dataUsingEncoding: NSASCIIStringEncoding];
      [self decodeURLEncodedForm: data into: params];
    }

  str = [[request headerNamed: @"content-type"] value];
  if ([str isEqualToString: @"application/x-www-form-urlencoded"] == YES)
    {
      data = [request convertToData];
      [self decodeURLEncodedForm: data into: params];
    }
  else if ([str isEqualToString: @"multipart/form-data"] == YES)
    {
      NSArray	*contents = [request content];
      unsigned	count = [contents count];
      unsigned	i;

      for (i = 0; i < count; i++)
	{
	  GSMimeDocument	*doc = [contents objectAtIndex: i];
	  GSMimeHeader		*hdr = [doc headerNamed: @"content-type"];
	  NSString		*k = [hdr parameterForKey: @"name"];

	  if (k == nil)
	    {
	      hdr = [doc headerNamed: @"content-disposition"];
	      k = [hdr parameterForKey: @"name"];
	    }
	  if (k != nil)
	    {
	      NSMutableArray	*a;

	      a = [params objectForKey: k];
	      if (a == nil)
		{
		  a = [[NSMutableArray alloc] initWithCapacity: 1];
		  [params setObject: a forKey: k];
		  RELEASE(a);
		}
	      [a addObject: [doc convertToData]];
	    }
	}
    }

  return params;
}

- (NSData*) parameter: (NSString*)name
		   at: (unsigned)index
		 from: (NSDictionary*)params
{
  NSArray	*a = [params objectForKey: name];

  if (a == nil)
    {
      NSEnumerator	*e = [params keyEnumerator];
      NSString		*k;

      while ((k = [e nextObject]) != nil)
	{
	  if ([k caseInsensitiveCompare: name] == NSOrderedSame)
	    {
	      a = [params objectForKey: k];
	      break;
	    }
	}
    }
  if (index >= [a count])
    {
      return nil;
    }
  return [a objectAtIndex: index];
}

- (NSData*) parameter: (NSString*)name from: (NSDictionary*)params
{
  return [self parameter: name at: 0 from: params];
}

- (NSString*) parameterString: (NSString*)name
			   at: (unsigned)index
			 from: (NSDictionary*)params
{
  return [self parameterString: name at: index from: params charset: nil];
}

- (NSString*) parameterString: (NSString*)name
			   at: (unsigned)index
			 from: (NSDictionary*)params
		      charset: (NSString*)charset
{
  NSData	*d = [self parameter: name at: index from: params];
  NSString	*s = nil;

  if (d != nil)
    {
      s = [NSString alloc];
      if (charset == nil || [charset length] == 0)
	{
	  s = [s initWithData: d encoding: NSUTF8StringEncoding];
	}
      else
	{
	  NSStringEncoding	enc;

	  enc = [GSMimeDocument encodingFromCharset: charset];
	  s = [s initWithData: d encoding: enc];
	}
    }
  return AUTORELEASE(s);
}

- (NSString*) parameterString: (NSString*)name from: (NSDictionary*)params
{
  return [self parameterString: name at: 0 from: params charset: nil];
}

- (NSString*) parameterString: (NSString*)name
			 from: (NSDictionary*)params
		      charset: (NSString*)charset
{
  return [self parameterString: name at: 0 from: params charset: charset];
}

- (void) setDelegate: (id)anObject
{
  _delegate = anObject;
}

- (void) setMaxBodySize: (unsigned)max
{
  _maxBodySize = max;
}

- (void) setMaxRequestSize: (unsigned)max
{
  _maxRequestSize = max;
}

- (void) setMaxSessions: (unsigned)max
{
  _maxSess = max;
  if (_maxSess > 512)
    {
      _maxSess = 512;
    }
}

- (BOOL) setPort: (NSString*)aPort secure: (NSDictionary*)secure
{
  BOOL	ok = YES;
  BOOL	update = NO;

  if (aPort == nil || [aPort isEqual: _port] == NO)
    {
      update = YES;
    }
  if ((secure == nil && _sslConfig != nil)
    || (secure != nil && [secure isEqual: _sslConfig] == NO))
    {
      update = YES;
    }

  if (update == YES)
    {
      ASSIGN(_sslConfig, secure);
      if (_listener != nil)
	{
	  [_nc removeObserver: self
			 name: NSFileHandleConnectionAcceptedNotification
		       object: _listener];
	  DESTROY(_listener);
	}
      _accepting = NO;	// No longer listening for connections.
      DESTROY(_port);
      if (aPort != nil)
	{
	  _port = [aPort copy];
	  if (_sslConfig != nil)
	    {
	      _listener = [[NSFileHandle sslClass]
		fileHandleAsServerAtAddress: nil
		service: _port
		protocol: @"tcp"];
	    }
	  else
	    {
	      _listener = [NSFileHandle fileHandleAsServerAtAddress: nil
							    service: _port
							   protocol: @"tcp"];
	    }

	  if (_listener == nil)
	    {
	      [self _alert: @"Failed to listen on port %@", _port];
	      DESTROY(_port);
	      ok = NO;
	    }
	  else
	    {
	      RETAIN(_listener);
	      [_nc addObserver: self
		      selector: @selector(_didConnect:)
			  name: NSFileHandleConnectionAcceptedNotification
			object: _listener];
	      if (_accepting == NO
		&& (_maxSess <= 0 || NSCountMapTable(_sessions) < _maxSess))
		{
		  [_listener acceptConnectionInBackgroundAndNotify];
		  _accepting = YES;
		}
	    }
	}
    }
  return ok;
}

- (void) setRoot: (NSString*)aPath
{
  ASSIGN(_root, aPath);
}

- (void) setSessionTimeout: (NSTimeInterval)aDelay
{
  _sessionTimeout = aDelay;
}

- (void) setSubstitutionLimit: (unsigned)depth
{
  _substitutionLimit = depth;
}

- (void) setVerbose: (BOOL)aFlag
{
  _verbose = aFlag;
}

- (BOOL) substituteFrom: (NSString*)aTemplate
                  using: (NSDictionary*)map
		   into: (NSMutableString*)result
		  depth: (unsigned)depth
{
  unsigned	length;
  unsigned	pos = 0;
  NSRange	r = NSMakeRange(pos, length);

  if (depth > _substitutionLimit)
    {
      [self _alert: @"Substitution exceeded limit (%u)", _substitutionLimit];
      return NO;
    }

  length = [aTemplate length];
  r = NSMakeRange(pos, length);
  r = [aTemplate rangeOfString: @"<!--"
		       options: NSLiteralSearch
			 range: r];
  while (r.length > 0)
    {
      unsigned	start = r.location;

      if (start > pos)
	{
	  r = NSMakeRange(pos, r.location - pos);
	  [result appendString: [aTemplate substringWithRange: r]];
	}
      pos = r.location;
      r = NSMakeRange(start + 4, length - start - 4);
      r = [aTemplate rangeOfString: @"-->"
			   options: NSLiteralSearch
			     range: r];
      if (r.length > 0)
	{
	  unsigned	end = NSMaxRange(r);
	  NSString	*subFrom;
	  NSString	*subTo;

	  r = NSMakeRange(start + 4, r.location - start - 4);
	  subFrom = [aTemplate substringWithRange: r];
	  subTo = [map objectForKey: subFrom];
	  if (subTo == nil)
	    {
	      [result appendString: @"<!--"];
	      pos += 4;
	    }
	  else
	    {
	      /*
	       * Unless the value substituted in is a comment,
	       * perform recursive substitution.
	       */
	      if ([subTo hasPrefix: @"<!--"] == NO)
		{
		  BOOL	v;

		  v = [self substituteFrom: subTo
				     using: map
				      into: result
				     depth: depth + 1];
		  if (v == NO)
		    {
		      return NO;
		    }
		}
	      else
		{
		  [result appendString: subTo];
		}
	      pos = end;
	    }
	}
      else
	{
	  [result appendString: @"<!--"];
	  pos += 4;
	}
      r = NSMakeRange(pos, length - pos);
      r = [aTemplate rangeOfString: @"<!--"
			   options: NSLiteralSearch
			     range: r];
    }

  if (pos < length)
    {
      r = NSMakeRange(pos, length - pos);
      [result appendString: [aTemplate substringWithRange: r]];
    }
  return YES;
}
@end

@implementation	WebServer (Private)

- (void) _alert: (NSString*)fmt, ...
{
  va_list	args;

  va_start(args, fmt);
  if ([_delegate respondsToSelector: @selector(webAlert:for:)] == YES)
    {
      NSString	*s;

      s = [NSString stringWithFormat: fmt arguments: args];
      [_delegate webAlert: s for: self];
    }
  else
    {
      NSLogv(fmt, args);
    }
  va_end(args);
}

- (void) _didConnect: (NSNotification*)notification
{
  NSDictionary		*userInfo = [notification userInfo];
  NSFileHandle		*hdl;
  NSString		*a;
  NSHost		*h;

  _accepting = NO;
  hdl = [userInfo objectForKey: NSFileHandleNotificationFileHandleItem];
  if (hdl == nil)
    {
      [NSException raise: NSInternalInconsistencyException
		  format: @"[%@ -%@] missing handle",
	NSStringFromClass([self class]), NSStringFromSelector(_cmd)];
    }
  else if ((a = [hdl socketAddress]) == nil)
    {
      [self _alert: @"Unknown address for new connection."]; 
      [hdl closeFile];
    }
  else
    {
      if (_sslConfig != nil)
	{
	  [hdl sslSetCertificate: [_sslConfig objectForKey: @"CertificateFile"]
		      privateKey: [_sslConfig objectForKey: @"KeyFile"]
		       PEMpasswd: [_sslConfig objectForKey: @"Password"]];
	}

      if ((h = [NSHost hostWithAddress: a]) == nil)
	{
	  [self _alert: @"Unknown host (%@) on new connection.", a];
	}
      else if (_hosts != nil && [_hosts containsObject: h] == NO)
	{
	  [self _alert: @"Invalid host (%@) on new connection.", a];
	}
      else if (_sslConfig != nil && [hdl sslAccept] == NO)
	{
	  [self _alert: @"SSL accept fail on new connection (%@).", a];
	}
      else
	{
	  WebServerSession	*session = [WebServerSession new];

	  [session setAddress: a];
	  [session setHandle: hdl];
	  [session setBuffer: [NSMutableData dataWithCapacity: 1024]];
	  [session setTicked: _ticked];
	  NSMapInsert(_sessions, (void*)hdl, (void*)session);
	  RELEASE(session);
	  [_nc addObserver: self
		  selector: @selector(_didRead:)
		      name: NSFileHandleReadCompletionNotification
		    object: hdl];
	  [_nc addObserver: self
		  selector: @selector(_didWrite:)
		      name: GSFileHandleWriteCompletionNotification
		    object: hdl];
	  [hdl readInBackgroundAndNotify];
	  if (_verbose == YES) NSLog(@"%@ connect", session);
	}
    }
  if (_accepting == NO
    && (_maxSess == 0 || NSCountMapTable(_sessions) < _maxSess))
    {
      [_listener acceptConnectionInBackgroundAndNotify];
      _accepting = YES;
    }
}

- (void) _didRead: (NSNotification*)notification
{
  NSDictionary		*dict = [notification userInfo];
  NSFileHandle		*hdl = [notification object];
  NSData		*d;
  id			parser;
  NSString		*method = @"";
  NSString		*query = @"";
  NSString		*path = @"";
  NSString		*version = @"";
  WebServerSession	*session;
  GSMimeDocument	*doc;

  session = (WebServerSession*)NSMapGet(_sessions, (void*)hdl);
  NSAssert(session != nil, NSInternalInconsistencyException);
  parser = [session parser];

  d = [dict objectForKey: NSFileHandleNotificationDataItem];

  if ([d length] == 0)
    {
      if (parser == nil)
	{
	  NSMutableData	*buffer = [session buffer];

	  if ([buffer length] == 0)
	    {
	      [self _alert: @"%@ read end-of-file in empty request", session];
	    }
	  else
	    {
	      [self _alert: @"%@ read end-of-file in partial request - %@",
		session, buffer];
	    }
	}
      else
	{
	  [self _alert: @"%@ read end-of-file in incomplete request - %@",
	    session, [parser mimeDocument]];
	}
      [self _alert: @"%@ read end-of-file in request", session];
      [self _endSession: session];
      return;
    }
  // NSLog(@"Data read on %@ ... %@", session, d);

  // Mark session as having had I/O ... not idle.
  [session setTicked: _ticked];

  if (parser == nil)
    {
      unsigned char	*bytes;
      unsigned int	length;
      unsigned int	pos;
      NSMutableData	*buffer;

      /*
       * Add new data to any we already have and search for the end
       * of the initial request line.
       */
      buffer = [session buffer];
      [buffer appendData: d];
      bytes = [buffer mutableBytes];
      length = [buffer length];
      for (pos = 0; pos < length; pos++)
	{
	  if (bytes[pos] == '\n')
	    {
	      break;
	    }
	}

      /*
       * Attackers may try to send too much data in the hope of causing
       * a buffer overflow ... so we try to detect it here.
       */
      if (pos >= _maxBodySize)
	{
	  [self _alert: @"Request too long ... rejected"];
	  [session setShouldEnd: YES];
	  [hdl writeInBackgroundAndNotify:
	    [@"HTTP/1.0 500 Request data too long\r\n\r\n"
	    dataUsingEncoding: NSASCIIStringEncoding]];
	  return;
	}

      if (pos == length)
	{
	  [hdl readInBackgroundAndNotify];	// Needs more data.
	  return;
	}
      else
	{
	  unsigned	back = pos;
	  unsigned	start = 0;
	  unsigned	end;

	  /*
	   * Trim trailing whitespace from request line.
	   */
	  bytes[pos++] = '\0';
	  while (back > 0 && isspace(bytes[--back]))
	    {
	      bytes[back] = '\0';
	    }

	  /*
	   * Remove and store trailing HTTP version extension
	   */
	  while (back > 0 && !isspace(bytes[back]))
	    {
	      back--;
	    }
	  if (isspace(bytes[back]))
	    {
	      bytes[back] = '\0';
	      end = back + 1;
	      if (strncmp(bytes + end, "HTTP/", 5) == 0)
		{
		  end += 5;
		  version = [NSString stringWithUTF8String: bytes + end];
		}
	    }
	  if ([version floatValue] < 1.1)
	    {
	      [session setShouldEnd: YES];	// Not persistent.
	    }

	  /*
	   * Remove leading white space.
	   */
	  start = 0;
	  while (start < back && isspace(bytes[start]))
	    {
	      start++;
	    }

	  /*
	   * Extract method string as uppercase value.
	   */
	  end = start;
	  while (end < back && !isspace(bytes[end]))
	    {
	      if (islower(bytes[end]))
		{
		  bytes[end] = toupper(bytes[end]);
		}
	      end++;
	    }
	  bytes[end++] = '\0';
	  method = [NSString stringWithUTF8String: bytes + start];

	  /*
	   * Extract path string.
	   */
	  start = end;
	  while (start < back && isspace(bytes[start]))
	    {
	      start++;
	    }
	  end = start;
	  while (end < back && bytes[end] != '?')
	    {
	      end++;
	    }
	  if (bytes[end] == '?')
	    {
	      /*
	       * Extract query string.
	       */
	      bytes[end++] = '\0';
	      query = [NSString stringWithUTF8String: bytes + end];

	    }
	  else
	    {
	      bytes[end] = '\0';
	    }
	  path = [NSString stringWithUTF8String: bytes + start];

	  if ([method isEqualToString: @"GET"] == NO
	    && [method isEqualToString: @"POST"] == NO)
	    {
	      [session setShouldEnd: YES];	// Not persistent.
	      [hdl writeInBackgroundAndNotify:
		[@"HTTP/1.0 501 Not Implemented\r\n\r\n"
		dataUsingEncoding: NSASCIIStringEncoding]];
	      return;
	    }

	  /*
	   * Any left over data is passed to the mime parser.
	   */
	  if (pos < length)
	    {
	      memmove(bytes, &bytes[pos], length - pos);
	      [buffer setLength: length - pos];
	      d = AUTORELEASE(RETAIN(buffer));
	    }

	  parser = [GSMimeParser new];
	  [parser setIsHttp];
	  [parser setDefaultCharset: @"utf-8"];

	  doc = [parser mimeDocument];

	  [doc setHeader: @"x-http-method"
		   value: method
	      parameters: nil];
	  [doc setHeader: @"x-http-path"
		   value: path
	      parameters: nil];
	  [doc setHeader: @"x-http-query"
		   value: query
	      parameters: nil];
	  [doc setHeader: @"x-http-version"
		   value: version
	      parameters: nil];

	  [session setParser: parser];
	  RELEASE(parser);

	  if (pos >= length)
	    {
	      [hdl readInBackgroundAndNotify];	// Needs more data.
	      return;
	    }
	  // Fall through to parse remaining data with mime parser
	}
    }

  doc = [parser mimeDocument];
  method = [[doc headerNamed: @"x-http-method"] value];

  if ([session moreBytes: [d length]] > _maxRequestSize)
    {
      [self _alert: @"Request body too long ... rejected"];
      [session setShouldEnd: YES];	// Not persistent.
      [hdl writeInBackgroundAndNotify:
	[@"HTTP/1.0 500 Request body too long\r\n\r\n"
	dataUsingEncoding: NSASCIIStringEncoding]];
      return;
    }
  else if ([parser parse: d] == NO)
    {
      if ([parser isComplete] == YES)
	{
	  [self _process: session];
	}
      else
	{
	  [self _alert: @"HTTP parse failure - %@", parser];
	  [self _endSession: session];
	}
    }
  else if (([parser isComplete] == YES)
    || ([parser isInHeaders] == NO && ([method isEqualToString: @"GET"])))
    {
      [self _process: session];
    }
  else
    {
      [hdl readInBackgroundAndNotify];
    }
}

- (void) _didWrite: (NSNotification*)notification
{
  NSFileHandle		*hdl = [notification object];
  WebServerSession	*session;

  session = (WebServerSession*)NSMapGet(_sessions, (void*)hdl);
  NSAssert(session != nil, NSInternalInconsistencyException);

  if ([session shouldEnd] == YES)
    {
      [self _endSession: session];
    }
  else
    {
      [session reset];
      [hdl readInBackgroundAndNotify];	// Want another request.
    }
}

- (void) _endSession: (WebServerSession*)session
{
  NSFileHandle	*hdl = [session handle];

  if (_verbose == YES) NSLog(@"%@ disconnect", session);
  [_nc removeObserver: self
		 name: NSFileHandleReadCompletionNotification
	       object: hdl];
  [_nc removeObserver: self
		 name: GSFileHandleWriteCompletionNotification
	       object: hdl];
  NSMapRemove(_sessions, (void*)hdl);
  if (_accepting == NO
    && (_maxSess <= 0 || NSCountMapTable(_sessions) < _maxSess))
    {
      [_listener acceptConnectionInBackgroundAndNotify];
      _accepting = YES;
    }
  _handled++;
}

- (void) _process: (WebServerSession*)session
{
  GSMimeDocument	*request;
  GSMimeDocument	*response;
  BOOL			responded = NO;
  NSString		*con;
  NSMutableData		*raw;
  NSMutableData		*out;
  unsigned char		*buf;
  unsigned int		len;
  unsigned int		pos;
  unsigned int		contentLength;
  NSEnumerator		*enumerator;
  GSMimeHeader		*hdr;

  AUTORELEASE(RETAIN(session));
  request = [[session parser] mimeDocument];

  /*
   * If the client specified that the connection should close, we don't
   * keep it open.
   */
  con = [[request headerNamed: @"connection"] value]; 
  if (con != nil && [con caseInsensitiveCompare: @"close"] == NSOrderedSame)
    {
      [session setShouldEnd: YES];	// Not persistent.
    }

  /*
   * Provide more information about the connection.
   */
  [request setHeader: @"x-local-address"
	       value: [[session handle] socketLocalAddress]
	  parameters: nil];
  [request setHeader: @"x-local-port"
	       value: [[session handle] socketLocalService]
	  parameters: nil];
  [request setHeader: @"x-remote-address"
	       value: [[session handle] socketAddress]
	  parameters: nil];
  [request setHeader: @"x-remote-port"
	       value: [[session handle] socketService]
	  parameters: nil];

  response = AUTORELEASE([GSMimeDocument new]);
  [response setContent: [NSData data] type: @"text/plain" name: nil];

  if (_verbose == YES) NSLog(@"Request %@ - %@", session, request);
  NS_DURING
    {
      [session setProcessing: YES];
      [session setTicked: _ticked];
      responded = [_delegate processRequest: request
				   response: response
					for: self];
      _ticked = [NSDate timeIntervalSinceReferenceDate];
      [session setTicked: _ticked];
      [session setProcessing: NO];
    }
  NS_HANDLER
    {
      [session setProcessing: NO];
      [self _alert: @"Exception %@, processing %@", localException, request];
      [response setHeader: @"http"
		    value: @"HTTP/1.0 500 Internal Server Error"
	       parameters: nil];
    }
  NS_ENDHANDLER

  [response setHeader: @"content-transfer-encoding"
		value: @"binary"
	   parameters: nil];
  raw = [response rawMimeData];
  buf = [raw mutableBytes];
  len = [raw length];

  for (pos = 4; pos < len; pos++)
    {
      if (strncmp(&buf[pos-4], "\r\n\r\n", 4) == 0)
	{
	  break;
	}
    }
  contentLength = len - pos;
  pos -= 2;
  [raw replaceBytesInRange: NSMakeRange(0, pos) withBytes: 0 length: 0];

  out = [NSMutableData dataWithCapacity: len + 1024];
  [response deleteHeaderNamed: @"mime-version"];
  [response deleteHeaderNamed: @"content-length"];
  [response deleteHeaderNamed: @"content-encoding"];
  [response deleteHeaderNamed: @"content-transfer-encoding"];
  if (contentLength > 0)
    {
      NSString	*str;

      str = [NSString stringWithFormat: @"%u", contentLength];
      [response setHeader: @"content-length" value: str parameters: nil];
    }
  else
    {
      [response deleteHeaderNamed: @"content-type"];
    }
  hdr = [response headerNamed: @"http"];
  if (hdr == nil)
    {
      const char	*s;

      if (contentLength == 0)
	{
	  s = "HTTP/1.1 204 No Content\r\n";
	}
      else
	{
	  s = "HTTP/1.1 200 Success\r\n";
	}
      [out appendBytes: s length: strlen(s)];
    }
  else
    {
      NSString	*s = [[hdr value] stringByTrimmingSpaces];

      s = [s stringByAppendingString: @"\r\n"];
      [out appendData: [s dataUsingEncoding: NSASCIIStringEncoding]];
      [response deleteHeader: hdr];
      /*
       * If the http version has been set to be an old one,
       * we must be prepared to close the connection at once.
       */
      if ([s hasPrefix: @"HTTP/"] == NO
	|| [[s substringFromIndex: 5] floatValue] < 1.1) 
	{
	  [session setShouldEnd: YES];
	}
    }

  enumerator = [[response allHeaders] objectEnumerator];
  while ((hdr = [enumerator nextObject]) != nil)
    {
      [out appendData: [hdr rawMimeData]];
    }
  if ([raw length] > 0)
    {
      [out appendData: raw];
    }
  else
    {
      [out appendBytes: "\r\n" length: 2];	// Terminate headers
    }
  if (_verbose == YES) NSLog(@"Response %@ - %@", session, out);
  [[session handle] writeInBackgroundAndNotify: out];
}

- (void) _timeout: (NSTimer*)timer
{
  unsigned		count;

  _ticked = [NSDate timeIntervalSinceReferenceDate];

  count = NSCountMapTable(_sessions);
  if (count > 0)
    {
      NSMapEnumerator	enumerator;
      WebServerSession	*session;
      NSFileHandle	*handle;
      NSMutableArray	*array;

      array = [NSMutableArray arrayWithCapacity: count];
      enumerator = NSEnumerateMapTable(_sessions);
      while (NSNextMapEnumeratorPair(&enumerator,
	(void **)(&handle), (void**)(&session)))
	{
	  if (_ticked - [session ticked] > _sessionTimeout
	      && [session processing] == NO)
	    {
	      [array addObject: session];
	    }
	}
      NSEndMapTableEnumeration(&enumerator);
      while ([array count] > 0)
	{
	  session = [array objectAtIndex: 0];
	  [self _alert: @"Session timed out - %@", session];
	  [self _endSession: session];
	  [array removeObjectAtIndex: 0];
	}
    }
}
@end

