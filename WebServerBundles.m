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
#include	<Foundation/Foundation.h>
#include	"WebServer.h"

@implementation WebServerBundles
- (void) dealloc
{
  RELEASE(_http);
  RELEASE(_handlers);
  [super dealloc];
}

- (void) defaultsUpdate: (NSNotification *)aNotification
{
  NSUserDefaults	*defs = [aNotification object];
  NSString		*port;
  NSDictionary		*secure;

  port = [defs stringForKey: @"WebServerPort"];
  secure = [defs dictionaryForKey: @"WebServerSecure"];
  [_http setPort: port secure: secure];
}

- (NSMutableDictionary*) handlers
{
  if (_handlers == nil)
    {
      _handlers = [NSMutableDictionary new];
    }
  return _handlers;
}

- (WebServer*) http
{
  return _http;
}

- (id) init
{
  return [self initAsDelegateOf: nil];
}

- (id) initAsDelegateOf: (WebServer*)http
{
  if (http == nil)
    {
      DESTROY(self);
    }
  else
    {
      NSNotificationCenter	*nc = [NSNotificationCenter defaultCenter];
      NSUserDefaults		*defs = [NSUserDefaults standardUserDefaults];
      NSNotification		*n;

      ASSIGN(_http, http);
      [_http setDelegate: self];

      /*
       * Watch for config changes, and set initial config by sending a
       * faked change notification.
       */
      [nc addObserver: self
	     selector: @selector(defaultsUpdate:)
		 name: NSUserDefaultsDidChangeNotification
	       object: defs];
      n = [NSNotification
	notificationWithName: NSUserDefaultsDidChangeNotification
		      object: defs
		    userInfo: nil];
      [self defaultsUpdate: n];
    }
  return self;
}

/**
 * We handle the incoming requests here.
 */
- (BOOL) processRequest: (GSMimeDocument*)request
	       response: (GSMimeDocument*)response
		    for: (WebServer*)http
{
  NSString		*error;
  NSString		*path;
  NSMutableDictionary	*handlers;
  id			handler;

  path = [[request headerNamed: @"x-http-path"] value];
  handlers = [self handlers];
  handler = [handlers objectForKey: path];
  if (handler == nil)
    {
      NSUserDefaults	*defs;
      NSDictionary	*conf;
      NSDictionary	*byPath;

      defs = [NSUserDefaults standardUserDefaults];
      conf = [defs dictionaryForKey: @"WebServerBundles"];
      byPath = [conf objectForKey: path];
      if ([byPath isKindOfClass: [NSDictionary class]] == NO)
	{
	  error = [NSString stringWithFormat:
	    @"Unable to find Bundles config for '%@'", path];
	  [self webAlert: error for: http];
	}
      else
	{
	  NSString	*name;

	  name = [byPath objectForKey: @"Name"];

	  if ([name length] == 0)
	    {
	      error = [NSString stringWithFormat:
		@"Unable to find Name in Bundles config for '%@'", path];
	      [self webAlert: error for: http];
	    }
	  else
	    {
	      NSBundle	*mb = [NSBundle mainBundle];
	      NSString	*p = [mb pathForResource: name ofType: @"bundle"];
	      NSBundle	*b = [NSBundle bundleWithPath: p];
	      Class	c = [b principalClass];

	      if (c == 0)
		{
		  error = [NSString stringWithFormat:
		    @"Unable to find class in '%@' for '%@'", p, path];
		  [self webAlert: error for: http];
		}
	      else
		{
		  handler = [c new];
		  [handlers setObject: handler forKey: path];
		  RELEASE(handler);
		}
	    }
	}
    }
  if (handler == nil)
    {
      NSString	*error = @"bad path";

      /*
       * Return status code 400 (Bad Request) with the informative error
       */
      error = [NSString stringWithFormat: @"HTTP/1.0 400 %@", error];
      [response setHeader: @"http" value: error parameters: nil];
      return YES;
    }
  else
    {
      return [handler processRequest: request
			    response: response
				 for: http];
    }
}

- (void) webAlert: (NSString*)message for: (WebServer*)http
{
  NSLog(@"%@", message);
}
@end

