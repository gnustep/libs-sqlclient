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

<title>WebServer documentation</title>
<chapter>
  <heading>The WebServer class</heading>
  <section>
    <heading>What is the WebServer class?</heading>
    <p>
      The WebServer class provides the framework for a GNUstep program to
      act as an HTTP or HTTPS server for simple applications.<br />
      It does not attempt to be a general-purpose web server, but is rather
      intended to permit a program to easily handle requests from automated
      systems which are intended to control, monitor, or use the services
      provided by the program in which the class is embedded.<br />
      In particular of course, it may be used in conjunction with the
      [SQLClient] class to implement web-based database applications.
    </p>
    <p>
      The class is controlled by a few straightforward settings and
      basically operates by handing over requests to its delegate.
      The delegate must at least implement the
      [(WebServerDelegate)-processRequest:response:for:] method.
    </p>
  </section>
</chapter>

   $Date$ $Revision$
   */ 

#ifndef	INCLUDED_WEBSERVER_H
#define	INCLUDED_WEBSERVER_H

#include	<Foundation/NSObject.h>
#include	<Foundation/NSMapTable.h>
#include	<Foundation/NSDictionary.h>
#include	<Foundation/NSFileHandle.h>
#include	<Foundation/NSNotification.h>
#include	<Foundation/NSArray.h>
#include	<Foundation/NSSet.h>
#include	<Foundation/NSTimer.h>
#include	<GNUstepBase/GSMime.h>

@class	WebServer;

/**
 * This protocol is implemented by a delegate of a WebServer instance
 * in order to allow the delegate to process requests which arrive
 * at the server.
 */
@protocol	WebServerDelegate
/**
 * Process the http request whose headers and data are provided in
 * a GSMimeDocument.<br />
 * Extra headers are created as follows -
 * <deflist>
 *   <term>x-http-method</term>
 *   <desc>The method from the HTTP request (eg. GET or POST)</desc>
 *   <term>x-http-path</term>
 *   <desc>The path from the HTTP request, or an empty string if
 *     there was no path.</desc>
 *   <term>x-http-query</term>
 *   <desc>The query string from the HTTP request or an empty string
 *     if there was no query.</desc>
 *   <term>x-http-version</term>
 *   <desc>The version from the HTTP request.</desc>
 *   <term>x-local-address</term>
 *   <desc>The IP address of the local host receiving the request.</desc>
 *   <term>x-local-port</term>
 *   <desc>The port of the local host receiving the request.</desc>
 *   <term>x-remote-address</term>
 *   <desc>The IP address of the host that the request came from.</desc>
 *   <term>x-remote-port</term>
 *   <desc>The port of the host that the request came from.</desc>
 * </deflist>
 * On completion, the method must modify response to contain the data
 * and headers to be sent out.<br />
 * The 'content-length' header need not be set in the response as it will
 * be overridden anyway.<br />
 * The special 'http' header will be used as the response/status line.
 * If not supplied, 'HTTP/1.1 200 Success' or 'HTTP/1.1 204 No Content' will
 * be used as the response line, depending on whether the data is empty or
 * not.<br />
 * If an exception is raised by this method, the response produced will
 * be set to 'HTTP/1.0 500 Internal Server Error' and the connection will
 * be closed.
 */
- (BOOL) processRequest: (GSMimeDocument*)request
	       response: (GSMimeDocument*)reponse
		    for: (WebServer*)http;
/**
 * Log an error or warning ... if the delegate does not implement this
 * method, the message is logged to stderr using the NSLog function.
 */
- (void) webAlert: (NSString*)message for: (WebServer*)http;
@end

/**
 * You create an instance of the WebServer class in order to handle
 * incoming http or https requests on a single port.<br />
 * Before use, it must be configured using the -setPort:secure: method
 * to specify the port and if/how ssl is to be used.<br />
 * You must also set a delegate to handle incoming requests,
 * and may specify a maximum number of simultaneous connections
 * which may be in progress etc.
 */
@interface	WebServer : NSObject
{
@private
  NSNotificationCenter	*_nc;
  NSString		*_port;
  BOOL			_accepting;
  BOOL			_verbose;
  NSDictionary		*_sslConfig;
  NSArray		*_hosts;
  unsigned int		_substitutionLimit;
  unsigned int		_maxBodySize;
  unsigned int		_maxRequestSize;
  unsigned int		_maxSessions;
  unsigned int		_maxPerHost;
  id			_delegate;
  NSFileHandle		*_listener;
  NSMapTable		*_sessions;
  unsigned		_handled;
  NSString		*_root;
  NSTimer		*_ticker;
  NSTimeInterval	_sessionTimeout;
  NSTimeInterval	_ticked;
  NSCountedSet		*_perHost;
}

/**         
 * Decode an application/x-www-form-urlencoded form and store its
 * contents into the supplied dictionary.<br />
 * The resulting dictionary keys are strings.<br />
 * The resulting dictionary values are arrays of NSData objects.<br />
 * You probably don't need to call this method yourself ... more likely
 * you will use the -parameters: method instead.<br />
 * NB. For forms POSTed using <code>multipart/form-data</code> you don't
 * need to perform any explicit decoding as this will already have been
 * done for you and the decoded form will be presented as the request
 * GSMimeDocument.  The fields of the form will be the component parts
 * of the content of the request and can be accessed using the standard
 * GSMimeDocument methods.<br />
 * This method returns the number of fields actually decoded.
 */         
- (unsigned) decodeURLEncodedForm: (NSData*)data
			     into: (NSMutableDictionary*)dict;

/**
 * Returns YES if the server is for HTTPS (encrypted connections),
 * NO otherwise.
 */
- (BOOL) isSecure;

/**
 * Extracts request parameters from the http query string and from the
 * request body (if it was application/x-www-form-urlencoded or
 * multipart/form-data) and return the extracted parameters as a
 * mutable dictionary whose keys are the parameter names and whose
 * values are arrays containing the data for each parameter.<br />
 * You should call this no more than once per request, storing the result
 * and using it as an argument to the methods used to extract particular
 * parameters.<br />
 * Parameters from the request data are <em>added</em> to any found in the
 * query string.<br />
 * Values provided as <code>multipart/form-data</code> are also available
 * in a more flexible format as the content of the request.
 */
- (NSMutableDictionary*) parameters: (GSMimeDocument*)request;

/**
 * Returns the index'th data parameter for the specified name.<br />
 * Matching of names is case-insensitive<br />
 * If there are no data items for the name, or if the index is
 * too large for the number of items which exist, this returns nil.
 */
- (NSData*) parameter: (NSString*)name
		   at: (unsigned)index
		 from: (NSDictionary*)params;

/**
 * Calls -parameter:at:from: with an index of zero.
 */
- (NSData*) parameter: (NSString*)name from: (NSDictionary*)params;

/**
 * Calls -parameterString:at:from:charset: with a nil charset so that
 * UTF-8 encoding is used for string conversion.
 */
- (NSString*) parameterString: (NSString*)name
			   at: (unsigned)index
			 from: (NSDictionary*)params;
/**
 * Calls -parameter:at:from: and, if the result is non-nil
 * converts the data to a string using the specified mime
 * characterset, (if charset is nil, UTF-8 is used).
 */
- (NSString*) parameterString: (NSString*)name
			   at: (unsigned)index
			 from: (NSDictionary*)params
		      charset: (NSString*)charset;
/**
 * Calls -parameterString:at:from:charset: with an index of zero and
 * a nil value for charset (which causes data to be treated as UTF-8).
 */
- (NSString*) parameterString: (NSString*)name
			 from: (NSDictionary*)params;

/**
 * Calls -parameterString:at:from:charset: with an index of zero.
 */
- (NSString*) parameterString: (NSString*)name
			 from: (NSDictionary*)params
		      charset: (NSString*)charset;

/**
 * Loads a template file from disk and places it in aResponse as content
 * of type 'text/html' with a charset of 'utf-8'.<br />
 * The argument aPath is a path relative to the root path set using
 * the -setRoot: method.<br />
 * Substitutes values into the template from map using the
 * -substituteFrom:using:into:depth: method.<br />
 * Returns NO if them template could not be read or if any substitution
 * failed.  In this case no value is set in the response.<br />
 * If the response is actually text of another type, or you want another
 * characterset used, you can change the content type header in the
 * request after you call this method.
 */
- (BOOL) produceResponse: (GSMimeDocument*)aResponse
	    fromTemplate: (NSString*)aPath
		   using: (NSDictionary*)map;

/**
 * Sets the delegate object which processes requests for the receiver.
 */
- (void) setDelegate: (id)anObject;

/**
 * Sets the maximum size of an uploaded request body.<br />
 * The default is 4M bytes.<br />
 */
- (void) setMaxBodySize: (unsigned)max;

/**
 * Sets the maximum size of an incoming request (including all headers,
 * but not the body).<br />
 * The default is 8K bytes.<br />
 */
- (void) setMaxRequestSize: (unsigned)max;

/**
 * Sets the maximum number of simultaneous sessions with clients.<br />
 * The default is 32.<br />
 * A value of zero permits unlimited connections.
 */
- (void) setMaxSessions: (unsigned)max;

/**
 * Sets the maximum number of simultaneous sessions with a particular
 * remote host.<br />
 * The default is 8.<br />
 * A value of zero permits unlimited connections.
 */
- (void) setMaxSessionsPerHost: (unsigned)max;

/**
 * Sets the port and security information for the receiver ... without
 * this the receiver will not listen for incoming requests.<br />
 * If secure is nil then the receiver listens on aPort for HTTP requests.<br />
 * If secure is not nil, the receiver listens for HTTPS instead.<br />
 * If secure is a dictionary containing <code>CertificateFile</code>,
 * <code>KeyFile</code> and <code>Password</code> then the server will
 * use the specified certificate and key files (which it will access
 * using the password).<br />
 * This method returns YES on success, NO on failure ... if it returns NO
 * then the receiver will <em>not</em> be capable of handling incoming
 * web requests!<br />
 * Typically a failure will be due to an invalid port being specified ...
 * a port may not already be in use and may not be in the range up to 1024
 * (unless running as the super-user).
 */
- (BOOL) setPort: (NSString*)aPort secure: (NSDictionary*)secure;

/**
 * Sets the maximum recursion depth allowed for subsititutions into
 * templates.  This defaults to 4.
 */
- (void) setSubstitutionLimit: (unsigned)depth;

/**
 * Set root path for loading template files from.<br />
 * Templates may only be loaded from within this directory.
 */
- (void) setRoot: (NSString*)aPath;

/**
 * Sets the time after which an idle session should be shut down.<br />
 * Default is 30.0
 */
- (void) setSessionTimeout: (NSTimeInterval)aDelay;

/**
 * Sets a flag to determine whether verbose logging is to be performed.<br />
 * If this is YES then all incming requests and their responses will
 * be logged using the NSLog function.  This is useful for debugging
 * and where a full audit trail is required.
 */
- (void) setVerbose: (BOOL)aFlag;

/**
 * Perform substituations replacing the markup in aTemplate with the
 * values supplied by map and appending the results to the result.<br />
 * Substitutions are recursive, and the depth argument is used to
 * specify the current recursion depth (you should normally call this
 * method with a depth of zero at the start of processing a template).<br />
 * Any value inside SGML comment delimiters ('&lt;!--' and '--&gt;') is
 * treated as a possible key in map and the entire comment is replaced
 * by the corresponding map value (unless it is nil).  Recursive substitution
 * is done unless the mapped value <em>starts</em> with an SGML comment.<br />
 * While the map is nominally a dictionary, in fact it may be any
 * object which responds to the objectForKey: method by returning
 * an NSString or nil.<br />
 * The method returns YES on success, NO on failure (depth too great).<br />
 * You don't normally need to use this method directly ... call the
 * -produceResponse:fromTemplate:using: method instead.
 */
- (BOOL) substituteFrom: (NSString*)aTemplate
		  using: (NSDictionary*)map
		   into: (NSMutableString*)result
		  depth: (unsigned)depth;

@end

/**
 * WebServerBundles is an example delegate for the WebServer class.<br />
 * This is intended to act as a convenience for a scheme where the
 * WebServer instance in a program is configured by values obtained
 * from the user defaults system, and incoming requests may be handled
 * by different delegate objects depending on the path information
 * supplied in the request.  The WebServerBundles intance is responsible
 * for loading the bundles (based on information in the WebServerBundles
 * dictionary in the user defaults system) and for forwarding requests
 * to the appropriate bundles for processing.
 */
@interface	WebServerBundles : NSObject <WebServerDelegate>
{
  NSMutableDictionary	*_handlers;
  WebServer		*_http;
}

/**
 * Handle a notification that the defaults have been updated ... change
 * WebServer configuration if necessary.<br />
 * <list>
 *   <item>
 *     WebServerPort must be used to specify the port that the server
 *     listens on.  See [WebServer-setPort:secure:] for details.
 *   </item>
 *   <item>
 *     WebServerSecure may be supplied to make the server operate as an
 *     HTTPS server rather than an HTTP server.
 *     See [WebServer-setPort:secure:] for details.
 *   </item>
 * </list>
 * Returns YES on success, NO on failure (if the port of the WebServer
 * cannot be set).
 */
- (BOOL) defaultsUpdate: (NSNotification *)aNotification;

/**
 * Return dictionary of all handlers by name (path in request which maps
 * to that handler instance).
 */
- (NSMutableDictionary*) handlers;

/**
 * Return the WebServer instance that the receiver is actiang as a
 * delegate for.
 */
- (WebServer*) http;

/** <init />
 * Initialises the receiver as the delegate of http and configures
 * the WebServer based upon the settings found in the user defaults
 * system by using the -defaultsUpdate: method.
 */
- (id) initAsDelegateOf: (WebServer*)http;

/**
 * Handles an incoming request by forwarding it to another handler.<br />
 * If a direct mapping is available from the path in the request to
 * an existing handler, that handler is used to process the request.
 * Otherwise, the WebServerBundles dictionary (obtained from the
 * defaults system) is used to map the request path to configuration
 * information listing the bundle containing the handler to be used.<br />
 * The configuration information is a dictionary containing the name
 * of the bundle (keyed on 'Name'), and this is used to locate the
 * bundle in the applications resources.
 */
- (BOOL) processRequest: (GSMimeDocument*)request
               response: (GSMimeDocument*)response
		    for: (WebServer*)http;

/**
 * Just write to stderr using NSLog.
 */
- (void) webAlert: (NSString*)message for: (WebServer*)http;
@end

#endif

