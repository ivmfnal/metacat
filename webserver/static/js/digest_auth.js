//
// RFC2617 digest authentication client implementation
//

function HTTPRequest(url, callback, headers={})
{
	// format can be "json"
	var http_request = null;
	if (window.XMLHttpRequest)
		http_request=new XMLHttpRequest();      // code for IE7+, Firefox, Chrome, Opera, Safari
	else
		http_request=new ActiveXObject("Microsoft.XMLHTTP");  // code for IE6, IE5

    for( var hdr in headers )
        http_request.setRequestHeader(hdr, headers[hdr]);
    
	var state_change = function ()
	{
		if (this.readyState==4 )
        {
            if( this.status/100 == 2 )
                callback.succeeded(this);
            else if( this.status == 401 )
                callback.continue(this);
            else
                callback.failed(this);
        }
	}
	http_request.open("GET", url, true);
	http_request.send();
	return http_request;
}    

function digest_request_handler(url, uri, username, password, done, failed)
{
    this.url = url;
    this.username = username;
    this.password = password;
    
    this.start = function()
    {
        request = new HTTPRequest(url, this);
    }
    
    this.failed(response)
    {
        console.log("succeeded");      
        failed(this);  
    }
    
    this.continue = function(response)
    {
        var auth_header = response.getResponseHeader("WWW-Authenticate");
        if( auth_header == null )
            return failed(response, "null WWW-Authenticate header");

        var words = auth_header.split(" ", 1);
        if( words.length < 2 )
            return failed(response, "invalid WWW-Authenticate header: " + auth_header);
        if( words[0] != "Digest" )
            return failed(response, "invalid WWW-Authenticate header: unknown algorithm: " + words[0]);

        words = words[1]split(", ");
        if( words.length < 3 )
            return failed(response, "invalid WWW-Authenticate header: " + auth_header);
        
        var parsed={};

        for ( var w of words )
        {
            var parts = w.split('="', 1);
            if( parts.length != 2 )
                return failed(response, "invalid WWW-Authenticate header: " + auth_header);
            var name = parts[0];
            var value = parts[1].substring(0, value.length-1);      // remove training quote
            parsed[name] = value;
        }
        
        if( parsed.realm == null || parsed.nonce == null || parsed.algorithm != "MD5" )
            return failed(response, "invalid WWW-Authenticate header: " + auth_header);

        var nc = "00000001";
        var cnonce = parsed.nonce;
        
        var hashed_password = password_hash(parsed.realm, this.username, this.password);
        var method_uri_hash = md5("GET:" + this.uri);
        var auth_response = md5(
                hashed_password + ":" + 
                parsed.nonce + ":" + 
                nc + ":" +
                cnonce + ":" +
                parsed.qop + ":" +
                method_uri_hash
                );

        headers["Authorization"] = "Digest " + \
            'username="' + this.username + '", ' + \
            'realm="' + parsed.realm + '", ' + \
            'nonce="' + parsed.nonce + '", ' + \
            'uri="' + this.uri + '", ' + \
            'cnonce="' + cnonce + '", ' + \
            'nc=' + nc + ', ' + \
            'qop=' + parsed.qop + ', ' + \
            'response="' + auth_response + '", ' + \
            'algorithm="' + parsed.algorithm + "'";
            
        request = HTTPRequest(url, this, headers);
    }
    
    this.succeeded = function(response)
    {
        console.log("succeeded");
        done(this);
    }
}

function authenticate_digest(url)
{
    var handler = new digest_request_handler("http://localhost:8443/auth", "/auth", "ivm", "hello");
    handler.start();
}