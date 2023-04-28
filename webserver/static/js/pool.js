function HTTPRequest(url, callback_object, param, format="")
{
	// format can be "json"
	var http_request = null;
	if (window.XMLHttpRequest)
		http_request=new XMLHttpRequest();      // code for IE7+, Firefox, Chrome, Opera, Safari
	else
		http_request=new ActiveXObject("Microsoft.XMLHTTP");  // code for IE6, IE5
	var state_change = function ()
	{
	    var callback_object = state_change.callback_object;
	    var param = state_change.callback_param;
		if (this.readyState==4 )
        {
            if( this.status==200 && callback_object.data_received != null )
    		{
    		    var data = this.response;
    		    if ( data == null ) data = this.responseText;
    		    callback_object.data_received(data, param);
    		}
            else if ( callback_object.request_failed != null )
            {
                callback_object.request_failed(this.status, this.statusText, this.responseText, param);
            }
        }
	}
	state_change.callback_object = callback_object;
	state_change.callback_param = param;

	http_request.onreadystatechange = state_change;
	http_request.responseType = format
	http_request.open("GET", url, true);
	http_request.send();
	return http_request;
}    

class RequestQueue
{
	constructor(max_active)
	{
		this.MaxActive = max_active;
		this.Active = [];
		this.Pending = [];
		this.I = 0;
	}
	
	remove_request(rid)
	{
		for( let i = 0; i < this.Active.length; )
			if( this.Active[i].rid == rid )
				this.Active.splice(i, 1);
			else
				i++;
		for( let i = 0; i < this.Pending.length; )
			if( this.Pending[i].rid == rid )
				this.Pending.splice(i, 1);
			else
				i++;
	}
	
	submit(url, callback_object, param, format="")
	{
		const rid = ++this.I
		this.Pending.push({ url:url, rid:rid, format:format, callback:callback_object, param:param });
		this.start_next();
	}
	
	data_received(data, desc)
	{
		desc.callback.data_received(data, desc.param);
		this.remove_request(desc.rid);
		this.start_next();
	}
	
    request_failed(status, statusText, responseText, desc)
	{
		desc.callback.data_received(status, statusText, responseText, desc.param);
		this.remove_request(desc.rid);
		this.start_next();
	}
	
	start_next()
	{
		while( this.Pending.length > 0 && this.Active.length < this.MaxActive )
		{
			let desc = this.Pending.shift();
			if( desc !== undefined )
			{
				desc.request = HTTPRequest(desc.url, this, desc, desc.format);
				this.Active.push(desc);
			}
		}
	}
	
	cancel()
	{
		this.Active = this.Pending = [];
	}
}