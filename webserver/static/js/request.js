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
		if (this.readyState==4 && this.status==200)
		{
		    var data = this.response;
		    if ( data == null ) data = this.responseText;
		    callback_object.data_received(data, param);
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

