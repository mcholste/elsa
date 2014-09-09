if ( !Date.prototype.toISOString ) {
     
    ( function() {
     
        function pad(number) {
            var r = String(number);
            if ( r.length === 1 ) {
                r = '0' + r;
            }
            return r;
        }
  
        Date.prototype.toISOString = function() {
            return this.getUTCFullYear()
                + '-' + pad( this.getUTCMonth() + 1 )
                + '-' + pad( this.getUTCDate() )
                + 'T' + pad( this.getUTCHours() )
                + ':' + pad( this.getUTCMinutes() )
                + ':' + pad( this.getUTCSeconds() )
                + '.' + String( (this.getUTCMilliseconds()/1000).toFixed(3) ).slice( 2, 5 )
                + 'Z';
        };
   
    }() );
}

/**
 * Similar to Perl's keys function, returns 
 * the properties of an object as an array
 * @return	Array	object properties array
 */
function keys(obj){
	var ks = [];
	for(var k in obj){
		ks.push(k);
	}
	return ks;
}

function values(obj){
	var ks = [];
	for(var k in obj){
		ks.push(obj[k]);
	}
	return ks;
}

function sortByValueDesc(obj){
	var aRet = values(obj);
	aRet.sort(sortNumberDesc);
	return aRet;
}

function sortByKeyDesc(obj,key){
	var aTuples = [];
	for (var i in obj){
		aTuples.push([i, obj[i][key]]);
	}
	aTuples.sort(function(a, b) {
	    a = a[1];
	    b = b[1];
	
	    return a > b ? -1 : (a < b ? 1 : 0);
	});
	var aRet = [];
	for (var i in aTuples){
		aRet.push(aTuples[i][0]);
	}
	return aRet;
}

function sortNumberAsc(a, b){
	return a - b;
}
function sortNumberDesc(a, b){
	return b - a;
}

/**
 * Since javascript only copies by reference 
 * for objects, this function will create a 
 * copy by value
 * @param	mixed	Give it anything.
 * @return	mixed	Returns a real copy.
 */
function cloneVar(original){
	var t = typeof(original);
	if(t=='undefined' || t=='integer' || t=='string' || t=='number' || !original){
		//should copy without issue
		return original;
	}else if(t=='object'){
		if(original){
			var clone = (original instanceof Array) ? [] : {};
			if(isHTMLElement(original)){
				clone = original.cloneNode(true);
				return clone;
			}
			for(var prop in original){
				clone[prop] = cloneVar(original[prop]);
			}
			return clone;
		}else{
			//NULL
			return original;
		}
	}else{
		if(YAHOO && typeof(logger)=='function'){
			logger.log('Unable to copy variable:' + original + ' of type:' + t);
		}
	}
	
}

/**
* This function will escape potentially harmful HTML characters that Javascript wants to print to the page ("&"=>"&amp;", "<"=>"&lt;", ">"=>"&gt;")
* @param	string	text to escape
* @return	string	escaped text
*/
function escapeHTML(text){
	if(!text || typeof(text)!='string'){
		//var logger = new YAHOO.ELSA.ConsoleProvider();
		//logger.log('escapeHTML was expecting a string, received: ', text);
		try{
			text = text.toString();
		}catch(e){
			return '';
		}
	}
	return text.split("&").join("&amp;").split( "<").join("&lt;").split(">").join("&gt;").split('"').join("&quot;");
}

/**
 * Takes a date object and formats it in ISO timestamp
 * YYYY/MM/DD HH:ii:ss
 * @param Date
 * @return String
 */
function formatDateTimeAsISO(eD){
	if(eD instanceof Date){
		return getISODate(eD)+' '+getISOTime(eD);
	}
	else if(eD.toString().match(/^\d{10}$/)){
		var p = new Date(eD*1000);
		if(p){
			var d = new Date();
			d.setTime(p);
			return getISODate(d)+' '+getISOTime(d);
		}
	}
	else if(eD){
		var p = Date.parse(eD);
		if(p){
			var d = new Date();
			d.setTime(p);
			return getISODate(d)+' '+getISOTime(d);
		}
	}
	return false;
}
function getISODate(oDate){
	if(oDate instanceof Date){
		return (parseInt(oDate.getFullYear()))+'-'+(oDate.getMonth() < 9 ? '0'+(1+parseInt(oDate.getMonth())): (1+parseInt(oDate.getMonth())))+'-'+(oDate.getDate() < 10 ? '0'+oDate.getDate() : oDate.getDate());
	}
}
function getISOTime(oDate){
	if(oDate instanceof Date){
		return (oDate.getHours() < 10 ? '0'+oDate.getHours() : oDate.getHours())+':'+(oDate.getMinutes() < 10 ? '0'+oDate.getMinutes() : oDate.getMinutes())+':'+(oDate.getSeconds() < 10 ? '0'+oDate.getSeconds() : oDate.getSeconds());
	}
}

function getISODateTime(oDate, bUTC){
	if (!oDate){
		oDate = new Date();
	}
	if (bUTC){
		var sIso = oDate.toISOString();
		sIso = sIso.replace('T', ' ').replace('Z', '');
		return sIso;
	}
	return getISODate(oDate) + ' ' + getISOTime(oDate);
}

function getDateFromISO(sIsoDate, bUTC){
	var date = new Date();
	var re = new RegExp(/(\d{4})\-(\d{1,2})\-(\d{1,2})\s+(\d{1,2})\:(\d{1,2})\:(\d{1,2})/);
	var aMatches = re.exec(sIsoDate);
	if (typeof(aMatches) == 'undefined' || aMatches == null){
		return false;
	}
	// strip ny leading zeros before int parsing
	for (var i in aMatches){
		var str = '' + aMatches[i];
		str = str.replace(/^0(\d)/, '$1');
		aMatches[i] = str;
	}
	if (bUTC){
		date.setUTCFullYear(parseInt(aMatches[1]));
		date.setUTCDate(parseInt(aMatches[3]));
		date.setUTCHours(parseInt(aMatches[4]));
		date.setUTCMinutes(parseInt(aMatches[5]));
		date.setUTCSeconds(parseInt(aMatches[6]));
		date.setUTCMonth(parseInt(aMatches[2]) - 1); //must do these last in case the month we had doesn't have the day we've set
		date.setUTCDate(parseInt(aMatches[3]));
	}
	else {
		date.setFullYear(parseInt(aMatches[1]));
		date.setDate(parseInt(aMatches[3]));
		date.setHours(parseInt(aMatches[4]));
		date.setMinutes(parseInt(aMatches[5]));
		date.setSeconds(parseInt(aMatches[6]));
		date.setMonth(parseInt(aMatches[2]) - 1); //must do these last in case the month we had doesn't have the day we've set
		date.setDate(parseInt(aMatches[3]));
	}
	return date;
}

function getJSDateTime(iEpoch){
	var oDate = new Date(iEpoch * 1000);
	return (oDate.getMonth() + 1) + '/' + oDate.getDate() + '/' + oDate.getFullYear() + ' ' + oDate.getHours() + ':' + oDate.getMinutes() + ':' + oDate.getSeconds();
}

function isHTMLElement(elem){
	//Elements are of nodeType 1
	if(elem && elem.nodeType && elem.nodeType==1){ return true;}else{return false;}
}

RegExp.escape = function(text) {
  if (!arguments.callee.sRE) {
    var specials = [
      '/', '.', '*', '+', '?', '|',
      '(', ')', '[', ']', '{', '}', '\\'
    ];
    arguments.callee.sRE = new RegExp(
      '(\\' + specials.join('|\\') + ')', 'g'
    );
  }
  return text.replace(arguments.callee.sRE, '\\$1');
}

function str_repeat(i, m) { for (var o = []; m > 0; o[--m] = i); return(o.join('')); }

function sprintf () {
  var i = 0, a, f = arguments[i++], o = [], m, p, c, x;
  while (f) {
    if (m = /^[^\x25]+/.exec(f)) o.push(m[0]);
    else if (m = /^\x25{2}/.exec(f)) o.push('%');
    else if (m = /^\x25(?:(\d+)\$)?(\+)?(0|'[^$])?(-)?(\d+)?(?:\.(\d+))?([b-fosuxX])/.exec(f)) {
      if (((a = arguments[m[1] || i++]) == null) || (a == undefined)) throw("Too few arguments.");
      if (/[^s]/.test(m[7]) && (typeof(a) != 'number'))
        throw("Expecting number but found " + typeof(a));
      switch (m[7]) {
        case 'b': a = a.toString(2); break;
        case 'c': a = String.fromCharCode(a); break;
        case 'd': a = parseInt(a); break;
        case 'e': a = m[6] ? a.toExponential(m[6]) : a.toExponential(); break;
        case 'f': a = m[6] ? parseFloat(a).toFixed(m[6]) : parseFloat(a); break;
        case 'o': a = a.toString(8); break;
        case 's': a = ((a = String(a)) && m[6] ? a.substring(0, m[6]) : a); break;
        case 'u': a = Math.abs(a); break;
        case 'x': a = a.toString(16); break;
        case 'X': a = a.toString(16).toUpperCase(); break;
      }
      a = (/[def]/.test(m[7]) && m[2] && a > 0 ? '+' + a : a);
      c = m[3] ? m[3] == '0' ? '0' : m[3].charAt(1) : ' ';
      x = m[5] - String(a).length;
      p = m[5] ? str_repeat(c, x) : '';
      o.push(m[4] ? a + p : p + a);
    }
    else throw ("Huh ?!");
    f = f.substring(m[0].length);
  }
  return o.join('');
}

var defined = function(variable){
	//logger.log('variable: ' + variable + ', typeof: ' + (typeof variable));
	if (typeof variable != 'undefined' && variable != '' && variable != null){
		return true;
	}
	return false;
}

/**
*
*  Base64 encode / decode
*  http://www.webtoolkit.info/
*
**/
 
var Base64 = {
 
	// private property
	_keyStr : "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=",
 
	// public method for encoding
	encode : function (input) {
		var output = "";
		var chr1, chr2, chr3, enc1, enc2, enc3, enc4;
		var i = 0;
 
		input = Base64._utf8_encode(input);
 
		while (i < input.length) {
 
			chr1 = input.charCodeAt(i++);
			chr2 = input.charCodeAt(i++);
			chr3 = input.charCodeAt(i++);
 
			enc1 = chr1 >> 2;
			enc2 = ((chr1 & 3) << 4) | (chr2 >> 4);
			enc3 = ((chr2 & 15) << 2) | (chr3 >> 6);
			enc4 = chr3 & 63;
 
			if (isNaN(chr2)) {
				enc3 = enc4 = 64;
			} else if (isNaN(chr3)) {
				enc4 = 64;
			}
 
			output = output +
			this._keyStr.charAt(enc1) + this._keyStr.charAt(enc2) +
			this._keyStr.charAt(enc3) + this._keyStr.charAt(enc4);
 
		}
 
		return output;
	},
 
	// public method for decoding
	decode : function (input) {
		var output = "";
		var chr1, chr2, chr3;
		var enc1, enc2, enc3, enc4;
		var i = 0;
 
		input = input.replace(/[^A-Za-z0-9\+\/\=]/g, "");
 
		while (i < input.length) {
 
			enc1 = this._keyStr.indexOf(input.charAt(i++));
			enc2 = this._keyStr.indexOf(input.charAt(i++));
			enc3 = this._keyStr.indexOf(input.charAt(i++));
			enc4 = this._keyStr.indexOf(input.charAt(i++));
 
			chr1 = (enc1 << 2) | (enc2 >> 4);
			chr2 = ((enc2 & 15) << 4) | (enc3 >> 2);
			chr3 = ((enc3 & 3) << 6) | enc4;
 
			output = output + String.fromCharCode(chr1);
 
			if (enc3 != 64) {
				output = output + String.fromCharCode(chr2);
			}
			if (enc4 != 64) {
				output = output + String.fromCharCode(chr3);
			}
 
		}
 
		output = Base64._utf8_decode(output);
 
		return output;
 
	},
 
	// private method for UTF-8 encoding
	_utf8_encode : function (string) {
		string = string.replace(/\r\n/g,"\n");
		var utftext = "";
 
		for (var n = 0; n < string.length; n++) {
 
			var c = string.charCodeAt(n);
 
			if (c < 128) {
				utftext += String.fromCharCode(c);
			}
			else if((c > 127) && (c < 2048)) {
				utftext += String.fromCharCode((c >> 6) | 192);
				utftext += String.fromCharCode((c & 63) | 128);
			}
			else {
				utftext += String.fromCharCode((c >> 12) | 224);
				utftext += String.fromCharCode(((c >> 6) & 63) | 128);
				utftext += String.fromCharCode((c & 63) | 128);
			}
 
		}
 
		return utftext;
	},
 
	// private method for UTF-8 decoding
	_utf8_decode : function (utftext) {
		var string = "";
		var i = 0;
		var c = c1 = c2 = 0;
 
		while ( i < utftext.length ) {
 
			c = utftext.charCodeAt(i);
 
			if (c < 128) {
				string += String.fromCharCode(c);
				i++;
			}
			else if((c > 191) && (c < 224)) {
				c2 = utftext.charCodeAt(i+1);
				string += String.fromCharCode(((c & 31) << 6) | (c2 & 63));
				i += 2;
			}
			else {
				c2 = utftext.charCodeAt(i+1);
				c3 = utftext.charCodeAt(i+2);
				string += String.fromCharCode(((c & 15) << 12) | ((c2 & 63) << 6) | (c3 & 63));
				i += 3;
			}
 
		}
 
		return string;
	}
 
}