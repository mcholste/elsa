var lastRequest = {};
var aFormInputs = [ 'sql', 'title', 'type', 'host', 'db', 'user', 'pass' ];
var graphAnything = function(){
	var logger;
	YAHOO.ELSA.initLogger();
	drawForm();
}

var drawForm = function(){
	var oElForm = document.createElement('form');
	var oTable = document.createElement('table');
	var oTbody = document.createElement('tbody');
	oTable.appendChild(oTbody);
	var oElTextArea = document.createElement('textarea');
	oElTextArea.rows = 10;
	oElTextArea.cols = 80;
	oElTextArea.name = 'sql';
	oElTextArea.id = 'sql';
	oElForm.appendChild(oElTextArea);
	var aTextInputs = [ 'title', 'type', 'host', 'db', 'user', 'pass' ];
	for (i in aTextInputs){
		var sText = aTextInputs[i];
		var oTr = document.createElement('tr');
		oTbody.appendChild(oTr);
		var oTd = document.createElement('td');
		oTd.innerHTML = sText.toUpperCase(); 
		oTr.appendChild(oTd);
		var oTd2 = document.createElement('td');
		var oEl = document.createElement('input');
		oEl.name = sText;
		oEl.id = sText;
		if (sText == 'pass'){
			oEl.type = 'password';
		}
		else {
			oEl.type = 'text';
		}
		oTd2.appendChild(oEl);
		oTr.appendChild(oTd2);
	}
	oElForm.appendChild(oTable);
	var oElDiv = document.createElement('div');
	
	var oSubmitEl = new YAHOO.widget.Button({
		id: 'post_chart_data_submit',
		label: 'Submit',
		container: oElDiv,
		onclick: {
			fn: sendData,
			obj: this
		}
	});
	oElForm.appendChild(oElDiv);
	YAHOO.util.Dom.get('form').appendChild(oElForm);
}

var sendData = function(p_oEvent){
	var aValues = [];
	var reqStr = '';
	lastRequest = {};
	for (var i in aFormInputs){
		var el = YAHOO.util.Dom.get(aFormInputs[i]);
		//logger.log('i: ' + i + ', ele: ' + aElements[i].name);
		var value = encodeURIComponent(el.value);
		reqStr += el.name + '=' + value + '&';
		lastRequest[ el.name ] = value;
	}
	//logger.log('reqstr: ' + reqStr);
	var request = YAHOO.util.Connect.asyncRequest('POST', 'Chart/sql', 
		{ success:makeChart, failure:makeChart },  reqStr);
}

var makeChart = function(p_oResponse){
	if(p_oResponse.responseText !== undefined && p_oResponse.responseText){
		logger.log('rawResponse: ' + p_oResponse.responseText);
		var oRawChartData = YAHOO.lang.JSON.parse(p_oResponse.responseText);
		logger.log('oRawChartData', oRawChartData);
		var divId = 'chart';
		var oChart = new YAHOO.ELSA.Chart.Auto({container:divId, type:lastRequest['type'], title:lastRequest['title'], data:oRawChartData});
	}
	else {
		YAHOO.ELSA.Error('Did not receive form params');
		return false;
	}
}
