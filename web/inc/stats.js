YAHOO.namespace('YAHOO.ELSA.Stats');

YAHOO.ELSA.Stats.main = function(){
	// Set viewMode for dev/prod
	var oRegExp = new RegExp('\\Wview=(\\w+)');
	var oMatches = oRegExp.exec(location.search);
	if (oMatches){
		YAHOO.ELSA.viewMode = oMatches[1];
	}
	YAHOO.ELSA.initLogger(); 
	YAHOO.ELSA.Stats.getStats();
}

YAHOO.ELSA.Stats.getStats = function(p_iStart, p_iEnd){
	var oDate = new Date();
	var iStart;
	var iEnd;
	if (typeof p_iStart == 'undefined'){
		iStart = Math.round(oDate.getTime() / 1000) - (3600 * 24 * 7);
	}
	else {
		iStart = p_iStart;
	}
	if (typeof p_iEnd == 'undefined'){
		iEnd = Math.round(oDate.getTime() / 1000);
	}
	else {
		iEnd = p_iEnd;
	}
	
	var request = YAHOO.util.Connect.asyncRequest('GET', 'Query/get_stats?start=' + formatDateTimeAsISO(iStart) + '&end=' + formatDateTimeAsISO(iEnd),
		{ 
			success:function(oResponse){
				if (oResponse.responseText){
					var oReturn = YAHOO.lang.JSON.parse(oResponse.responseText);
					if (typeof oReturn === 'object' && oReturn['error']){
						YAHOO.ELSA.Error(oReturn['error']);
						return;
					}
					else if (oReturn){
						showQueryStats(oReturn);
						showNodeStats(oReturn.combined_load_stats);
					}
					else {
						logger.log(oReturn);
						YAHOO.ELSA.Error('Could not parse responseText: ' + oResponse.responseText);
					}
				}
				else {
					YAHOO.ELSA.Error('No response text');
				}
				
			}, 
			failure:function(oResponse){
				var oRequest = oResponse.argument[0];
				YAHOO.ELSA.Error('Query failed!'); 
				return false;
			},
			argument: [this]
		}
	);
	
	var showQueryStats = function(p_oData){
		// queries per user
		var oData = p_oData.queries_per_user;
		logger.log('oData', oData);
		var oChartContainer = document.createElement('div');
		oChartContainer.id = 'user_queries_chart';
		YAHOO.util.Dom.get('query_stats').appendChild(oChartContainer);
		var oChart = new YAHOO.ELSA.Chart.Auto({container:oChartContainer.id, type:'bar', title:'Queries per User', data:oData});
		
		// Averages
		oData = p_oData.query_stats;
		logger.log('oData', oData);
		var oChartContainer = document.createElement('div');
		oChartContainer.id = 'general_stats_chart';
		YAHOO.util.Dom.get('query_stats').appendChild(oChartContainer);
		var oChart = new YAHOO.ELSA.Chart.Auto({container:oChartContainer.id, type:'bar', title:'Query Stats', data:oData});
	}
	
	var showNodeStats = function(p_oData){
		logger.log('p_oData', p_oData);
	
		// arrange the data
		var aStatTypes = ['load', 'index', 'archive'];
		for (var iStat in aStatTypes){
			var sStat = aStatTypes[iStat];
			var oData = p_oData[sStat];
			var oChartContainer = document.createElement('div');
			oChartContainer.id = 'stats_chart_' + sStat;
			YAHOO.util.Dom.get('load_stats').appendChild(oChartContainer);
			logger.log('oData', oData);
			var oChart = new YAHOO.ELSA.Chart.Auto({container:oChartContainer.id, type:'bar', title:'Stats: ' + sStat, data:oData});
		}
	}
}
