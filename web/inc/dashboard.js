YAHOO.namespace('YAHOO.ELSA.Chart');

YAHOO.ELSA.timeTypes = {
	timestamp:1,
	minute:60,
	hour:3600,
	day:86400,
	year:(86400*365)
};

YAHOO.ELSA.timeTypeDrillDowns = {
	minute: 'seconds',
	hour: 'minutes',
	day: 'hours',
	year: 'days'
};

YAHOO.ELSA.chartOptions = {
	PieChart: {
		chartArea: {
			//height: '75%',
			//width: '75%'
			height: 500,
			width: 500,
			is3D: true
		}
	},
	BarChart: {
		vAxis: {
			textPosition: 'in'
		}
	},
	ColumnChart: {
		
	},
	AreaChart: {},
	Table: {},
	GeoChart: {}
}

YAHOO.ELSA.Dashboard = function(p_iId, p_sTitle, p_sAlias, p_oRows, p_sContainerId){
	
	this.id = p_iId;
	this.alias = p_sAlias;
	this.title = p_sTitle;
	this.rows = p_oRows;
	this.container = YAHOO.util.Dom.get(p_sContainerId);
	this.container.innerHTML = '';
	this.charts = {};
	
	var oElTitle = document.createElement('h1');
	oElTitle.innerHTML = this.title;
	this.container.appendChild(oElTitle);
	
	this.outsideTable = document.createElement('table');
	var oElOutsideTableEl = new YAHOO.util.Element(this.outsideTable);
	oElOutsideTableEl.setStyle('width', YAHOO.ELSA.dashboardParams.width + 'px');
	oElOutsideTableEl.addClass('overlay');
	this.outsideTBody = document.createElement('tbody');
	this.outsideTBody.id = p_sContainerId + '_tbody';
	this.outsideTable.appendChild(this.outsideTBody);
	this.container.appendChild(this.outsideTable);
	for (var i in this.rows){
		this.loadChartRow(i);
	}
	
	if (YAHOO.ELSA.editCharts){
		// Add the "finished" button
		var oElFinished = document.createElement('a');
		oElFinished.href = window.location.pathname;
		oElFinished.innerHTML = 'Finished Editing';
		this.container.insertBefore(oElFinished, this.outsideTable);		
		
		// Add the "add" button
		var oElAddChart = document.createElement('a');
		oElAddChart.href = '#';
		oElAddChart.innerHTML = 'Add Chart';
		this.container.appendChild(oElAddChart);
		var oElAddChartEl = new YAHOO.util.Element(oElAddChart);
		oElAddChartEl.on('click', this.addChart, this, this);
	}
}
	
YAHOO.ELSA.Dashboard.prototype.loadChartRow = function(p_iRowId){
	var oElOutsideRow = document.createElement('tr');
	this.outsideTBody.appendChild(oElOutsideRow);
	var oElOutsideCol = document.createElement('td');
	oElOutsideRow.appendChild(oElOutsideCol);
	var oElDiv = document.createElement('div');
	oElOutsideCol.appendChild(oElDiv);
	if (this.rows[p_iRowId].title){
		var oElRowTitle = document.createElement('h2');
		oElRowTitle.innerText = YAHOO.ELSA.dashboardRows[p_iRowId].title;
		oElDiv.appendChild(oElRowTitle);
		oElRowTitle = new YAHOO.util.Element(oElRowTitle);
		oElRowTitle.addClass('chart_title');
	}
	
	var oElTr;
	var oElTable = document.createElement('table');
	oElDiv.appendChild(oElTable);
	var oElTbody = document.createElement('tbody');
	oElTable.appendChild(oElTbody);
	oElTr = document.createElement('tr');
	oElTbody.appendChild(oElTr);
	for (var j in this.rows[p_iRowId].charts){
		var oElTd = document.createElement('td');
		oElTd.id = 'chart_container_' + this.rows[p_iRowId].charts[j].chart_id;
		oElTr.appendChild(oElTd);
		this.charts[ this.rows[p_iRowId].charts[j].chart_id ] = new YAHOO.ELSA.Chart(this.rows[p_iRowId].charts[j], oElTd, this);
	}
}
	
YAHOO.ELSA.Dashboard.prototype.edit = function(){
	YAHOO.ELSA.async('Charts/get?dashboard_id=' + this.id, drawTable);
	
	function drawTable(p_oData){
		if (!p_oData){
			return;
		}
		var oChartsDataSource = new YAHOO.util.DataSource(p_oData);
		oChartsDataSource.responseType = YAHOO.util.DataSource.TYPE_JSON;
		oChartsDataSource.responseSchema = {
			resultsList: 'charts',
			fields: ['chart_id', 'chart_title', 'chart_type', 'x', 'y' ],
			metaFields: {
				totalRecords: 'totalRecords',
				recordsReturned: 'recordsReturned'
			}
		};
		
		var oPanel = new YAHOO.ELSA.Panel('Charts');
		oPanel.panel.setHeader('Charts');
		oPanel.panel.render();
		
		var oElCreate = document.createElement('a');
	    oElCreate.href = '#';
	    oElCreate.innerHTML = 'Create new chart';
	    oPanel.panel.body.appendChild(oElCreate);
	    var oElCreateEl = new YAHOO.util.Element(oElCreate);
	    oElCreateEl.on('click', function(){
	    	logger.log('creating dashboard');
	    	var handleSubmit = function(p_sType, p_oDialog){
				this.submit();
			};
			var handleCancel = function(){
				this.hide();
			};
			var oCreatePanel = new YAHOO.ELSA.Panel('Create Dashboard', {
				buttons : [ { text:"Submit", handler:handleSubmit, isDefault:true },
					{ text:"Cancel", handler:handleCancel } ]
			});
			var handleSuccess = function(p_oResponse){
				var response = YAHOO.lang.JSON.parse(p_oResponse.responseText);
				if (response['error']){
					YAHOO.ELSA.Error(response['error']);
				}
				else {
					this.hide();
					YAHOO.ELSA.editDashboard.dataTable.load();
					logger.log('successful submission');
				}
			};
			oCreatePanel.panel.callback = {
				success: handleSuccess,
				failure: YAHOO.ELSA.Error
			};
			
			oCreatePanel.panel.renderEvent.subscribe(function(){
				oCreatePanel.panel.setBody('');
				oCreatePanel.panel.setHeader('Create New Dashboard');
				oCreatePanel.panel.bringToTop();
				//var sFormId = 'create_dashboard_form';
				var sFormId = oCreatePanel.panel.form.id;
				
				var sAuthButtonId = 'auth_select_button';
				var sAuthId = 'auth_input_connector';
				var onAuthMenuItemClick = function(p_sType, p_aArgs, p_oItem){
					var sText = p_oItem.cfg.getProperty("text");
					// Set the label of the button to be our selection
					var oAuthButton = YAHOO.widget.Button.getButton(sAuthButtonId);
					oAuthButton.set('label', sText);
					var oFormEl = YAHOO.util.Dom.get(sFormId);
					var oInputEl = YAHOO.util.Dom.get(sAuthId);
					oInputEl.setAttribute('value', p_oItem.value);
				}
				var onAuthMenuItemClickChooseGroups = function(p_sType, p_aArgs, p_oItem){
					var sText = p_oItem.cfg.getProperty("text");
					// Set the label of the button to be our selection
					var oAuthButton = YAHOO.widget.Button.getButton(sAuthButtonId);
					oAuthButton.set('label', sText);
					var oFormEl = YAHOO.util.Dom.get(sFormId);
					var oInputEl = YAHOO.util.Dom.get(sAuthId);
					oInputEl.setAttribute('value', p_oItem.value);
					oCreatePanel.panel.form.appendChild(document.createTextNode('Groups'));
					var oElNew = document.createElement('input');
					oElNew.name = 'groups';
					oElNew.id = 'auth_groups';
					oCreatePanel.panel.form.appendChild(oElNew);
				}
				var aAuthMenu = [
					{ text:'Public', value:0, onclick: { fn: onAuthMenuItemClick } },
					{ text:'Any authenticated user', value:1, onclick: { fn: onAuthMenuItemClick } },
					{ text:'Specific groups', value:2, onclick: { fn: onAuthMenuItemClickChooseGroups } }
				];
				
				var oAuthMenuButtonCfg = {
					id: sAuthButtonId,
					type: 'menu',
					label: 'Who has access',
					name: sAuthButtonId,
					menu: aAuthMenu
				};
				var oFormGridCfg = {
					form_attrs:{
						action: 'Charts/add_dashboard',
						method: 'POST',
						id: sFormId
					},
					grid: [
						[ {type:'text', args:'Title'}, {type:'input', args:{id:'dashboard_title', name:'title', size:32}} ],
						[ {type:'text', args:'Alias (end of URL for access)'}, {type:'input', args:{id:'dashboard_alias', name:'alias', size:32}} ],
						[ {type:'text', args:'Auth'}, {type:'widget', className:'Button', args:oAuthMenuButtonCfg} ]
					]
				};
				
				// Now build a new form using the element auto-generated by widget.Dialog
				var oForm = new YAHOO.ELSA.Form(oCreatePanel.panel.form, oFormGridCfg);
				
				var oInputEl = document.createElement('input');
				oInputEl.id = sAuthId;
				oInputEl.setAttribute('type', 'hidden');
				oInputEl.setAttribute('name', 'auth_required');
				oInputEl.setAttribute('value', 0);
				oForm.form.appendChild(oInputEl);
			});
			oCreatePanel.panel.render();
			oCreatePanel.panel.show();
	    });
		
		var deleteChart = function(p_sType, p_aArgs, p_a){
			var p_oRecord = p_a[0], p_oDataTable = p_a[1];
			YAHOO.ELSA.async('Charts/del?chart_id=' + p_oRecord.getData().chart_id, function(p_oReturn){
				if (!p_oReturn){
					return;
				}
				p_oDataTable.deleteRow(p_oRecord.getId());
			});
		};
		var editChartQueries = function(p_sType, p_aArgs, p_a){
			var p_oRecord = p_a[0], p_oDataTable = p_a[1];
			var oData = p_oRecord.getData();
			oData.recordSetId = p_oRecord.getId();
			logger.log('oData', oData);
			YAHOO.ELSA.editChartQueries(p_oData, oData.id);
		};
		var formatMenu = function(elLiner, oRecord, oColumn, oData){
			// Create menu for our menu button
			var oButtonMenuCfg = [
				{ 
					text: 'Delete', 
					value: 'delete', 
					onclick:{
						fn: deleteChart,
						obj: [oRecord,this]
					}
				},
				{ 
					text: 'Edit', 
					value: 'edit', 
					onclick:{
						fn: editChartQueries,
						obj: [oRecord,this]
					}
				}
			];
			
			var oButton = new YAHOO.widget.Button(
				{
					type:'menu', 
					label:'Actions',
					menu: oButtonMenuCfg,
					name: 'charts_menu_button',
					container: elLiner
				});
		};
		
		var oColumnDefs = [
			{ key:'menu', label:'Action', formatter:formatMenu },
			{ key:"chart_id", label:"ID", formatter:YAHOO.widget.DataTable.formatNumber, sortable:true },
			{ key:"chart_title", label:"Title", sortable:true },
			{ key:"chart_type", label:"Type", sortable:true },
			{ key:"x", label:"X", formatter:YAHOO.widget.DataTable.formatNumber, sortable:true },
			{ key:"y", label:"Y", formatter:YAHOO.widget.DataTable.formatNumber, sortable:true }
		];
		var oPaginator = new YAHOO.widget.Paginator({
		    pageLinks          : 10,
	        rowsPerPage        : 5,
	        rowsPerPageOptions : [5,20],
	        template           : "{CurrentPageReport} {PreviousPageLink} {PageLinks} {NextPageLink} {RowsPerPageDropdown}",
	        pageReportTemplate : "<strong>Records: {totalRecords} </strong> "
	    });
	    
	    var oDataTableCfg = {
	    	//initialRequest: 'startIndex=0&results=5',
	    	//initialLoad: true,
	    	//dynamicData: true,
	    	sortedBy : {key:"chart_id", dir:YAHOO.widget.DataTable.CLASS_DESC},
	    	paginator: oPaginator
	    };
	    
	    var oElDiv = document.createElement('div');
		oElDiv.id = 'charts_dt';
		oPanel.panel.body.appendChild(oElDiv);
	    
	    try {	
			this.dataTable = new YAHOO.widget.DataTable(oElDiv,	oColumnDefs, oChartsDataSource, oDataTableCfg);
			this.dataTable.handleDataReturnPayload = function(oRequest, oResponse, oPayload){
				oPayload.totalRecords = oResponse.meta.totalRecords;
				return oPayload;
			}
			
			oPanel.panel.body.appendChild(oElDiv);
		}
		catch (e){
			logger.log('Error:', e);
		}
		
		oPanel.panel.show();
		oPanel.panel.bringToTop();
	}
}
	
YAHOO.ELSA.Dashboard.prototype.removeChart = function(p_iChartId){
	for (var i in this.rows){
		for (var j in this.rows[i].charts){
			if (this.rows[i].charts[j].chart_id == p_iChartId){
				this.rows[i].charts.splice(j, 1);
				if (this.rows[i].charts.length == 0){
					this.rows.splice(i, 1);
					// Update row_id
					for (var i2 in this.rows){
						if (i2 <= i){
							for (var j2 in this.rows[i2].charts){
								logger.log('decremented row ' + i2 + ' col ' + j2);
								this.rows[i2].charts[j2].row_id--;
							}
						}
					}
				}
				if (this.rows[i]){
					// Update col_id
					for (var j2 in this.rows[i].charts){
						if (j2 >= j){
							logger.log('decremented col ' + j2);
							this.rows[i].charts[j2].col_id--;
						}
					}
				}
			}		
		}
	}
	
	// remove chart from DOM
	var oChart = this.charts[p_iChartId];
	logger.log('oChart', oChart);
	var oElFather = new YAHOO.util.Element(oChart.container.parentNode);
	var oElRemoved = new YAHOO.util.Element(oElFather.removeChild(oChart.container_el));
	while (!oElFather.hasChildNodes() && oElFather.get('element').id != oChart.dashboard.outsideTBody.id){
		var oElChild = oElFather;
		oElFather = new YAHOO.util.Element(oElChild.get('element').parentNode);
		oElRemoved = new YAHOO.util.Element(oElFather.removeChild(oElChild));
		logger.log('oElRemoved', oElRemoved);
	}
	logger.log('removed: ', oElRemoved);
	delete this.charts[p_iChartId];
}

YAHOO.ELSA.Dashboard.prototype.addChart = function(p_oEvent, p_Obj, p_bAddBefore){
	logger.log('arguments', arguments);
	logger.log('this', this);
	var oSelf = this;
	var p_iRowId;
	var oElTr;
	var p_iCellId;
	var p_sPathToQueryDir = '../';
	if (p_Obj instanceof YAHOO.ELSA.Chart){
		// find row based on chart id
		for (var i in this.rows){
			for (var j in this.rows[i].charts){
				if (this.rows[i].charts[j].chart_id == p_Obj.id){
					p_iRowId = i;
					oElTr = p_Obj.container.parentNode;
					if (p_bAddBefore){
						p_iCellId = Number(j) - 1;
					}
					else {
						p_iCellId = Number(j) + 1;
					}
					break;
				}
			}
		}
	}
	else {
		p_iRowId = this.rows.length;
		logger.log('adding chart to new row ' + p_iRowId);
		oElTr = document.createElement('tr');
		this.outsideTBody.appendChild(oElTr);
		p_iCellId = 0;
	}
		
	logger.log('creating chart');
	var handleSubmit = function(p_sType, p_oDialog){
		// format the query input param into queries
		var oData = this.getData();
		var oInputEl = document.createElement('input');
		oInputEl.id = oSelf.id + '_queries';
		oInputEl.setAttribute('type', 'hidden');
		oInputEl.setAttribute('name', 'queries');
		oInputEl.setAttribute('value', YAHOO.lang.JSON.stringify([{label:oData.label, query:oData.query}]));
		this.form.appendChild(oInputEl);
		this.submit();
	};
	var handleCancel = function(){
		this.hide();
	};
	var oCreatePanel = new YAHOO.ELSA.Panel('Create Chart', {
		buttons : [ { text:"Submit", handler:handleSubmit, isDefault:true },
			{ text:"Cancel", handler:handleCancel } ]
	});
	var handleSuccess = function(p_oResponse){
		var response = YAHOO.lang.JSON.parse(p_oResponse.responseText);
		if (response['error']){
			YAHOO.ELSA.Error(response['error']);
		}
		else {
			oCreatePanel.panel.hide();
			// Account for non-time-based summation
			var oMeta = YAHOO.ELSA.queryMetaParamsDefaults;
			if (!response.query.match(/groupby[\:\=]/) && !response.query.match(/\| sum\(/)){
				response.query += ' groupby:' + oMeta.groupby[0];
			}
			delete oMeta.groupby;
//			if (response.query.match(/groupby[\:\=]/) || response.query.match(/\| sum\(/)){
//				delete oMeta.groupby;
//			}
			
			var oNewChart = {
				chart_id: response.chart_id,
				queries: [
					{
						query_string: response.query,
						query_id: response.query_id,
						label: response.label,
						query_meta_params: oMeta
					}
				],
				type: response.chart_type,
				y: p_iRowId,
				x: p_iCellId
			};
			
			// new row?
			if (p_iRowId >= oSelf.rows.length){
				oSelf.rows.push({
					title: '',
					charts: [oNewChart]
				});
			}
			else {
				// adding to an existing row
				oSelf.rows[p_iRowId].charts.push(oNewChart);
			}
			
			var oElTd = document.createElement('td');
			oElTr.appendChild(oElTd);
			oSelf.charts[ oNewChart.chart_id ] = new YAHOO.ELSA.Chart(oNewChart, oElTd, oSelf);
			logger.log('successful submission');
		}
	};
	oCreatePanel.panel.callback = {
		success: handleSuccess,
		failure: YAHOO.ELSA.Error
	};
	
	oCreatePanel.panel.renderEvent.subscribe(function(){
		oCreatePanel.panel.setBody('');
		oCreatePanel.panel.setHeader('Create New Chart');
		oCreatePanel.panel.bringToTop();
		//var sFormId = 'create_dashboard_form';
		var sFormId = oCreatePanel.panel.form.id;
		
		var sButtonId = 'chart_type_select_button';
		var sId = 'chart_type_input_connector';
		var onMenuItemClick = function(p_sType, p_aArgs, p_oItem){
			var sText = p_oItem.cfg.getProperty("text");
			// Set the label of the button to be our selection
			var oAuthButton = YAHOO.widget.Button.getButton(sButtonId);
			oAuthButton.set('label', sText);
			var oFormEl = YAHOO.util.Dom.get(sFormId);
			var oInputEl = YAHOO.util.Dom.get(sId);
			oInputEl.setAttribute('value', p_oItem.value);
		}
		
		var aMenu = [
			{ text:'Bar', value:'ColumnChart', onclick: { fn: onMenuItemClick } },
			{ text:'Pie', value:'PieChart', onclick: { fn: onMenuItemClick } },
			{ text:'Table', value:'Table', onclick: { fn: onMenuItemClick } },
			{ text:'Map', value:'GeoChart', onclick: { fn: onMenuItemClick } }
		];
		
		var oMenuButtonCfg = {
			id: sButtonId,
			type: 'menu',
			label: 'Chart Type',
			name: sButtonId,
			menu: aMenu
		};
		var oFormGridCfg = {
			form_attrs:{
				action: p_sPathToQueryDir + 'Charts/add',
				method: 'POST',
				id: sFormId
			},
			grid: [
				[ {type:'text', args:'Title'}, {type:'input', args:{id:'chart_title', name:'title', size:32}} ],
				[ {type:'text', args:'Type'}, {type:'widget', className:'Button', args:oMenuButtonCfg} ],
				[ {type:'text', args:'Query:'} ],
				[ {type:'text', args:'Label'}, {type:'input', args:{id:'label', name:'label', size:32}} ],
				[ {type:'text', args:'Query'}, {type:'input', args:{id:'query', name:'query', size:64}} ],
			]
		};
		
		// Now build a new form using the element auto-generated by widget.Dialog
		var oForm = new YAHOO.ELSA.Form(oCreatePanel.panel.form, oFormGridCfg);
		
		var oInputEl = document.createElement('input');
		oInputEl.id = sId;
		oInputEl.setAttribute('type', 'hidden');
		oInputEl.setAttribute('name', 'chart_type');
		oInputEl.setAttribute('value', 0);
		oForm.form.appendChild(oInputEl);
		
		var oDashboardInputEl = document.createElement('input');
		oDashboardInputEl.id = 'dashboard_id';
		oDashboardInputEl.setAttribute('type', 'hidden');
		oDashboardInputEl.setAttribute('name', 'dashboard_id');
		oDashboardInputEl.setAttribute('value', oSelf.id);
		oForm.form.appendChild(oDashboardInputEl);
		
		if (typeof(p_iRowId) != 'undefined' && typeof(p_iCellId) != 'undefined'){
			var oMap = { x:p_iCellId, y:p_iRowId };
			var oElements = {};
			for (var i in oMap){
				oElements[i] = document.createElement('input');
				oElements[i].id = 'coordinate_' + oSelf.id + '_' + p_iRowId + '_' + p_iCellId; 
				oElements[i].setAttribute('type', 'hidden');
				oElements[i].setAttribute('name', i);
				oElements[i].setAttribute('value', oMap[i]);
				oForm.form.appendChild(oElements[i]);
			}
		}
	});
	oCreatePanel.panel.render();
	oCreatePanel.panel.show();
}

YAHOO.ELSA.Dashboard.prototype.redraw = function(){
	this.outsideTBody.innerHTML = '';
	this.charts = {};
	for (var i in this.rows){
		this.loadChartRow(i);
	}
}

YAHOO.ELSA.Dashboard.prototype.moveChart = function(p_oEvent, p_a){
	var p_oChart = p_a[0], p_sDirection = p_a[1];
	var oSelf = this;
	logger.log('moving ' + p_sDirection, p_oChart);
	var oArgs = {
		dashboard_id: oSelf.id,
		chart_id: p_oChart.id,
		direction: p_sDirection,
		start_time: YAHOO.ELSA.queryMetaParamsDefaults.start,
		end_time: YAHOO.ELSA.queryMetaParamsDefaults.end
	}
	
	if (typeof(YAHOO.ELSA.queryMetaParamsDefaults.groupby) != 'undefined'){
		oArgs.groupby = YAHOO.ELSA.queryMetaParamsDefaults.groupby[0];
	}
	
	YAHOO.ELSA.async('../Charts/move', function(p_oReturn){
		if (!p_oReturn){
			return;
		}
		
		oSelf.rows = p_oReturn.rows;
		oSelf.redraw();
		
//		if (p_sDirection == 'left'){
//			var oSwappedChart = oSelf.charts[ oSelf.rows[ p_oChart.y ].charts[p_oChart.x - 1].chart_id ];
//			var elem = oSwappedChart.container;
//			elem.parentNode.insertBefore(elem,elem.parentNode.previousSibling);
//			oSwappedChart.x--;
//			//oSelf.loadChartRow(p_oChart.y);
//		}
//		else if (p_sDirection == 'right'){
//			
//		}
//		else if (p_sDirection == 'up'){
//			oSelf.loadChartRow(p_oChart.y - 1);
//			oSelf.loadChartRow(p_oChart.y);
//		}
//		else if (p_sDirection == 'down'){
//			oSelf.loadChartRow(p_oChart.y);
//			oSelf.loadChartRow(p_oChart.y + 1);
//		}
		
	}, oArgs);
}

YAHOO.ELSA.Chart = function(p_oArgs, p_oContainer, p_oDashboard){
	
	logger.log('chart p_oArgs', p_oArgs);
	this.original_args = p_oArgs;
	this.id = p_oArgs.chart_id;
	this.dashboard = p_oDashboard;
	this.queries = p_oArgs.queries;
	this.queries_received = 0;
	this.queries_sent = 0;
	this.type = p_oArgs.type;
	this.container = p_oContainer;
	this.options = p_oArgs.chart_options;
	this.x = p_oArgs.x;
	this.y = p_oArgs.y;
	
	var aNeededIds = [ 'chart', 'dashboard', 'control' ];
	if (YAHOO.ELSA.editCharts){
		aNeededIds.unshift('edit'); 
	}
	for (var k in aNeededIds){
		var sType = aNeededIds[k];
		var sId = sType + '_' + this.id;
		var oEl = document.createElement('div');
		oEl.id = sId;
		this.container.appendChild(oEl);
		this[sType + '_el'] = oEl;
	}
		
	this.container_el = new YAHOO.util.Element(this.container);
		
	if (YAHOO.ELSA.editCharts){
		var oElEditEl = new YAHOO.util.Element(this.edit_el);
		oElEditEl.addClass('hiddenElement');
		
		var oElEditChart = document.createElement('a');
		oElEditChart.href = '#';
		oElEditChart.innerHTML = 'Edit Chart Appearance';
		this.edit_el.appendChild(oElEditChart);
		var oElEditChartEl = new YAHOO.util.Element(oElEditChart);
		oElEditChartEl.on('click', this.openEditor, this, this);
		
		this.edit_el.appendChild(document.createElement('br'));
		
		var oElEditQueries = document.createElement('a');
		oElEditQueries.href = '#';
		oElEditQueries.innerHTML = 'Edit Chart Queries';
		this.edit_el.appendChild(oElEditQueries);
		var oElEditQueriesEl = new YAHOO.util.Element(oElEditQueries);
		var oSelf = this;
		oElEditQueriesEl.on('click', function(){
			YAHOO.ELSA.async('../Charts/get?chart_id=' + oSelf.id, function(p_oData){
				if (!p_oData){
					return;
				}
				oSelf.editQueries(p_oData, '../');
			});
		});
		
		this.edit_el.appendChild(document.createElement('br'));
		
		var oElDelChart = document.createElement('a');
		oElDelChart.href = '#';
		oElDelChart.innerHTML = 'Delete Chart';
		this.edit_el.appendChild(oElDelChart);
		var oElDelChartEl = new YAHOO.util.Element(oElDelChart);
		oElDelChartEl.on('click', function(){
			var oSelf = this;
			var oConfirmationPanel = new YAHOO.ELSA.Panel.Confirmation(function(){
				oPanel = this;
				YAHOO.ELSA.async('../Charts/del?chart_id=' + oSelf.id + '&dashboard_id=' + oSelf.dashboard.id, function(p_oData){
					if (!p_oData){
						return;
					}
					logger.log('deleted chart ' + oSelf.id);
					// remove chart from data structure
					oSelf.dashboard.removeChart(oSelf.id);
					oPanel.hide();
				});
			}, null, 'Really delete chart?');
			
		}, this, this);
		
		this.edit_el.appendChild(document.createElement('br'));
		
		// Add the "add" button
		var oElAddChart = document.createElement('a');
		oElAddChart.href = '#';
		oElAddChart.innerHTML = 'Add Another Chart to This Row';
		this.edit_el.appendChild(oElAddChart);
		var oElAddChartEl = new YAHOO.util.Element(oElAddChart);
		oElAddChartEl.on('click', this.dashboard.addChart, this, this.dashboard);
		
		this.edit_el.appendChild(document.createElement('br'));
		
		// Add the "move" button
		var oElMove = document.createElement('div');
		oElMove.appendChild(document.createTextNode('Move '));
		var oDirections = {
			up:1,
			down:1,
			left:1,
			right:1
		}
		if ((this.y >= (this.dashboard.rows.length - 1)) && (this.dashboard.rows[this.y].length < 2)){
			delete oDirections.down;
		}
		if (this.y == 0){
			delete oDirections.up;
		}
		if (this.x == 0){
			delete oDirections.left;
		}
		if (this.x >= (this.dashboard.rows[this.y].charts.length - 1)){
			delete oDirections.right;
		}
		for (var i in oDirections){
			var oEl = document.createElement('a');
			oEl.href = '#';
			oEl.innerHTML = i;
			oElMove.appendChild(oEl);
			oElMove.appendChild(document.createTextNode(' '));
			var oElEl = new YAHOO.util.Element(oEl);
			oElEl.on('click', this.dashboard.moveChart, [this, i], this.dashboard);
		}
		this.edit_el.appendChild(oElMove);
	}
	
	for (var i in this.queries){
		this.sendQuery(i);
	}
}
	
YAHOO.ELSA.Chart.prototype.sendQuery = function(p_iQueryNum, p_bRedraw){
	var oQuery = this.queries[p_iQueryNum];
	logger.log('oQuery', oQuery);
	var sReqId = oQuery.query_id;
	this.reqStr = '../datasource/?tqx=reqId:' + sReqId
		+ ';out:json&q=' + encodeURIComponent(JSON.stringify(oQuery));
	logger.log('sReqId: ' + sReqId + ', reqStr: ' + this.reqStr);
	var oDSQuery = new google.visualization.Query(this.reqStr);
	// Disable errors showing up on the chart
	google.visualization.errors.addError = function(){ logger.log('error: ', arguments); };
	var oSelf = this;
	
	var fnStoreResult = function(p_oResponse){
		logger.log('result back for ' + sReqId + ' with label ' + oQuery.label);
		if (p_oResponse.isError()){
			logger.log('FAIL: ' + p_oResponse.getMessage() + ' ' + p_oResponse.getDetailedMessage());
			YAHOO.ELSA.Error(p_oResponse.getMessage() + ' ' + p_oResponse.getDetailedMessage());
			return;
		}
				
		
		// Check to see if any of the queries are time-based
		var sTime = false;
		for (var i in oSelf.queries){
			var oIndividualQuery = oSelf.queries[i];
			logger.log('oIndividualQuery', oIndividualQuery);
			if ((typeof(oIndividualQuery.query_meta_params) != 'undefined' 
				&& typeof(oIndividualQuery.query_meta_params.groupby) != 'undefined')){
				logger.log('oIndividualQuery.groupby', oIndividualQuery.query_meta_params.groupby);
				if (YAHOO.ELSA.timeTypes[ oIndividualQuery.query_meta_params.groupby[0] ]){
					sTime = oIndividualQuery.query_meta_params.groupby[0];
					logger.log('set sTime to true because found group ' + oIndividualQuery.query_meta_params.groupby[0]);
					break;
				}
			}
			else {
				var aMatches = oIndividualQuery.query_string.match(/groupby[\:\=](\w+)/i);
				if (aMatches){
					logger.log('oIndividualQuery.groupby', aMatches[1]);
					if (YAHOO.ELSA.timeTypes[ aMatches[1] ]){
						sTime = aMatches[1];
						logger.log('set sTime to true because found group ' + aMatches[1]);
						break;
					}
				}
			}
		}
		oSelf.isTimeChart = sTime;
		var oRegExp = new RegExp('\\Wnoscroll=(\\w+)');
		var oMatches = oRegExp.exec(location.search);
		if (oMatches && oMatches[1] == 'true'){
			oSelf.isTimeChart = false;
		}
		
		
		if (oSelf.dataTable){
			oSelf.mergeDataTables(p_oResponse.getDataTable(), oQuery.label);
			oSelf.dataTable.setColumnLabel((oSelf.dataTable.getNumberOfColumns() - 1), oQuery.label);
		}
		else {
			oSelf.dataTable = p_oResponse.getDataTable();
			logger.log('starting with first data col label: ' + oSelf.dataTable.getColumnLabel(1));
		}
		
		for (var i = 0; i < oSelf.dataTable.getNumberOfColumns(); i++){
			logger.log('now label: ', oSelf.dataTable.getColumnLabel(i));
		}
		
		if (oSelf.group){
			if (typeof(google.visualization.data[oSelf.group]) != 'undefined'){
				oSelf.dataTable = google.visualization.data.group(oSelf.dataTable, [0], 
				[{'column': 1, 'aggregation': google.visualization.data[oSelf.group], 'type': 'number'}]);
			}
			else {
				logger.log('invalid group: ' + oSelf.group);
			}
		}
		
		if (oSelf.dataTable.getDistinctValues(0).length == 0){
			logger.log('No data found in dataTable column range, not drawing chart.');
			oSelf.dashboard_el.innerText = 'No Data Available';
		}
		
		oSelf.queries_received++;
				
		if (oSelf.queries_received == oSelf.queries.length || p_bRedraw){
			logger.log('received all (' + oSelf.queries.length + ') with query id ' + p_iQueryNum + ' chart data for chart ' + oSelf.id);
			
			// Remove loading gif
			oSelf.container_el.removeClass('loading');
			if (oSelf.edit_el){
				var oElEditEl = new YAHOO.util.Element(oSelf.edit_el);
				oElEditEl.removeClass('hiddenElement');
			}
			logger.log('finished loading');
			try {
				oSelf.draw();
				oSelf.queries_received = 0;
				oSelf.queries_sent = 0;
			} catch (e){ logger.log('error drawing chart', e); }
		}
		else {
			logger.log('received ' + oSelf.queries_received + ' of ' + oSelf.queries.length);
		}
		logger.log('received, now: ' + oSelf.queries_sent + '/' + oSelf.queries_received);
	}
		
	this.container_el.addClass('loading');
	oDSQuery.send(fnStoreResult);
	this.queries_sent++;
	
	logger.log('sent ' + (Number(p_iQueryNum) + 1) + ' of ' + oSelf.queries.length);
}

YAHOO.ELSA.Chart.prototype.mergeDataTables = function(p_oAddTable, p_sLabel){
	logger.log('merging ' + p_sLabel);
	
	this.dataTable.addColumn('number', p_sLabel, p_oAddTable.getColumnId(1));
	
	try {
		var iNumAdded = 0;
		var iNumCols = this.dataTable.getNumberOfColumns();
		// For each time value in our add table, add to the appropriate bucket in the existing table
		for (var i = 0; i < p_oAddTable.getNumberOfRows(); i++){
			var x = p_oAddTable.getValue(i, 0);
			var y = p_oAddTable.getValue(i, 1);
			
			var aRowsForUpdate;
			aRowsForUpdate = this.dataTable.getFilteredRows([{ column:0, value:x}]);
			
			if (aRowsForUpdate.length){
				this.dataTable.setCell(aRowsForUpdate[0], (iNumCols - 1), y);
				//logger.log('set cell ' + aRowsForUpdate[0] + ' ' + (iNumCols - 1) + ' ' + y);
			}
			else {
				//logger.log('no date for ', oDate);
				var aNewRow = [x];
				for (var j = 1; j < (iNumCols - 1); j++){
					aNewRow.push(null);
				}
				aNewRow.push(y);
				this.dataTable.addRow(aNewRow);
				iNumAdded++;
			}
		}
		if (iNumAdded){
			this.dataTable.sort({column:0});
		}
	} catch(e){ logger.log('Error merging tables', e); }
}
	
YAHOO.ELSA.Chart.prototype.draw = function(){
	if (this.isTimeChart){
		this.makeTimeChart();
	}
	else if (this.type == 'GeoChart'){
		this.makeGeoChart();
	}
	else {
		this.dataTable.sort({column:1, desc:true});
		this.makeSimpleChart();
	}
}
	
YAHOO.ELSA.Chart.prototype.redraw = function(){
	logger.log('this', this);
	logger.log('loading');
	
	this.chart_el.innerHTML = '';
	this.control_el.innerHTML = '';
	this.container_el.addClass('loading');
	delete this.dataTable;
	this.queries_received = 0;
	this.queries_sent = 0;
	for (var i in this.queries){
		this.sendQuery(i);
	}
	logger.log('redrawn');
}
	
YAHOO.ELSA.Chart.prototype.selectHandler = function(){
	var oSelection = this.wrapper.getChart().getSelection();
	var oDataTable = this.wrapper.getDataTable();
	logger.log('select', oSelection);
			
	var message = '';
	  for (var i = 0; i < oSelection.length; i++) {
	    var item = oSelection[i];
	    if (item.row != null && item.column != null) {
	      var str = oDataTable.getFormattedValue(item.row, item.column);
	      message += '{row:' + item.row + ',column:' + item.column + '} = ' + str;
	      logger.log('label', oDataTable.getColumnLabel(item.column));
	      logger.log('properties', oDataTable.getColumnProperties(item.column));
	      logger.log('value', oDataTable.getColumnProperty(item.column, 'value'));
	      logger.log('getProperties', oDataTable.getProperties(item.row, item.column));
	      if (this.isTimeChart){
	      	var oStart = oDataTable.getValue(item.row, 0);
	      	var iOffset = YAHOO.ELSA.timeTypes[this.isTimeChart];
	      	var oEnd = new Date(oStart.getTime() + (iOffset * 1000));
	      	logger.log('oStart ' + oStart + ', oEnd ' + oEnd);
	      	var sNewLocation = location.pathname + '?start=' + getISODateTime(oStart) + '&end=' + getISODateTime(oEnd) 
	      		+ '&' + YAHOO.ELSA.timeTypeDrillDowns[this.isTimeChart];
	      	logger.log('sNewLocation', sNewLocation);
	      	window.location = sNewLocation;
	      }
	    } else if (item.row != null) {
	      var str = oDataTable.getFormattedValue(item.row, 0);
	      message += '{row:' + item.row + ', (no column, showing first)} = ' + str;
	    } else if (item.column != null) {
	      var str = oDataTable.getFormattedValue(0, item.column);
	      message += '{(no row, showing first), column:' + item.column + '} = ' + str;
	    }
	  }
	  if (message == '') {
	    message = 'nothing';
	  }
	  logger.log(message);
}

YAHOO.ELSA.Chart.prototype.getOptions = function(){
	var oOptions = YAHOO.ELSA.chartOptions[this.type];
	if (this.options){
		oOptions = this.options;
	}
	oOptions.width = (YAHOO.ELSA.dashboardParams.width / this.dashboard.rows[this.y].charts.length);
	return oOptions;
}

YAHOO.ELSA.Chart.prototype.makeSimpleChart = function(){
	logger.log('makeSimpleChart');
	var colorPalette = YAHOO.ODE.Chart.getPalette_a();
	var paletteLength = colorPalette.length;
	var data = [];
	var i = 0;
	var dt = this.dataTable;
	var n = dt.getNumberOfRows();
	for(var i = 0; i < n; ++i) {
		var label = dt.getValue(i, 0);
		var value = dt.getValue(i, 1);
		var thisColor = colorPalette[((paletteLength - ((i+6) % paletteLength)) - 1)];
		data.push( {
			label: label,
			value: value,
			color: thisColor[0],
			highlight: thisColor[3]
		} );
	}
	var chartDiv = document.createElement('div');
	var canvasEl = document.createElement('canvas');
	chartDiv.appendChild(canvasEl);
	var ctx = canvasEl.getContext("2d");
	var hElem = document.createElement('h3');
	hElem.innerHTML = this.queries[0].query_string;
	hElem.style['text-align'] = 'center';
	hElem.style['margin-bottom'] = 0;
	this.chart_el.appendChild(hElem);
	this.chart_el.appendChild(chartDiv);
	var chartClass = 'dbchart';
	if ('PieChart' == this.type) {
		chartClass = chartClass + ' pie-chart';
		var legendDiv = document.createElement('div');
		legendDiv.setAttribute('class', 'legend');
		chartDiv.appendChild(legendDiv);
		legendDiv.style.overflow = 'auto';
		legendDiv.style.height = '150px';
		canvasEl.height = 150;
		canvasEl.width = 160;
		canvasEl.style.width = '160px';
		var myPieChart = new Chart(ctx).Pie(data, {});
		legendDiv.innerHTML = myPieChart.generateLegend();
		var legendWidth = legendDiv.offsetWidth;
		legendDiv.style.width = (15 + legendWidth) + 'px';
		chartDiv.style.width = (legendWidth + 220) + 'px';
		legendDiv.style['margin-left'] = '15px';
	} else if ('ColumnChart' == this.type) {
		chartClass = chartClass + ' bar-chart';
		var label = dt.getColumnLabel(1);
		var labels = [];
		var values = [];
		var barCount = data.length;
		var ymax = 0;
		var thisColor = colorPalette[paletteLength - 3];
		for(var i = 0; i < data.length; ++i) {
			var val = data[i]["value"];
			if (val > ymax) { ymax = val; }
			values.push(val);
			labels.push(data[i]["label"]);
		}
		data = {
			labels: labels,
			datasets: [ {
				data: values,
				label: label,
				fillColor: thisColor[0],
				strokeColor: thisColor[1],
				highlightFill: thisColor[2],
				highlightStroke: thisColor[3]
			} ]
		};
		var opts = YAHOO.ODE.Chart.getSteps(ymax);
		var legendDiv = document.createElement('div');
		chartDiv.appendChild(legendDiv);
		var legendWidth = legendDiv.offsetWidth;
		canvasEl.height = 150;
		var cWidth = 400;
		if (20 + barCount * 6.8 > cWidth) {
			cWidth = 20 + barCount * 6.8;
		}
		canvasEl.width = cWidth;
		opts['barStrokeWidth'] = 1;
		opts['barValueSpacing'] = 2;
		var myBarChart = new Chart(ctx).Bar(data, opts);
		legendDiv.innerHTML = myBarChart.generateLegend();
		chartDiv.style.width = (45 + cWidth + legendWidth) + 'px';
		legendDiv.style.width = (15 + legendWidth) + 'px';
	} else {

		this.wrapper = new google.visualization.ChartWrapper({
			dataTable: this.dataTable,
			containerId: this.chart_el,
			chartType: this.type,
			options: this.getOptions()
		});

		var oSelf = this;
		google.visualization.events.addListener(this.wrapper, 'ready', function(){
			google.visualization.events.addListener(oSelf.wrapper.getChart(), 'select', function(){ oSelf.selectHandler() });
		});

		this.wrapper.draw();
		logger.log(this.wrapper);
	}
	chartDiv.setAttribute('class', chartClass);
}

YAHOO.ELSA.Chart.prototype.makeGeoChart = function(){
	this.wrapper = new google.visualization.ChartWrapper({
		dataTable: this.dataTable,
		containerId: this.chart_el,
		chartType: this.type,
		options: this.getOptions()
	});
	
	var oSelf = this;
	google.visualization.events.addListener(this.wrapper, 'ready', function(){
		google.visualization.events.addListener(oSelf.wrapper.getChart(), 'regionClick', function(){ oSelf.selectHandler() });
	});
	this.wrapper.draw();
}

YAHOO.ELSA.Chart.prototype.makeChart = function(){
	this.google_dashboard = new google.visualization.Dashboard(YAHOO.util.Dom.get(this.dashboard_el));
		
	var oRange = this.dataTable.getColumnRange(0);
	logger.log('oRange', oRange);
	logger.log('oRange', oRange.min);
	logger.log('oRange', oRange.max);
	logger.log('range diff: ' + (oRange.max - oRange.min));
	var iStep = (oRange.max - oRange.min) / 10;
	logger.log('iStep ' + iStep);
	var oFirstStep = oRange.min + iStep;
	
	var aChartCols = [];
	for (var i = 0; i < this.dataTable.getNumberOfColumns(); i++){
		aChartCols.push(i);
	}
	
	this.control = new google.visualization.ControlWrapper({
		'controlType': 'ChartRangeFilter',
		'containerId': this.control_el,
		'options': {
			// Filter by the date axis.
			'filterColumnIndex': 0,
			'ui': {
				'chartType': 'LineChart',
				'chartOptions': {
					'chartArea': {'width': '90%'},
					'hAxis': {'baselineColor': 'none'}
				},
				'chartView': {
					'columns': aChartCols
				},
				'minRangeSize': iStep
			}
     	},
		'state': {'range': {'start': oRange.min, 'end': oFirstStep}}
	});
	
	this.wrapper = new google.visualization.ChartWrapper({
		dataTable: this.dataTable,
		containerId: this.chart_el,
		chartType: this.type,
		options: this.getOptions()
	});
	
	var oSelf = this;
	google.visualization.events.addListener(this.wrapper, 'ready', function(){
		google.visualization.events.addListener(oSelf.wrapper.getChart(), 'regionClick', function(){ oSelf.selectHandler() });
	});
	this.google_dashboard.bind(this.control, this.wrapper);
	this.google_dashboard.draw(this.dataTable);
}

// This is a hack to deal with "maximum stack size exceeded" errors in Google Visualizations for big datatables
YAHOO.ELSA.Chart.prototype.cloneChart = function(p_oWrapper){
	var oNew = new google.visualization.ChartWrapper({
		dataTable: p_oWrapper.getDataTable(),
		chartType: p_oWrapper.getChartType(),
		containerId: null,
		options: p_oWrapper.getOptions()
	});
	
	return oNew;
}

YAHOO.ELSA.Chart.prototype.makeTimeChart = function(){
	this.google_dashboard = new google.visualization.Dashboard(YAHOO.util.Dom.get(this.dashboard_el));
	var oRange = this.dataTable.getColumnRange(0);
	logger.log('oRange', oRange);
	logger.log('oRange', oRange.min);
	logger.log('oRange', oRange.max);
	logger.log('range diff: ' + (oRange.max.getTime() - oRange.min.getTime()));
	var iStep = (oRange.max.getTime() - oRange.min.getTime()) / 10;
	logger.log('iStep ' + iStep);
	var oFirstStep = new Date(oRange.min.getTime() + iStep);
		
	var aChartCols = [];
	for (var i = 0; i < this.dataTable.getNumberOfColumns(); i++){
		aChartCols.push(i);
	}
	this.control = new google.visualization.ControlWrapper({
		'controlType': 'ChartRangeFilter',
		'containerId': this.control_el,
		'options': {
			'width': this.getOptions().width,
			// Filter by the date axis.
			'filterColumnIndex': 0,
			'ui': {
				'chartType': 'LineChart',
				'chartOptions': {
					'chartArea': {'width': '90%'},
					'hAxis': {'baselineColor': 'none'}
				},
				'chartView': {
					'columns': aChartCols
				},
				'minRangeSize': iStep
			}
     	},
		'state': {'range': {'start': oRange.min, 'end': oFirstStep}}
	});
				
	this.wrapper = new google.visualization.ChartWrapper({
		dataTable: this.dataTable,
		containerId: this.chart_el,
		chartType: this.type,
		options: this.getOptions()
	});
	this.wrapper.draw(); // draw this ahead of time so control gets the right width
	
	var oSelf = this;
	google.visualization.events.addListener(this.wrapper, 'ready', function(){
		google.visualization.events.addListener(oSelf.wrapper.getChart(), 'select', function(){ oSelf.selectHandler() });
	});
	
	this.google_dashboard.bind(this.control, this.wrapper);
	this.google_dashboard.draw(this.dataTable);
}


//YAHOO.ELSA.Chart.googleDataToYuiData = function(p_oGoogleData){
//	p_oGoogleData = YAHOO.lang.JSON.parse(p_oGoogleData);
//	
//	var aFields = [];
//	for (var i in p_oGoogleData.cols){
//		if (!p_oGoogleData.cols[i].label){
//			aFields.push('count');
//		}
//		else {
//			aFields.push(p_oGoogleData.cols[i].label);
//		}
//	}
//	
//	var oData = {
//		results: []
//	}
//	for (var i in p_oGoogleData.rows){
//		var aRow = {};
//		for (var j in p_oGoogleData.rows[i].c){
//			//logger.log('col: ' + aFields[j] + ' val:' + p_oGoogleData.rows[i].c[j].v);
//			aRow[ aFields[j] ] = p_oGoogleData.rows[i].c[j].v;
//		}
//		oData.results.push(aRow);
//	}
//	oData.totalRecords = oData.recordsReturned = oData.results.length;
//	
//	var oChartsDataSource = new YAHOO.util.DataSource(oData);
//	oChartsDataSource.responseType = YAHOO.util.DataSource.TYPE_JSON;
//	oChartsDataSource.responseSchema = {
//		resultsList: 'results',
//		fields: aFields,
//		metaFields: {
//			totalRecords: 'totalRecords',
//			recordsReturned: 'recordsReturned'
//		}
//	};
//	
//	return oChartsDataSource;
//	//return oData;
//}

YAHOO.ELSA.Chart.prototype.openEditor = function(p_oEvent){
	// Handler for the "Open Editor" button.
	if (!this.editor){
		this.editor = new google.visualization.ChartEditor();
		var oSelf = this;
		google.visualization.events.addListener(this.editor, 'ok', function() {
			var oWrapper = oSelf.editor.getChartWrapper();
			oWrapper.setContainerId(oSelf.wrapper.getContainerId());
			oSelf.wrapper = oWrapper;
			oSelf.wrapper.draw();
			if (oSelf.google_dashboard){
				oSelf.google_dashboard.bind(oSelf.control, oSelf.wrapper);
				oSelf.google_dashboard.draw(oSelf.dataTable);
			}
			
			var oToUpdate = {
				options: oSelf.wrapper.getOptions(),
				type: oSelf.wrapper.getChartType()
			}
			var oPostData = {
				chart_id: oSelf.id,
				to_update: oToUpdate
			}
			YAHOO.ELSA.async('../Charts/update', function(){
				logger.log('updated ok');
			}, oPostData);
		});
	}
	var oCloneChart = this.cloneChart(this.wrapper);
	this.editor.openDialog(oCloneChart);
	
}

YAHOO.ELSA.Chart.prototype.editQueries = function(p_oData, p_sPathToQueryDir){
	var oSelf = this;
	var oChartsDataSource = new YAHOO.util.DataSource(p_oData);
	oChartsDataSource.responseType = YAHOO.util.DataSource.TYPE_JSON;
	oChartsDataSource.responseSchema = {
		resultsList: 'queries',
		fields: ['chart_id', 'query_id', 'label', 'query' ],
		metaFields: {
			totalRecords: 'totalRecords',
			recordsReturned: 'recordsReturned'
		}
	};
	
	var oPanel = new YAHOO.ELSA.Panel('Queries', { zIndex: 2000 });
	oPanel.panel.setHeader('Queries');
	oPanel.panel.render();
	
	var oElCreate = document.createElement('a');
    oElCreate.href = '#';
    oElCreate.innerHTML = 'Add query to chart';
    oPanel.panel.body.appendChild(oElCreate);
    var oElCreateEl = new YAHOO.util.Element(oElCreate);
    oElCreateEl.on('click', oSelf.addQuery, [], oSelf);
	
	var formatMenu = function(elLiner, oRecord, oColumn, oData){
		// Create menu for our menu button
		var oButtonMenuCfg = [
			{ 
				text: 'Delete', 
				value: 'delete', 
				onclick:{
					fn: oSelf.deleteQuery,
					obj: [oRecord,this],
					scope: oSelf
				}
			}//,
//			{ 
//				text: 'Add', 
//				value: 'add', 
//				onclick:{
//					fn: oSelf.addQuery,
//					obj: [oRecord,this],
//					scope:oSelf
//				}
//			}
		];
		
		var oButton = new YAHOO.widget.Button(
			{
				type:'menu', 
				label:'Actions',
				menu: oButtonMenuCfg,
				name: 'queries_menu_button',
				container: elLiner
			});
	};
	
	var asyncSubmitter = function(p_fnCallback, p_oNewValue){
		// called in the scope of the editor
		logger.log('editor this: ', this);
		logger.log('p_oNewValue:', p_oNewValue);
		
		var oRecord = this.getRecord(),
			oColumn = this.getColumn(),
			sOldValue = this.value,
			oDatatable = this.getDataTable();
		logger.log('sOldValue:', sOldValue);
		logger.log('oColumn.getKey()', oColumn.getKey());
		logger.log('oRecord', oRecord.getData());
		
		var oNewValue = p_oNewValue;
		var oSendValue = oNewValue;
		
		YAHOO.ELSA.async(p_sPathToQueryDir + 'Charts/update_query', function(p_oReturn){
			if (p_oReturn.ok && p_oReturn.ok > 0){
				// update the edit queries datatable
				oDatatable.updateCell(oRecord, oColumn, oNewValue);
				// update the actual query
				for (var i in oSelf.queries){
					if (oSelf.queries[i].query_id == oRecord.getData().query_id){
						logger.log('set ' + oColumn.getKey() + ' to ', oNewValue);
						if (oColumn.getKey() == 'query'){
							oSelf.queries[i].query_string = oNewValue;
							//p_oChart.queries[i].query_meta_params = oNewValue.query_meta_params;
						}
						else {
							oSelf.queries[i][ oColumn.getKey() ] = oNewValue;
						}
						break;
					}
				}
				oSelf.redraw();
				p_fnCallback(true,oNewValue);
			}
			else {
				YAHOO.ELSA.Error(p_oReturn.warnings);
			}
		}, {
			chart_id: oRecord.getData().chart_id,
			query_id: oRecord.getData().query_id,
			col: oColumn.getKey(),
			val: oSendValue
		});
	};
	
	var cellEditorValidatorQuery = function(p_sInputValue, p_sCurrentValue, p_oEditorInstance){
		return p_sInputValue;
	};
	
	var onEventShowCellEditor = function(p_oArgs){
		logger.log('p_oArgs', p_oArgs);
		var oEl = new YAHOO.util.Element(p_oArgs.target);
		if (!oEl.hasClass('yui-dt-editable')){
			return;
		}
		this.onEventShowCellEditor(p_oArgs);
		if (oSelf.editChartDataTable.getCellEditor().value != null && typeof(oSelf.editChartDataTable.getCellEditor().value) == 'object'){
			oSelf.editChartDataTable.getCellEditor().textarea.value = oSelf.editChartDataTable.getCellEditor().value.query_string;
		}
		logger.log('getCellEditor():',oSelf.editChartDataTable.getCellEditor());
		// increase the size of the textbox, if we have one
		if (oSelf.editChartDataTable.getCellEditor() && oSelf.editChartDataTable.getCellEditor().textbox){				
			oSelf.editChartDataTable.getCellEditor().textbox.setAttribute('size', 20);
			oSelf.editChartDataTable.getCellEditor().textbox.removeAttribute('style');
			// create key listener for the submit
			var enterKeyListener = new YAHOO.util.KeyListener(
					oSelf.editChartDataTable.getCellEditor().textbox,
					{ keys: 13 },
					{ 	fn: function(eName, p_aArgs){
							var oEvent = p_aArgs[1];
							// Make sure we don't submit the form
							YAHOO.util.Event.stopEvent(oEvent);
							var tgt=(oEvent.target ? oEvent.target : 
								(oEvent.srcElement ? oEvent.srcElement : null)); 
							try{
								tgt.blur();
							}
							catch(e){}
							var op = '=';
							this.getCellEditor().save();
						},
						scope: YAHOO.ELSA,
						correctScope: false
					}
			);
			enterKeyListener.enable();
		}
	}
		
	var oColumnDefs = [
		{ key:'menu', label:'Action', formatter:formatMenu },
		{ key:"query_id", label:"ID", formatter:YAHOO.widget.DataTable.formatNumber, sortable:true },
		{ key:"label", label:"Label", sortable:true,
			editor: new YAHOO.widget.TextareaCellEditor({width:'500px', height:'8em', asyncSubmitter:asyncSubmitter}), },
		{ key:"query", label:"Query", sortable:true, validator:cellEditorValidatorQuery,
			editor: new YAHOO.widget.TextareaCellEditor({width:'500px', height:'8em', asyncSubmitter:asyncSubmitter}) //,formatter:formatQuery
		}
	];
	
	
	var oPaginator = new YAHOO.widget.Paginator({
	    pageLinks          : 10,
        rowsPerPage        : 5,
        rowsPerPageOptions : [5,20],
        template           : "{CurrentPageReport} {PreviousPageLink} {PageLinks} {NextPageLink} {RowsPerPageDropdown}",
        pageReportTemplate : "<strong>Records: {totalRecords} </strong> "
    });
    
    var oDataTableCfg = {
    	sortedBy : {key:"query_id", dir:YAHOO.widget.DataTable.CLASS_DESC}
    };
    
    var oElDiv = document.createElement('div');
	oElDiv.id = 'queries_dt';
	oPanel.panel.body.appendChild(oElDiv);
	
	
    try {	
		this.editChartDataTable = new YAHOO.widget.DataTable(oElDiv,	oColumnDefs, oChartsDataSource, oDataTableCfg);
		this.editChartDataTable.handleDataReturnPayload = function(oRequest, oResponse, oPayload){
			oPayload.totalRecords = oResponse.meta.totalRecords;
			return oPayload;
		}
		this.editChartDataTable.subscribe("cellClickEvent", onEventShowCellEditor);
		
		//oPanel.panel.setBody(oElDiv);
		oPanel.panel.body.appendChild(oElDiv);
	}
	catch (e){
		logger.log('Error:', e);
	}
	
	oPanel.panel.show();
	oPanel.panel.bringToTop();
}

YAHOO.ELSA.Chart.prototype.deleteQuery = function(p_sType, p_aArgs, p_a){
	var p_oRecord = p_a[0], p_oDataTable = p_a[1];
	var oSelf = this;
	var p_sPathToQueryDir = '../';
	var iQueryId = p_oRecord.getData().query_id;
	
	var oConfirmationPanel = new YAHOO.ELSA.Panel.Confirmation(function(){
		var oPanel = this;
		YAHOO.ELSA.async(p_sPathToQueryDir + 'Charts/del_query?query_id=' + iQueryId, function(p_oReturn){
			if (!p_oReturn){
				return;
			}
			p_oDataTable.deleteRow(p_oRecord.getId());
			for (var i in oSelf.queries){
				if (oSelf.queries[i].query_id == iQueryId){
					oSelf.queries.splice(i, 1);
					logger.log('queries is now', oSelf.queries);
				}
			}
			oSelf.redraw();
			oPanel.hide();
		});
	}, null, 'Really delete query?');
	logger.log('oConfirmationPanel', oConfirmationPanel);
	oConfirmationPanel.panel.bringToTop();
};

YAHOO.ELSA.Chart.prototype.addQuery = function(p_sType, p_aArgs, p_a){
	var oSelf = this;
	var p_sPathToQueryDir = '../';
	logger.log('addQuery args', arguments);
	//var p_oDataTable = p_a[0];
	var handleSubmit = function(p_sType, p_oDialog){
		//if (!YAHOO.util.Dom.get('query').value.match(/groupby[\:\=](\w+)/i)){
		if (YAHOO.util.Dom.get('groupby').value){
			var sGroupby = YAHOO.util.Dom.get('groupby').value;
			YAHOO.util.Dom.get('query').value += ' groupby:' + sGroupby;
		}
		this.submit();
	};
	var handleCancel = function(){
		this.hide();
	};
	var oPanel = new YAHOO.ELSA.Panel('Create Dashboard', {
		buttons : [ { text:"Submit", handler:handleSubmit, isDefault:true },
			{ text:"Cancel", handler:handleCancel } ]
	});
	var handleSuccess = function(p_oResponse){
		var response = YAHOO.lang.JSON.parse(p_oResponse.responseText);
		if (response['error']){
			YAHOO.ELSA.Error(response['error']);
		}
		else {
			logger.log('response', response);
			oPanel.panel.hide();
			oSelf.editChartDataTable.addRow(response);
			if (!response.query.match(/groupby[\:\=]([\w\.\_]+)/i)){
				response.query += ' groupby:' + YAHOO.ELSA.queryMetaParamsDefaults.groupby;
			}
			
			oSelf.queries.push({label: response.label, query_string: response.query, query_id: response.query_id});
			oSelf.sendQuery(oSelf.queries.length - 1, true);
			//oSelf.redraw();
			logger.log('successful submission');
		}
	};
	oPanel.panel.callback = {
		success: handleSuccess,
		failure: YAHOO.ELSA.Error
	};
	
	oPanel.panel.renderEvent.subscribe(function(){
		oPanel.panel.setBody('');
		oPanel.panel.setHeader('Add Query to Chart');
		oPanel.panel.bringToTop();
		//var sFormId = 'create_dashboard_form';
		var sFormId = oPanel.panel.form.id;
		
		var sGroupby = '';
		var bDisabled = false;
		if (oSelf.queries.length > 0){
			var aMatches = oSelf.queries[0].query_string.match(/groupby[\:\=]([\w\.\_]+)/i);
			if (aMatches){
				sGroupby = aMatches[1];
				bDisabled = true;
			}
		}
		var oFormGridCfg = {
			form_attrs:{
				action: p_sPathToQueryDir + 'Charts/add_query',
				method: 'POST',
				id: sFormId
			},
			grid: [
				[ {type:'text', args:'Label'}, {type:'input', args:{id:'label', name:'label', size:32}} ],
				[ {type:'text', args:'Query'}, {type:'input', args:{id:'query', name:'query', size:64}} ],
				[ {type:'text', args:'Report On'}, {type:'input', args:{id:'groupby', name:'groupby', size:32, value:sGroupby, disabled:bDisabled}} ]
			]
		};
		
		// Now build a new form using the element auto-generated by widget.Dialog
		var oForm = new YAHOO.ELSA.Form(oPanel.panel.form, oFormGridCfg);
		
		var oInputEl = document.createElement('input');
		oInputEl.id = 'chart_id';
		oInputEl.setAttribute('type', 'hidden');
		oInputEl.setAttribute('name', 'chart_id');
		oInputEl.setAttribute('value', oSelf.id);
		oForm.form.appendChild(oInputEl);
	});
	oPanel.panel.render();
	oPanel.panel.show();
	
};


YAHOO.ELSA.addChart = function(p_oEvent, p_a){
	logger.log('arguments', arguments);
	var p_iDashboardId = p_a[0], p_sPathToQueryDir = p_a[1], p_iRowId = p_a[2], p_iCellId = p_a[3];
	if (typeof(p_iRowId) == 'undefined'){
		p_iRowId = YAHOO.ELSA.Chart.getNumRows();
	}
	
	logger.log('creating chart');
	var handleSubmit = function(p_sType, p_oDialog){
		// format the query input param into queries
		var oData = this.getData();
		var oInputEl = document.createElement('input');
		oInputEl.id = p_iDashboardId + '_queries';
		oInputEl.setAttribute('type', 'hidden');
		oInputEl.setAttribute('name', 'queries');
		oInputEl.setAttribute('value', YAHOO.lang.JSON.stringify([{label:oData.label, query:oData.query}]));
		this.form.appendChild(oInputEl);
		this.submit();
	};
	var handleCancel = function(){
		this.hide();
	};
	var oCreatePanel = new YAHOO.ELSA.Panel('Create Chart', {
		buttons : [ { text:"Submit", handler:handleSubmit, isDefault:true },
			{ text:"Cancel", handler:handleCancel } ]
	});
	var handleSuccess = function(p_oResponse){
		var response = YAHOO.lang.JSON.parse(p_oResponse.responseText);
		if (response['error']){
			YAHOO.ELSA.Error(response['error']);
		}
		else {
			oCreatePanel.panel.hide();
			// Account for non-time-based summation
			var oMeta = YAHOO.ELSA.queryMetaParamsDefaults;
			if (response.query.match(/groupby[\:\=]/) || response.query.match(/\| sum\(/)){
				delete oMeta.groupby;
			}
			var iNewRow;
			// new row?
			if (p_iRowId >= YAHOO.ELSA.dashboardRows.length){
				YAHOO.ELSA.dashboardRows.push({
					title: '',
					charts: [
						{
							chart_id: response.chart_id,
							queries: [
								{
									query_string: response.query,
									query_id: response.query_id,
									label: response.label,
									query_meta_params: oMeta
								}
							],
							type: response.chart_type
						}
					]
				});
				iNewRow = YAHOO.ELSA.dashboardRows.length - 1;
			}
			else {
				// adding to an existing row
				YAHOO.ELSA.dashboardRows[p_iRowId].charts.push({
					chart_id: response.chart_id,
					queries: [
						{
							query_string: response.query,
							query_id: response.query_id,
							label: response.label,
							query_meta_params: oMeta
						}
					],
					type: response.chart_type
				});
				iNewRow = p_iRowId;
			}
			
			//YAHOO.ELSA.Chart.loadChartRow(YAHOO.ELSA.dashboardRows.length - 1);
			//YAHOO.ELSA.Chart.loadChartCell(p_iRowId, p_iCellId);
			var iNewCell = YAHOO.ELSA.dashboardRows[iNewRow].charts.length - 1;
			
			YAHOO.ELSA.Chart.loadChartCell(iNewRow, iNewCell);
			logger.log('successful submission');
		}
	};
	oCreatePanel.panel.callback = {
		success: handleSuccess,
		failure: YAHOO.ELSA.Error
	};
	
	oCreatePanel.panel.renderEvent.subscribe(function(){
		oCreatePanel.panel.setBody('');
		oCreatePanel.panel.setHeader('Create New Chart');
		oCreatePanel.panel.bringToTop();
		//var sFormId = 'create_dashboard_form';
		var sFormId = oCreatePanel.panel.form.id;
		
		var sButtonId = 'chart_type_select_button';
		var sId = 'chart_type_input_connector';
		var onMenuItemClick = function(p_sType, p_aArgs, p_oItem){
			var sText = p_oItem.cfg.getProperty("text");
			// Set the label of the button to be our selection
			var oAuthButton = YAHOO.widget.Button.getButton(sButtonId);
			oAuthButton.set('label', sText);
			var oFormEl = YAHOO.util.Dom.get(sFormId);
			var oInputEl = YAHOO.util.Dom.get(sId);
			oInputEl.setAttribute('value', p_oItem.value);
		}
		
		var aMenu = [
			{ text:'Bar', value:'ColumnChart', onclick: { fn: onMenuItemClick } },
			{ text:'Pie', value:'PieChart', onclick: { fn: onMenuItemClick } },
			{ text:'Table', value:'Table', onclick: { fn: onMenuItemClick } }
		];
		
		var oMenuButtonCfg = {
			id: sButtonId,
			type: 'menu',
			label: 'Chart Type',
			name: sButtonId,
			menu: aMenu
		};
		var oFormGridCfg = {
			form_attrs:{
				action: p_sPathToQueryDir + 'Charts/add',
				method: 'POST',
				id: sFormId
			},
			grid: [
				[ {type:'text', args:'Title'}, {type:'input', args:{id:'chart_title', name:'title', size:32}} ],
				[ {type:'text', args:'Type'}, {type:'widget', className:'Button', args:oMenuButtonCfg} ],
				[ {type:'text', args:'Query:'} ],
				[ {type:'text', args:'Label'}, {type:'input', args:{id:'label', name:'label', size:32}} ],
				[ {type:'text', args:'Query'}, {type:'input', args:{id:'query', name:'query', size:64}} ],
			]
		};
		
		// Now build a new form using the element auto-generated by widget.Dialog
		var oForm = new YAHOO.ELSA.Form(oCreatePanel.panel.form, oFormGridCfg);
		
		var oInputEl = document.createElement('input');
		oInputEl.id = sId;
		oInputEl.setAttribute('type', 'hidden');
		oInputEl.setAttribute('name', 'chart_type');
		oInputEl.setAttribute('value', 0);
		oForm.form.appendChild(oInputEl);
		
		var oDashboardInputEl = document.createElement('input');
		oDashboardInputEl.id = 'dashboard_id';
		oDashboardInputEl.setAttribute('type', 'hidden');
		oDashboardInputEl.setAttribute('name', 'dashboard_id');
		oDashboardInputEl.setAttribute('value', p_iDashboardId);
		oForm.form.appendChild(oDashboardInputEl);
		
		if (typeof(p_iRowId) != 'undefined' && typeof(p_iCellId) != 'undefined'){
			var oMap = { x:p_iCellId, y:p_iRowId };
			var oElements = {};
			for (var i in oMap){
				oElements[i] = document.createElement('input');
				oElements[i].id = 'coordinate_' + p_iDashboardId + '_' + p_iRowId + '_' + p_iCellId; 
				oElements[i].setAttribute('type', 'hidden');
				oElements[i].setAttribute('name', i);
				oElements[i].setAttribute('value', oMap[i]);
				oForm.form.appendChild(oElements[i]);
			}
		}
	});
	oCreatePanel.panel.render();
	oCreatePanel.panel.show();
}


