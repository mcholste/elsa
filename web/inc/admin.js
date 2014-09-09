YAHOO.namespace('YAHOO.ELSA.Admin');

YAHOO.ELSA.Admin.main = function(){
	// Set viewMode for dev/prod
	var oRegExp = new RegExp('\\Wview=(\\w+)');
	var aMatches = oRegExp.exec(location.search);
	if (aMatches){
		YAHOO.ELSA.viewMode = aMatches[1];
	}
	
	aMatches = location.search.match(/search=(.+)/, 'i');
	var sSearch = '';
	if (aMatches){
		sSearch = aMatches[1];
	}
	
	YAHOO.ELSA.initLogger();
	
	// Create search form
	var oTable = document.createElement('table');
	oTable.id = 'search_form';
	var oTr = document.createElement('tr');
	oTable.appendChild(oTr);
	
	var oTd = document.createElement('td');
	oTd.appendChild(document.createTextNode('Search Groups'));
	oTr.appendChild(oTd);
	
	var oTd = document.createElement('td');
	var oForm = document.createElement('form');
	oForm.action = '?';
	var oInput = document.createElement('input');
	oInput.name = 'search';
	oInput.value = sSearch;
	oForm.appendChild(oInput);
	var oSubmit = document.createElement('input');
	oSubmit.type = 'submit';
	oForm.appendChild(oSubmit);
	oTd.appendChild(oForm);
	oTr.appendChild(oTd);
	YAHOO.util.Dom.get('permissions').appendChild(oTable);
	
	var load = function(oResponse){
		logger.log('oResponse', oResponse);
		YAHOO.ELSA.Admin.formParams = oResponse.form_params;
		
		parseExceptions = function(p_oExceptions){
			return p_oExceptions;
		}
		
		// Show LDAP-based permissions
		try {
			YAHOO.ELSA.Admin.main.dataSource = new YAHOO.util.DataSource(oResponse);
			YAHOO.ELSA.Admin.main.dataSource.responseType = YAHOO.util.DataSource.TYPE_JSON;
			YAHOO.ELSA.Admin.main.dataSource.responseSchema = {
				resultsList: "results",
				fields: [ 
					{ key:'uid', parser:YAHOO.util.DataSourceBase.parseString },
					{ key:'username', parser:YAHOO.util.DataSourceBase.parseString },
					{ key: 'gid', parser:YAHOO.util.DataSourceBase.parseString },
					{ key: 'groupname', parser:YAHOO.util.DataSourceBase.parseString },
					{ key: 'has_exceptions', parser:YAHOO.util.DataSourceBase.parseString },
					{ key: '_exceptions' }
				],
				metaFields: {
					totalRecords: 'totalRecords',
					recordsReturned: 'recordsReturned'
				}
			};
		}
		catch (e){
			YAHOO.ELSA.Error(e);
			return;
		}
		
		var formatExceptions = function(elLiner, oRecord, oColumn, oData){
			if (parseInt(oRecord.getData().has_exceptions)){
				logger.log('oRecord.getData().has_exceptions', oRecord.getData().has_exceptions);
				var oElLink = document.createElement('a');
				oElLink.id = 'permissions_exceptions_' + oRecord.getId();
				oElLink.innerHTML = 'Exceptions';
				oElLink.href = '#';
				elLiner.appendChild(oElLink);
				var oEl = new YAHOO.util.Element(oElLink);
				oEl.subscribe('click', function(){
					YAHOO.ELSA.Admin.showExceptions(oRecord.getData());
				});
			}
			
			var oButton = new YAHOO.widget.Button({
				container: elLiner,
				type: 'button', 
				label: 'Add', 
				id: 'permissions_add_exception_' + oRecord.getData().gid + '_' + oRecord.getData().attr_id, 
				value: 'Add',
				onclick: {
					fn: function(){
						YAHOO.ELSA.Admin.addException(oRecord.getData());
					}
				}
			});
		}
		
		try {
			var aDefaultColumnDefs = [
				{ key:"groupname", label:"Group", sortable:true },
				{ key:'_exceptions', label:'Exceptions', formatter:formatExceptions, sortable:true }
			];
			var oDefaultPaginator = new YAHOO.widget.Paginator({
			    pageLinks          : 10,
		        rowsPerPage        : 5,
		        rowsPerPageOptions : [15,30,60],
		        template           : "{CurrentPageReport} {PreviousPageLink} {PageLinks} {NextPageLink} {RowsPerPageDropdown}",
		        pageReportTemplate : "<strong>Records: {totalRecords} </strong> "
		    });
		    var oDefaultDataTableCfg = {
		    	paginator: oDefaultPaginator
		    };
		    
		}
		catch (e){
			YAHOO.ELSA.Error(e);
			return;
		}
		
	    var dtDiv = document.createElement('div');
		dtDiv.id = 'get_permissions_dt';
		YAHOO.util.Dom.get('permissions').appendChild(dtDiv);
		
		try {
			YAHOO.ELSA.Admin.main.dataTable = new YAHOO.widget.DataTable(dtDiv, aDefaultColumnDefs, YAHOO.ELSA.Admin.main.dataSource, oDefaultDataTableCfg );
			YAHOO.ELSA.Admin.main.dataTable.handleDataReturnPayload = function(oRequest, oResponse, oPayload){
				oPayload.totalRecords = oResponse.meta.totalRecords;
				return oPayload;
			}
			YAHOO.ELSA.Admin.main.dataTable.render();
		}
		catch (e){
			logger.log('Error:', e);
		}
	}
	
	var request = YAHOO.util.Connect.asyncRequest('GET', 'Query/get_permissions?search=' + sSearch,
		{ 
			success:function(oResponse){
				if (oResponse.responseText){
					var oReturn = YAHOO.lang.JSON.parse(oResponse.responseText);
					if (typeof oReturn === 'object' && oReturn['error']){
						YAHOO.ELSA.Error(oReturn['error']);
						return;
					}
					else if (oReturn){
						load(oReturn);
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
}

YAHOO.ELSA.Admin.deleteExceptions = function(){
	var aToDelete = [];
	var oElements = YAHOO.util.Dom.getElementsByClassName('delete_permissions_checkbox');
	for (var i in oElements){
		var oEl = oElements[i];
		if(!oEl.checked){
			continue;
		}
		logger.log('oEl', oEl);
		var sRawJson = oEl.value;
		var oArgs = YAHOO.lang.JSON.parse(sRawJson);
		aToDelete.push(oArgs);
	}
	var reqStr = 'Query/set_permissions?action=delete&permissions=' + YAHOO.lang.JSON.stringify(aToDelete);
	var request = YAHOO.util.Connect.asyncRequest('GET', reqStr,
		{ 
			success:function(oResponse){
				if (oResponse.responseText){
					var oReturn = YAHOO.lang.JSON.parse(oResponse.responseText);
					if (typeof oReturn === 'object' && oReturn['error']){
						YAHOO.ELSA.Error(oReturn['error']);
						return;
					}
					else if (oReturn){
						logger.log('Successfully updated permissions');
						
						for (var i in YAHOO.ELSA.Admin.main.dataTable.getRecordSet().getRecords()){
							i = parseInt(i);
							for (var j in aToDelete){
								if (YAHOO.ELSA.Admin.main.dataTable.getRecord(i).getData().gid == aToDelete[j].gid){
									var oExceptions = YAHOO.ELSA.Admin.main.dataTable.getRecord(i).getData()._exceptions;
									for (var sAttr in oExceptions){
										for (var sAttrId in oExceptions[sAttr]){
											logger.log('i ' + i + ', sAttr ' + sAttr + ', sAttrId ' + sAttrId + ', p_iAttrId ' + aToDelete[j].attr_id);
											if (sAttrId == aToDelete[j].attr_id){
												logger.log('deleting sAttr ' + sAttr + ' and sAttrId ' + sAttrId);
												delete oExceptions[sAttr][sAttrId];
											}
										}
									}
								}
							}
						}
						
						// find the row in the datatable and delete it
						//logger.log('aRecordSet', aRecordSet);
						if (YAHOO.ELSA.Admin.main.permissionsDataTable){
							for (var i in YAHOO.ELSA.Admin.main.permissionsDataTable.getRecordSet().getRecords()){
								i = parseInt(i);
								for (var j in aToDelete){
									logger.log('checking attr: ' + YAHOO.ELSA.Admin.main.permissionsDataTable.getRecord(i).getData().attr +
										' against ' + aToDelete[j].attr + ' and value ' + YAHOO.ELSA.Admin.main.permissionsDataTable.getRecord(i).getData().value +
										' against ' + aToDelete[j].attr_id);
									if (YAHOO.ELSA.Admin.main.permissionsDataTable.getRecord(i).getData().attr == aToDelete[j].attr
										&& YAHOO.ELSA.Admin.main.permissionsDataTable.getRecord(i).getData().value == aToDelete[j].attr_id){
										logger.log('deleting record ' + i);
										var sGroupName = YAHOO.ELSA.Admin.main.permissionsDataTable.getRecord(i).getData().groupname;
										YAHOO.ELSA.Admin.main.permissionsDataTable.deleteRow(i);
										
										// update the main table cell to remove this link
										if (!YAHOO.ELSA.Admin.main.permissionsDataTable.getRecordSet().getLength()){
											for (var j in YAHOO.ELSA.Admin.main.dataTable.getRecordSet().getRecords()){
												j = parseInt(j);
												if (YAHOO.ELSA.Admin.main.dataTable.getRecord(j).getData().groupname == sGroupName){
													var oRecord = YAHOO.ELSA.Admin.main.dataTable.getRecord(j);
													YAHOO.util.Dom.get('permissions_exceptions_' + oRecord.getId()).innerHTML = '';
													break;
												}
											}
										}
										break;
									}
								}
							}
						}
						//logger.log('aRecordSet', aRecordSet);
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
			failure:function(oResponse){ YAHOO.ELSA.Error('Failed to update ' + p_iGid); },
			argument: [this]
		});
}

YAHOO.ELSA.Admin.addException = function(p_oRecordData){
	YAHOO.ELSA.addExceptionRecordData = p_oRecordData; //why must we do this?  it doesn't seem to affect setting the panel header
	var submit = function(p_oEvent){
		logger.log('YAHOO.ELSA.addException.recordData', YAHOO.ELSA.addExceptionRecordData);
		var args = oPanel.argsToSubmit;
		//tack on input field
		if (YAHOO.util.Dom.get('add_exception_form_' + YAHOO.ELSA.addExceptionRecordData.gid + '_selected_host').value){
			args.attr_id = YAHOO.util.Dom.get('add_exception_form_' + YAHOO.ELSA.addExceptionRecordData.gid + '_selected_host').value;
			args.attr = 'host_id';
		}
		else if (YAHOO.util.Dom.get('add_exception_form_' + YAHOO.ELSA.addExceptionRecordData.gid + '_selected_attr').value){
			args.attr = YAHOO.util.Dom.get('add_exception_form_' + YAHOO.ELSA.addExceptionRecordData.gid + '_selected_attr').value;
			args.text = args.attr_id = YAHOO.util.Dom.get('add_exception_form_' + YAHOO.ELSA.addExceptionRecordData.gid + '_selected_value').value;
		}
		else if (YAHOO.util.Dom.get('add_exception_form_' + p_oRecordData.gid + '_string').value){
			args.attr = 'filter';
			args.text = args.attr_id = YAHOO.util.Dom.get('add_exception_form_' + YAHOO.ELSA.addExceptionRecordData.gid + '_string').value;
		}
				
		var argStr = YAHOO.lang.JSON.stringify([args]);
		//var reqStr = 'Query/set_permissions_exception?action=add&exception=' + argStr;
		var reqStr = 'Query/set_permissions?action=add&permissions=' + argStr;
		logger.log('this', this);
		var request = YAHOO.util.Connect.asyncRequest('GET', reqStr,
			{ 
				success:function(oResponse){
					var oDialogPanel = oResponse.argument[0];
					logger.log('oDialogPanel', oDialogPanel);
					oDialogPanel.hide();
					if (oResponse.responseText){
						var oReturn = YAHOO.lang.JSON.parse(oResponse.responseText);
						if (typeof oReturn === 'object' && oReturn['error']){
							YAHOO.ELSA.Error(oReturn['error']);
							return;
						}
						else if (oReturn){
							logger.log('Successfully updated group ' + YAHOO.ELSA.addExceptionRecordData.gid);
							// Add the exceptions link to the table cell
							for (var i in YAHOO.ELSA.Admin.main.dataTable.getRecordSet().getRecords()){
								logger.log('i ' + i);
								i = parseInt(i);
								if (YAHOO.ELSA.Admin.main.dataTable.getRecord(i).getData().gid == YAHOO.ELSA.addExceptionRecordData.gid){
									var oRecord = YAHOO.ELSA.Admin.main.dataTable.getRecord(i);
									logger.log('oRecord', oRecord);
									logger.log('column', YAHOO.ELSA.Admin.main.dataTable.getColumn('_exceptions'));
									var oElLiner = YAHOO.ELSA.Admin.main.dataTable.getTdLinerEl({
										record: oRecord,
										column: YAHOO.ELSA.Admin.main.dataTable.getColumn('_exceptions')
									});
									
									// check to see if we need to update this record
									if (YAHOO.ELSA.Admin.exceptionsGid == oRecord.getData().gid){
										logger.log('args', args);
										var oData = oRecord.getData();
										logger.log('oData', oData);
										if (typeof(oData._exceptions[args.attr]) == 'undefined'){
											oData._exceptions[args.attr] = {};
										}
										oData._exceptions[args.attr][args.text] = args.attr_id;
										YAHOO.ELSA.Admin.main.dataTable.updateRow(i, oData);
										// check to see if we need to update the displayed exceptions
										if (YAHOO.ELSA.Admin.main.permissionsDataTable && YAHOO.ELSA.Admin.exceptionsGid == oRecord.getData().gid){
											logger.log('getting exceptions with record ', oData);
											YAHOO.ELSA.Admin.showExceptions(oData);
										}
									}
									else {
										logger.log('exceptionsGid: ' + YAHOO.ELSA.Admin.exceptionsGid + ' oRecord.getData.gid ' + oRecord.getData().gid);
									}
										
									var oElA = YAHOO.util.Dom.get('permissions_exceptions_' + oRecord.getId());
									if (!oElA){
										var oElLink = document.createElement('a');
										oElLink.id = 'permissions_exceptions_' + oRecord.getId();
										oElLink.innerHTML = 'Exceptions';
										oElLink.href = '#';
										oElLiner.appendChild(oElLink);
										var oEl = new YAHOO.util.Element(oElLink);
										oEl.subscribe('click', function(){
											YAHOO.ELSA.Admin.showExceptions(oRecord.getData());
										});	
									}
									else {
										oElA.innerHTML = 'Exceptions';
									}
									
									break;
								}
							}
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
				failure:function(oResponse){ var oDialogPanel = oResponse.argument[0]; oDialogPanel.hide(); YAHOO.ELSA.Error('Failed to update ' + YAHOO.ELSA.addExceptionRecordData.gid); },
				argument: [this]
			});
	}
	var handleSubmit = function(){
		this.submit();
	};
	var handleCancel = function(){
		this.hide();
	};
	var oPanel = new YAHOO.ELSA.Panel('add_exception', {
		buttons : [ 
			{ text:"Submit", handler:{fn:submit}, isDefault:true },
			{ text:"Cancel", handler:handleCancel } ]
	});
	
	oPanel.panel.setHeader('Add exception for ' + p_oRecordData.groupname);
	oPanel.panel.setBody('');
	oPanel.panel.render();
	
	oPanel.argsToSubmit = { gid:p_oRecordData.gid };
	var onMenuSelect = function(p_sType, p_aArgs, p_oItem){
		logger.log('onMenuSelect p_aArgs', p_aArgs);
		logger.log('onMenuSelect p_oItem', p_oItem);
		oPanel.argsToSubmit = p_oItem;
		var oButton;
		if (p_oItem.attr == 'class_id'){
			oButton = YAHOO.widget.Button.getButton('add_exception_form_' + p_oRecordData.gid + '_selected_class');
		}
//		else if (p_oItem.attr == 'program_id'){
//			oButton = YAHOO.widget.Button.getButton('add_exception_form_' + p_oRecordData.gid + '_selected_program');
//		}
		else if (p_oItem.attr == 'node_id'){
			oButton = YAHOO.widget.Button.getButton('add_exception_form_' + p_oRecordData.gid + '_selected_node');
		}
				
		oButton.set('label', p_oItem.text);
	}
	
	var aMenus = [];
	
	// create the classes menu
	var aClassesMenuItems = [];
	for (var sClassName in YAHOO.ELSA.Admin.formParams.classes){
		if (!sClassName){
			sClassName = 'All';
		}
		//is this currently a blacklist?
		var oMenuItem = {
			text: sClassName,
			value: sClassName, 
			onclick: { 
				fn: onMenuSelect,
				obj: { 
					gid: p_oRecordData.gid, 
					attr: 'class_id', 
					attr_id: YAHOO.ELSA.Admin.formParams.classes[sClassName], 
					text: sClassName
				}
			}
		}
		aClassesMenuItems.push(oMenuItem);
	}
	logger.log('aClassesMenuItems', aClassesMenuItems);
	var oClassMenuCfg = {
		type:'menu',
		id: 'add_exception_form_' + p_oRecordData.gid + '_selected_class',
		name: 'add_exception_form_' + p_oRecordData.gid + '_selected_class',
		menu: aClassesMenuItems,
		label: 'Class'
	}
	
	// create the nodes menu
	var aNodeMenuItems = [];
	for (var i in YAHOO.ELSA.Admin.formParams.nodes){
		var sNode = YAHOO.ELSA.Admin.formParams.nodes[i];
		var oMenuItem = {
			text: sNode,
			value: sNode, 
			onclick: { 
				fn: onMenuSelect,
				obj: { 
					gid: p_oRecordData.gid, 
					attr: 'node_id', 
					attr_id: sNode, 
					text: sNode
				}
			}
		}
		aNodeMenuItems.push(oMenuItem);
	}
	logger.log('aNodeMenuItems', aNodeMenuItems);
	var oNodeMenuCfg = {
		type:'menu',
		id: 'add_exception_form_' + p_oRecordData.gid + '_selected_node',
		name: 'add_exception_form_' + p_oRecordData.gid + '_selected_node',
		menu: aNodeMenuItems,
		label: 'Node'
	}
	
	var oFormCfg = {
		form_attrs:{
			id: 'add_exception_form_' + p_oRecordData.gid,
		},
		grid: [
			[ {type:'text', args:'Allow a given class'}, {type:'widget', className:'Button', args:oClassMenuCfg} ],
			[ {type:'text', args:'Allow a given node'}, {type:'widget', className:'Button', args:oNodeMenuCfg} ],
			[ {type:'text', args:'Allow a given host'}, {type:'input', args:{id:'add_exception_form_' + p_oRecordData.gid + '_selected_host', size:35}} ],
			[ {type:'text', args:'Arbitrary attr/value pair Attribute:'}, {type:'input', args:{id:'add_exception_form_' + p_oRecordData.gid + '_selected_attr', size:35} }, {type:'text', args:'Value:' }, {type:'input', args:{id:'add_exception_form_' + p_oRecordData.gid + '_selected_value', size:35} } ],
			[ {type:'text', args:'Arbitrary string to add to each query:'}, {type:'input', args:{id:'add_exception_form_' + p_oRecordData.gid + '_string', size:35} } ]
		]		
	}
	
	var oForm = new YAHOO.ELSA.Form(oPanel.panel.form, oFormCfg);
	oPanel.panel.show();
	
	logger.log('oPanel', oPanel);
}

YAHOO.ELSA.Admin.showExceptions = function(p_oData){
	
	logger.log('p_oData', p_oData);
	
	YAHOO.ELSA.Admin.exceptionsGid = p_oData.gid;
	
	var oDiv = YAHOO.util.Dom.get('exceptions');
	
	var oButton = YAHOO.widget.Button.getButton('delete_exceptions_button');
	if (!oButton){
		oButton = new YAHOO.widget.Button({
			container: 'delete_exceptions_button_container',
			id: 'delete_exceptions_button',
			type: 'button', 
			label: 'Delete Checked',
			onclick: {
				fn: YAHOO.ELSA.Admin.deleteExceptions
			}
		});
	}
	
	// format this data as a JS_ARRAY
	var aData = [];
	for (var attr in p_oData._exceptions){
		for (var attr_value in p_oData._exceptions[attr]){
			aData.push({
				gid: p_oData.gid,
				attr: attr,
				display_value: attr_value,
				value: p_oData._exceptions[attr][attr_value]
			});
		}
	}
	
	var formatterValue = function(elLiner, oRecord, oColumn, oData){
		var oElCheckbox = document.createElement('input');
		oElCheckbox.id = 'delete_permission-' + oRecord.getData().attr + '-' + oRecord.getData().value;
		oElCheckbox.type = 'checkbox';
		oElCheckbox.value = YAHOO.lang.JSON.stringify({
			gid: oRecord.getData().gid,
			attr: oRecord.getData().attr,
			attr_id: oRecord.getData().value
		});
		elLiner.appendChild(oElCheckbox);
		var oEl = new YAHOO.util.Element(oElCheckbox);
		oEl.addClass('delete_permissions_checkbox');
	}
	
	try {
		logger.log('aData', aData);
		var oDataSource = new YAHOO.util.DataSource(aData);
		oDataSource.responseType = YAHOO.util.DataSource.TYPE_JSARRAY;
		var aColumnDefs = [
			{ key:'value', label:'Delete', formatter:formatterValue },
			{ key:'attr', label:'Attribute', sortable:true },
			{ key:'display_value', label:'Value', sortable:true }
		];
		var oPaginator = new YAHOO.widget.Paginator({
		    pageLinks          : 10,
	        rowsPerPage        : 5,
	        rowsPerPageOptions : [15,30,60],
	        template           : "{CurrentPageReport} {PreviousPageLink} {PageLinks} {NextPageLink}",
	        pageReportTemplate : "<strong>Records: {totalRecords} </strong> "
	    });
	    var oDataTableCfg = {
	    	//paginator: oPaginator
	    };
		YAHOO.ELSA.Admin.main.permissionsDataTable = new YAHOO.widget.DataTable('exceptions', 
	    	aColumnDefs, oDataSource, oDataTableCfg );
		YAHOO.ELSA.Admin.main.permissionsDataTable.render();
		
	}	
	catch (e){
		YAHOO.ELSA.Error(e);
		return;
	}
}