
var logger; //pretty global, but everybody needs it

YAHOO.ELSA.main = function () {
	// Set viewMode for dev/prod
	var oRegExp = new RegExp('\\Wview=(\\w+)');
	var oMatches = oRegExp.exec(location.search);
	if (oMatches){
		YAHOO.ELSA.viewMode = oMatches[1];
	}
	
	YAHOO.ELSA.initLogger();
	
	YAHOO.ELSA.currentQuery = new YAHOO.ELSA.Query();
	
	var sArchiveMenuButtonName = 'archive_menu_select_button';
	var sGroupByMenuSelectButtonName = 'groupby_menu_select_button';
	
	var submitQuery = function(){
		var oQuery = new YAHOO.ELSA.Query();
		oQuery.queryString = cloneVar(YAHOO.ELSA.currentQuery.queryString);
		oQuery.metas = cloneVar(YAHOO.ELSA.currentQuery.metas);
		// apply the start/stop times
		try {
//			var oStartTime, oEndTime;
//			if (YAHOO.util.Dom.get('start_time').value){
//				oQuery.addMeta('start', YAHOO.util.Dom.get('start_time').value);
//				//oStartTime = getDateFromISO(YAHOO.util.Dom.get('start_time').value)/1000;
//				//if (!oStartTime){
//				//	YAHOO.ELSA.Error('Invalid start time');
//				//	return;
//				//}
//				//else {
//				//	oQuery.addMeta('start', oStartTime);
//				//}
//			}
//			//else {
//			//	oQuery.delMeta('start');
//			//}
//			if (YAHOO.util.Dom.get('end_time').value){
//				oEndTime = getDateFromISO(YAHOO.util.Dom.get('end_time').value)/1000;
//				if (!oEndTime){
//					YAHOO.ELSA.Error('Invalid end time');
//					return;
//				}
//				else {
//					oQuery.addMeta('end', oEndTime);
//				}
//			}
//			else {
//				oQuery.delMeta('end');
//			}
//			if (oStartTime > oEndTime){
//				YAHOO.ELSA.Error('Start time greater than end time');
//				return;
//			}
//			logger.log('submitting query: ', oQuery);
//						
//			var oResults = new YAHOO.ELSA.Results.Tabbed.Live(YAHOO.ELSA.tabView, oQuery);
//			logger.log('got query results:', oResults);
//			YAHOO.ELSA.currentQuery.resetTerms();
			oQuery.submit();
		} catch(e) { YAHOO.ELSA.Error(e); }
	}
	
	var drawQueryForm = function(){		
		var oDialog=null;
		
		YAHOO.ELSA.formParams = formParams;
		YAHOO.ELSA.formParams.classIdMap = formParams['classes'];
		YAHOO.ELSA.formParams.classIdMap['0'] = 'ALL';
		
		var oSubmitButtonConfig = { 
			type: "button", 
			label: "Submit Query", 
			id: "query_submit"
		};
		
		var onTermMetaSelectionClick = function(p_sType, p_aArgs, p_oItem){
			logger.log('adding meta', p_oItem);
			var id = this.element.id;
			logger.log('id:' + this.element.id);
			var op = '=';
			if (p_oItem.className){
				YAHOO.ELSA.currentQuery.addTerm('class', p_oItem.className, op);
			}
			if (p_oItem.program){
				YAHOO.ELSA.currentQuery.addTerm('program', p_oItem.program, op);
			}
			
		}
		
		var onTermSelectionClick = function(p_sType, p_aArgs, p_oItem){
			logger.log('this', this);
			logger.log('p_aArgs', p_aArgs);
			logger.log('p_oItem', p_oItem);
			//logger.log('oMenu', oMenu);
			var id = this.element.id;
			logger.log('id:' + this.element.id);
			var oPanel = new YAHOO.ELSA.Panel(this.element.id, {context:[this.element.id, 'tl', 'tr']});
			// find field type to determine if we can do a range comparison
			var sType = 'string';
			for (var i in YAHOO.ELSA.formParams.fields){
				//logger.log(YAHOO.ELSA.formParams.fields[i]);
				//logger.log(YAHOO.ELSA.formParams.fields[i].field_type + ' ' + this.element.id);
				if (YAHOO.ELSA.formParams.fields[i].fqdn_field === this.element.id){
					sType = YAHOO.ELSA.formParams.fields[i].field_type;
					break;
				}
			}
			if (sType === 'int'){
				var elOperator = document.createElement('select');
				elOperator.id = 'add_term_op_' + this.element.id;
				var aOps = ['=', '!=', '>=', '<='];
				for (var i in aOps){
					var elOption = document.createElement('option');
					elOption.value = aOps[i];
					elOption.innerHTML = aOps[i];
					elOperator.appendChild(elOption);
				}
				oPanel.panel.body.appendChild(elOperator);
			}
			var elInput = document.createElement('input');
			elInput.type = 'text';
			elInput.id = 'add_term_' + this.element.id;
			oPanel.panel.setHeader(p_oItem.fqdn_field);
			oPanel.panel.body.appendChild(elInput);
			var fnButtonSubmit = function(oEvent){
				// Make sure we don't submit the form
				YAHOO.util.Event.stopEvent(oEvent);
				var tgt=(oEvent.target ? oEvent.target : 
					(oEvent.srcElement ? oEvent.srcElement : null)); 
				try{
					tgt.blur();
				}
				catch(e){}
				var op = '=';
				if (YAHOO.util.Dom.get('add_term_op_' + id)){
					op = YAHOO.util.Dom.get('add_term_op_' + id).value;
				}
				if (YAHOO.ELSA.currentQuery.addTerm(p_oItem.fqdn_field, elInput.value, op, elInput)){
					oPanel.panel.hide();
				}
			};
			var oSubmitButtonCfg = {
				container: oPanel.panel.body,
				id: 'term_menu_submit_button',
				type: 'button',
				label: 'Add',
				name: 'term_menu_submit_button',
				onclick: { 	
					fn: fnButtonSubmit,
					scope: YAHOO.ELSA,
					correctScope: false
				}
			};
			var oSubmitButton = new YAHOO.widget.Button(oSubmitButtonCfg);
			oPanel.panel.show();
			elInput.focus();
			var enterKeyListener = new YAHOO.util.KeyListener(
					elInput,
					{ keys: 13 },
					{ 	
						fn: function(eName, p_aArgs){
							var oEvent = p_aArgs[1];
							fnButtonSubmit(oEvent);
						},
						scope: YAHOO.ELSA,
						correctScope: false
					}
			);
			enterKeyListener.enable();
		}
		
		// Build term selection menu
		var oFields = {
			'none': {
				text: 'Unclassified',
				submenu: { id:'none', itemdata: [
					{
						text: 'Unclassified',
						id: 'none',
						onclick: { 
							fn: onTermMetaSelectionClick,
							obj: { className:'none', id:'none' }
						}
					}
				] }
			}
		};
		for (var i in YAHOO.ELSA.formParams.fields){
			var sClass = YAHOO.ELSA.formParams.fields[i]['class'];
			var sField = YAHOO.ELSA.formParams.fields[i]['value'];
			if (!sClass){
				continue;
			}
			if (!oFields[sClass]){
				// find class id
				var iClassId;
				for (var j in YAHOO.ELSA.formParams.classes){
					if (sClass === YAHOO.ELSA.formParams.classes[j]){
						iClassId = j;
						break;
					}
				}
				
				oFields[sClass] = {
					text: sClass,
					submenu: { id:sClass, itemdata: [
						{
							text: 'Class ' + sClass,
							id: sClass,
							onclick: { 
								fn: onTermMetaSelectionClick,
								obj: { className:sClass, id:sClass }
							}
						}
					] }
				};
			}
			oFields[sClass]['submenu']['itemdata'].push({
				text: 'Field ' + sField,
				id: sClass + '.' + sField,
				onclick: { fn:onTermSelectionClick, obj:{ fqdn_field:sClass + '.' + sField, id:sClass + '_' + sField } }
			});
		}
		
		var aTermMenuItems = [];
		for (var sClass in oFields){
			aTermMenuItems.push(oFields[sClass]);
		}
		logger.log('aTermMenuItems', aTermMenuItems);
		
		var oTermMenuButtonCfg = {
			id: 'term_menu_select_button',
			type: 'menu',
			label: YAHOO.ELSA.Labels.noTerm,
			name: 'term_menu_select_button',
			menu: aTermMenuItems
		};
		
		// Groupby menu
	
		//	"click" event handler for each item in the Button's menu
		var onGroupBySelectionClick = function(p_sType, p_aArgs, p_oItem){
			logger.log('p_oItem:', p_oItem);
			var sText = p_oItem.fqdn_field;
			var aClassVal = sText.split(/\./);
			// Set the label of the button to be our selection
			var oButton = YAHOO.widget.Button.getButton(sGroupByMenuSelectButtonName);
			oButton.set('label', sText);
			logger.log('oButton:', oButton);
			
			// reset old values
			YAHOO.ELSA.currentQuery.delMeta('groupby');
			//YAHOO.ELSA.currentQuery.delMeta('class');
			//YAHOO.ELSA.currentQuery.delMeta('limit');
			oButton.removeClass('elsa-accent');
			
			if (aClassVal[0] != YAHOO.ELSA.Labels.noGroupBy){
				YAHOO.ELSA.currentQuery.addMeta('groupby', [sText]);
				oButton.addClass('elsa-accent');
			}
			//else groupby is cleared by above delMetas
		}
		
		var aUnclassedFields = ['Host', 'Class', 'Program', 'Day', 'Hour', 'Minute', 'Timestamp', 'Node'];
		var aUnclassedItems = [];
		for (var i in aUnclassedFields){
			var sPrettyClass = aUnclassedFields[i];
			var sClass = sPrettyClass.toLowerCase();
			aUnclassedItems.push({
				text: sPrettyClass,
				id: 'groupby_any.' + sClass,
				onclick: { 
					fn: onGroupBySelectionClick, 
					obj: { 
						fqdn_field: sClass, 
						//fqdn_field: 'any' + '.' + sClass,
						id: 'any' + '_' + sClass 
					} 
				}
			});
		}
		
		// Build term selection menu
		var aGroupByMenuItems = [
			{
				text: YAHOO.ELSA.Labels.noGroupBy,
				onclick: { fn:onGroupBySelectionClick, obj:{ fqdn_field:YAHOO.ELSA.Labels.noGroupBy } }
			},
			{
				text: 'All Classes',
				onclick: { fn:onGroupBySelectionClick, obj:{ fqdn_field:'Any', id:'groupby_Any' } },
				submenu: {
					id: 'groupby_Any',
					itemdata: aUnclassedItems
				}
			}
		];
		var oGroupByFields = {};
		for (var i in YAHOO.ELSA.formParams.fields){
			var sClass = YAHOO.ELSA.formParams.fields[i]['class'];
			var sField = YAHOO.ELSA.formParams.fields[i]['value'];
			if (!sClass){
				continue;
			}
			if (!oGroupByFields[sClass]){
				oGroupByFields[sClass] = {
					text: sClass,
					submenu: { id:'groupby_' + sClass, itemdata: [] }
				};
			}
			oGroupByFields[sClass]['submenu']['itemdata'].push({
				text: sField,
				id: 'groupby_' + sClass + '.' + sField,
				onclick: { fn:onGroupBySelectionClick, obj:{ fqdn_field:sClass + '.' + sField, id:sClass + '_' + sField } }
			});
		}
		for (var i in oGroupByFields){
			aGroupByMenuItems.push(oGroupByFields[i]);
		}
		logger.log('aGroupByMenuItems', aGroupByMenuItems);
		
		var oGroupByMenuButtonCfg = {
			id: sGroupByMenuSelectButtonName,
			type: 'menu',
			label: YAHOO.ELSA.Labels.defaultGroupBy,
			name: sGroupByMenuSelectButtonName,
			menu: aGroupByMenuItems
		};
		
		var onArchiveSelectionClick = function(p_sType, p_aArgs, p_oItem){
			logger.log('p_oItem:', p_oItem);
			var sText = p_oItem;
			// Set the label of the button to be our selection
			var oButton = YAHOO.widget.Button.getButton(sArchiveMenuButtonName);
			oButton.set('label', sText);
			logger.log('oButton:', oButton);
			
			if (sText == YAHOO.ELSA.Labels.index){
				YAHOO.ELSA.currentQuery.delMeta('livetail');
				YAHOO.ELSA.currentQuery.delMeta('archive');
				YAHOO.ELSA.currentQuery.delMeta('analytics');
				YAHOO.ELSA.currentQuery.delMeta('connector');
				YAHOO.ELSA.currentQuery.delMeta('connector_params');
				oButton.removeClass('elsa-accent');
			}
			else if (sText == YAHOO.ELSA.Labels.archive){
				YAHOO.ELSA.currentQuery.addMeta('archive', 1);
				YAHOO.ELSA.currentQuery.delMeta('livetail');
				YAHOO.ELSA.currentQuery.delMeta('analytics');
				YAHOO.ELSA.currentQuery.delMeta('connector');
				YAHOO.ELSA.currentQuery.delMeta('connector_params');
				oButton.addClass('elsa-accent');
			}
			else if (sText == YAHOO.ELSA.Labels.index_analytics){
				YAHOO.ELSA.currentQuery.delMeta('livetail');
				YAHOO.ELSA.currentQuery.delMeta('archive');
				YAHOO.ELSA.currentQuery.addMeta('analytics', 1);
				YAHOO.ELSA.showAddConnectorDialog();
				oButton.addClass('elsa-accent');
			}
			else if (sText == YAHOO.ELSA.Labels.archive_analytics){
				YAHOO.ELSA.currentQuery.delMeta('livetail');
				YAHOO.ELSA.currentQuery.addMeta('archive', 1);
				YAHOO.ELSA.currentQuery.addMeta('analytics', 1);
				YAHOO.ELSA.showAddConnectorDialog();
				oButton.addClass('elsa-accent');
			}
			else if (sText == YAHOO.ELSA.Labels.livetail){
				YAHOO.ELSA.currentQuery.addMeta('livetail', 1);
				YAHOO.ELSA.currentQuery.delMeta('archive');
				YAHOO.ELSA.currentQuery.delMeta('analytics');
				YAHOO.ELSA.currentQuery.delMeta('connector');
				YAHOO.ELSA.currentQuery.delMeta('connector_params');
				oButton.addClass('elsa-accent');
			}
		}
		
		var aArchiveButtonMenuItems = [
			{
				text: 'Index',
				onclick: { fn:onArchiveSelectionClick, obj:YAHOO.ELSA.Labels.index }
			},
			{
				text: 'Archive',
				onclick: { fn:onArchiveSelectionClick, obj:YAHOO.ELSA.Labels.archive }
			},
			{
				text: 'Index Analytics (Map/Reduce)',
				onclick: { fn:onArchiveSelectionClick, obj:YAHOO.ELSA.Labels.index_analytics }
			},
			{
				text: 'Archive Analytics (Map/Reduce)',
				onclick: { fn:onArchiveSelectionClick, obj:YAHOO.ELSA.Labels.archive_analytics }
			}/*,
			{
				text: 'Live Tail',
				onclick: { fn:onArchiveSelectionClick, obj:YAHOO.ELSA.Labels.livetail }
			}*/ //Disabled until it can be made more reliable
		];
		
		var oArchiveButtonCfg = {
			id: sArchiveMenuButtonName,
			type: 'menu',
			label: 'Index',
			name: sArchiveMenuButtonName,
			value: 'archive_query',
			menu: aArchiveButtonMenuItems
		}
		
//		var oArchiveButtonCfg = {
//			id: 'archive_button',
//			type: 'checkbox',
//			label: 'Index',
//			name: 'archive_button',
//			value: 'archive_query',
//			checked: false,
//			onclick: {
//				fn: function(p_oEvent){
//					logger.log('arguments', arguments);
//					if (p_oEvent.target.innerHTML == 'Index'){
//						YAHOO.ELSA.currentQuery.addMeta('archive_query', 1);
//						p_oEvent.target.innerHTML = 'Archive';
//					}
//					else {
//						YAHOO.ELSA.currentQuery.delMeta('archive_query');
//						p_oEvent.target.innerHTML = 'Index';
//					}
//				}
//			}
//		}
		
		/* Draw form */
		
		var oQueryFormGridCfg = {
			form_attrs:{
				id: 'query_menu'
			}
		};
		oQueryFormGridCfg['grid'] = [
			[ 
				{type:'text', args:'Query'},
				{type:'input', args:{id:'q', name:'elsa_query_box', size:80} },
				{type:'widget', className:'Button', args:oSubmitButtonConfig},
				{type:'element', element:'a', args:{href:'http://code.google.com/p/enterprise-log-search-and-archive/wiki/Documentation#Queries', innerHTML:'Help', target:'_new'}}
			]
		];
		
		var oFormGridCfg = {
			form_attrs:{
				id: 'query_menu'
			}
		};
		
		var oStartDate = new Date((formParams.display_start_int) * 1000);
		var oSameTabCheckboxArgs = {id:'same_tab_checkbox', type:'checkbox'};
		var perUserSetting;
		
		if (YAHOO.ELSA.sameTabForQueries){
			var perUserSetting = YAHOO.ELSA.getPreference('reuse_tab', YAHOO.ELSA.DefaultSettingsType);
			logger.log('perUserSetting', perUserSetting);
			if (perUserSetting == null){
				oSameTabCheckboxArgs.checked = true;
			}
			else if (perUserSetting != 1){
				// Server config says yes, user says no
				oSameTabCheckboxArgs.checked = false;
			}
			else {
				oSameTabCheckboxArgs.checked = true;
			}
		}
		else if (YAHOO.ELSA.getPreference('reuse_tab', YAHOO.ELSA.DefaultSettingsType)){
			// server config says no, user says yes
			oSameTabCheckboxArgs.checked = true;
		} 
		
		var oGridCheckboxArgs = {id:'grid_display_checkbox', type:'checkbox'};
		if (YAHOO.ELSA.gridDisplay){
			oGridCheckboxArgs.checked = true;
		}
		else if (YAHOO.ELSA.getPreference('grid_display', YAHOO.ELSA.DefaultSettingsType)){
			oGridCheckboxArgs.checked = true;
		}
		
		var oUTCCheckboxArgs = {id:'use_utc', type:'checkbox'};
		if (YAHOO.ELSA.getPreference('use_utc', 'default_settings')){
			oUTCCheckboxArgs.checked = true;
		}
				
		oFormGridCfg['grid'] = [
			[ 
				{type:'element', 'element':'a', args:{'id':'start_time_link', 'name':'from_time', 'innerHTML':'From', 'href':'#'} }, 
				{type:'input', args:{id:'start_time', size:15, value:getISODateTime(oStartDate)}}, 
				{type:'element', 'element':'a', args:{'id':'end_time_link', 'name':'to_time', 'innerHTML':'To', 'href':'#'} },
				{type:'input', args:{id:'end_time', size:15}},
				{type:'input', args:oUTCCheckboxArgs},
				{type:'text', args:'UTC'},
				{type:'widget', className:'Button', args:oTermMenuButtonCfg},
				{type:'widget', className:'Button', args:oGroupByMenuButtonCfg},
				{type:'widget', className:'Button', args:oArchiveButtonCfg},
				{type:'input', args:oSameTabCheckboxArgs},
				{type:'text', args:'Reuse current tab'},
				{type:'input', args:oGridCheckboxArgs},
				{type:'text', args:'Grid display'}
			]
		];
		
		try {
			
			var oTargetForm = document.createElement('form');
			YAHOO.ELSA.queryForm = new YAHOO.ELSA.Form(oTargetForm, oQueryFormGridCfg);
			new YAHOO.ELSA.Form(oTargetForm, oFormGridCfg);
			
			YAHOO.util.Dom.get('query_form').appendChild(oTargetForm);
			
			var oStartCal = new YAHOO.ELSA.Calendar('start', formParams);
			YAHOO.util.Event.addListener('start_time_link', 'click', oStartCal.dialog.show, oStartCal.dialog, true);
			var oEndCal = new YAHOO.ELSA.Calendar('end', formParams);
			YAHOO.util.Event.addListener('end_time_link', 'click', oEndCal.dialog.show, oEndCal.dialog, true);
			
			YAHOO.util.Event.addListener('grid_display_checkbox', 'click', YAHOO.ELSA.toggleGridDisplay);
					
			// Tack on the tooltip for earliest start date
			var oStartToolTip = new YAHOO.widget.Tooltip('start_time_tool_tip', { context: 'start_time', text: 'Earliest: ' + formParams.start + ', Archive Earliest: ' + formParams.archive_start});
			var oStartToolTip = new YAHOO.widget.Tooltip('end_time_tool_tip', { context: 'end_time', text: 'Latest: ' + formParams.end + ', Archive Latest: ' + formParams.archive_end});
						
			/* Put the cursor in the main search field */
			if (YAHOO.util.Dom.get('q')){
				YAHOO.util.Dom.get('q').focus();	
			}
			else {
				YAHOO.ELSA.Error('Unable to find query input field');
				return;	
			}	
		}
		catch (e){
			YAHOO.ELSA.Error('Error drawing query grid:' + e.toString());
			return;	
		}
		
		var keylisteners = {};
		/* Have the enter key submit the form */
		keylisteners.enter = new YAHOO.util.KeyListener(
				YAHOO.util.Dom.get('query_menu'),
				{ keys: 13 },
				{ 	fn: function(eName, eObj){ var tgt=(eObj[1].target ? eObj[1].target : (eObj[1].srcElement ? eObj[1].srcElement : null)); try{tgt.blur();}catch(e){} submitQuery();},
					scope: YAHOO.ELSA,
					correctScope: false
				}
		);
		keylisteners.enter.enable();
		
		// Close all tabs with F8
		keylisteners.f8 = new YAHOO.util.KeyListener(
			window,
			{ keys: 119 },
			{ 	fn: function(p_sEventName, p_a){
				 	var iKey = p_a[0];
				 	var oEvent = p_a[1];
				 	oEvent.preventDefault();
					YAHOO.ELSA.closeTabs();
					YAHOO.util.Dom.get('q').focus();
				},
				scope: YAHOO.ELSA,
				correctScope:false 
			}
		);
		keylisteners.f8.enable();
		
		// Close tabs up to active with F9
		keylisteners.f9 = new YAHOO.util.KeyListener(
			window,
			{ keys: 120 },
			{ 	fn: function(p_sEventName, p_a){
				 	var iKey = p_a[0];
				 	var oEvent = p_a[1];
				 	oEvent.preventDefault();
					YAHOO.ELSA.closeTabsUntilCurrent();
					YAHOO.util.Dom.get('q').focus();
				},
				scope: YAHOO.ELSA,
				correctScope:false 
			}
		);
		keylisteners.f9.enable();
		
		// Close all other tabs with F10
		keylisteners.f10 = new YAHOO.util.KeyListener(
			window,
			{ keys: 121 },
			{ 	fn: function(p_sEventName, p_a){
				 	var iKey = p_a[0];
				 	var oEvent = p_a[1];
				 	oEvent.preventDefault();
					YAHOO.ELSA.closeOtherTabs();
					YAHOO.util.Dom.get('q').focus();
				},
				scope: YAHOO.ELSA,
				correctScope:false 
			}
		);
		keylisteners.f10.enable();
	}

	var drawMenuBar = function(){
		
		var aItemData = [
			{
				text: 'ELSA',
				submenu: {
					id: 'queries_menu',
					itemdata: [
						{
							text: 'Query Log',
							helptext: 'Queries this user has previously run',
							onclick: {
								fn: YAHOO.ELSA.getPreviousQueries
							}
						},
						{
							text: 'Saved Results',
							helptext: 'Results this user has manually saved',
							onclick: {
								fn: YAHOO.ELSA.getSavedResults
							}
						},
						{
							text: 'Alerts',
							helptext: 'Currently scheduled queries',
							onclick: {
								fn: YAHOO.ELSA.getQuerySchedule
							}
						},
						{
							text: 'Active Queries',
							helptext: 'View/cancel active queries',
							onclick: {
								fn: YAHOO.ELSA.getRunningArchiveQuery
							}
						},
						{
							text: 'Dashboards',
							helptext: 'View/edit dashboards',
							onclick: {
								fn: YAHOO.ELSA.getDashboards
							}
						},
						{
							text: 'Saved Searches',
							helptext: 'View/edit saved searches',
							onclick: {
								fn: YAHOO.ELSA.getSavedSearches
							}
						},
						{
							text: 'Preferences',
							helptext: 'View/edit preferences',
							onclick: {
								fn: YAHOO.ELSA.getPreferences
							}
						},
						{
							text: 'About',
							helptext: 'View ELSA version',
							onclick: {
								fn: YAHOO.ELSA.getVersion
							}
						}
					]
				}
			}
		];
		
		if (typeof YAHOO.ELSA.IsAdmin != 'undefined'){
			aItemData.push({
				text: 'Admin',
				submenu: {
					id: 'admin_menu',
					itemdata: [
						{
							text: 'Manage Permissions',
							helptext: 'Manage permissions for users',
							url: 'admin',
							target: '_blank'
						},
						{
							text: 'Stats',
							helptext: 'Query and load statistics',
							url: 'dashboard/_system',
							target: '_blank'
						},
						{
							text: 'Cancel All Livetails',
							helptext: 'Stop any running livetails from all users',
							url: 'Query/cancel_all_livetails',
							target: '_blank'
						},
						{
							text: 'View All Alerts',
							helptext: 'See alerts from all users',
							onclick: {
								fn: YAHOO.ELSA.getQueryScheduleAdmin
							}
						}
					]
				}
			});
		}
		
		var oMenuBar = new YAHOO.widget.MenuBar('menu_bar_content', {
			lazyload: false,
			itemdata: aItemData
		});
		oMenuBar.render('menu_bar');
		YAHOO.util.Dom.addClass(oMenuBar.element, "yuimenubarnav");
		// Fix z-index issues so that the menu is always on top
		var menuEl = new YAHOO.util.Element('queries_menu');
		menuEl.setStyle('z-index', 1000);
		
		// Add on totals
		formParams.totals_readable = {};
		for (var i in formParams.totals){
			var iIndexes = formParams.totals[i];
			var sUnit = '';
			var iDiv = 1;
			if (iIndexes > 1000000000){
				sUnit = 'billion';
				iDiv = 1000000000;
			}
			else if (iIndexes > 1000000){
				sUnit = 'million';
				iDiv = 1000000;
			}
			formParams.totals_readable[i] = Number(iIndexes / iDiv).toFixed(1) + ' ' + sUnit;
		}
		
		var aElItems = YAHOO.util.Dom.getElementsByClassName('yuimenubaritem-hassubmenu', 'li', 'menu_bar_content');
		var oElLi = document.createElement('li');
		oElDiv = document.createElement('div');
		oElDiv.innerHTML = 'Logs in Index: ' + formParams.totals_readable.indexes + ', Archive: ' 
			+ formParams.totals_readable.archive + ' on ' + formParams.nodes.length + ' nodes';
		oElDiv.innerHTML = '<b>' + formParams.nodes.length + '</b> node(s) with <b>' + formParams.totals_readable.indexes + '</b> logs indexed and <b>' 
			+ formParams.totals_readable.archive + '</b> archived';
		oElLi.appendChild(oElDiv); 
		oElLi.setAttribute('index', aElItems.length);
		oElLi.setAttribute('groupindex', 0);
		aElItems[aElItems.length - 1].parentNode.appendChild(oElLi);
		var oElLiEl = new YAHOO.util.Element(oElLi);
		oElLiEl.setStyle('text-align', 'right');
	}
	
	drawMenuBar();
	
	/* Get form params (goes all the way to a backend node) */
	drawQueryForm();
	
	/* Instantiate the tab view for our results */
	var setActiveQuery = function(p_oEvent){
		logger.log('set active query p_oEvent', p_oEvent);
		p_oTab = p_oEvent.newValue;
		
		var iTabIndex = YAHOO.ELSA.tabView.getTabIndex(p_oTab);
		if (typeof iTabIndex == 'undefined'){
			logger.log('unable to find tabindex for tab:', p_oTab);
			return;
		}
		
		// find the result that has this tabid
		var iLocalResultId = YAHOO.ELSA.getLocalResultId(p_oTab);
		var oQuery;
		if (iLocalResultId){
			try {
				//logger.log('localResults start: ' + YAHOO.ELSA.localResults[iLocalResultId].query.metas.start);
				logger.log('sentquery start: ' + YAHOO.ELSA.localResults[iLocalResultId].sentQuery);
				logger.log('local result: ', YAHOO.ELSA.localResults[iLocalResultId]);
//				if (typeof(YAHOO.ELSA.localResults[iLocalResultId].queryString) != 'undefined' &&
//					typeof(YAHOO.ELSA.localResults[iLocalResultId].query) != 'undefined' &&
//					typeof(YAHOO.ELSA.localResults[iLocalResultId].query.metas) != 'undefined'){
//					oQuery = { 
//						'query_string': YAHOO.ELSA.localResults[iLocalResultId].queryString,
//						'query_meta_params': YAHOO.ELSA.localResults[iLocalResultId].query.metas
//					};
//				}
//				else {
					logger.log('parsing ' + YAHOO.ELSA.localResults[iLocalResultId].sentQuery);
					oQuery = YAHOO.lang.JSON.parse(YAHOO.ELSA.localResults[iLocalResultId].sentQuery);
					//oQuery = YAHOO.ELSA.localResults[iLocalResultId].results;
//				}
			}
			catch (e){
				logger.log('error getting query for results:', e);
				logger.log('results:', YAHOO.ELSA.localResults[iLocalResultId]);
				return;
			}
			YAHOO.ELSA.currentQuery.queryString = oQuery.query_string;
			logger.log('set active query: ', oQuery);
			// set the q bar
			YAHOO.util.Dom.get('q').value = oQuery.query_string;
			
			//set the groupby button
			var oGroupButton = YAHOO.widget.Button.getButton(sGroupByMenuSelectButtonName);
			var oArchiveButton = YAHOO.widget.Button.getButton(sArchiveMenuButtonName);
			if (oQuery.query_meta_params){
				YAHOO.ELSA.currentQuery.metas = oQuery.query_meta_params;
				if (typeof(YAHOO.ELSA.currentQuery.metas.groupby) == 'undefined'){
					// groupby could've been set in query text instead of data struct
					if (typeof(YAHOO.ELSA.localResults[iLocalResultId].results.groupby) != 'undefined'){
						logger.log('setting groupby from results: ', YAHOO.ELSA.localResults[iLocalResultId].results.groupby);
						YAHOO.ELSA.currentQuery.metas.groupby = YAHOO.ELSA.localResults[iLocalResultId].results.groupby;
					}
				}
				logger.log('current query: ' + YAHOO.lang.JSON.stringify(YAHOO.ELSA.currentQuery));
				logger.log('type of class: ' + typeof YAHOO.ELSA.currentQuery.metas['class']);
				logger.log('current groupby:', YAHOO.ELSA.currentQuery.metas.groupby);
				logger.log('typeof current groupby:', typeof(YAHOO.ELSA.currentQuery.metas.groupby));
				if (typeof(YAHOO.ELSA.currentQuery.metas.groupby) != 'undefined' &&
					YAHOO.ELSA.currentQuery.metas.groupby.length){
//					if (typeof YAHOO.ELSA.currentQuery.metas['class'] != 'undefined'){
//						oGroupButton.set('label', YAHOO.ELSA.currentQuery.metas['class'] + '.' + YAHOO.ELSA.currentQuery.metas.groupby);
//					}
//					else {
//						oGroupButton.set('label', 'any.' + YAHOO.ELSA.currentQuery.metas.groupby[0]);
//					}
					// Don't set the button if the query string has the groupby in it
					var aMatches = YAHOO.ELSA.currentQuery.queryString.match(/\s*groupby[:=]([\w\.]+)\s*/, 'i');
					//if (aMatches != null && aMatches[1] == YAHOO.ELSA.currentQuery.metas.groupby[0]){
					if (aMatches != null){
						logger.log('groupby set via queryString');
						// Clear metas.groupby[0] so that this doesn't get sent twice as it's represented in the queryString
						YAHOO.ELSA.currentQuery.metas.groupby.splice(0,1);
						oGroupButton.set('label', aMatches[1]);
					}
					else {
						oGroupButton.set('label', YAHOO.ELSA.currentQuery.metas.groupby[0]);
					}
					oGroupButton.addClass('elsa-accent');
				}
				else {
					oGroupButton.set('label', YAHOO.ELSA.Labels.defaultGroupBy);
					oGroupButton.removeClass('elsa-accent');
				}
				// set times
				if (YAHOO.ELSA.currentQuery.metas.start){
					YAHOO.util.Dom.get('start_time').value = getISODateTime(new Date(YAHOO.ELSA.currentQuery.metas.start * 1000));
				}
				else {
					YAHOO.util.Dom.get('start_time').value = '';
				}
				if (YAHOO.ELSA.currentQuery.metas.end){
					YAHOO.util.Dom.get('end_time').value = getISODateTime(new Date(YAHOO.ELSA.currentQuery.metas.end * 1000));
				}
				else {
					YAHOO.util.Dom.get('end_time').value = '';
				}
				
				//set the archive button
				if (typeof(YAHOO.ELSA.currentQuery.metas.archive) == 'undefined'){
					// groupby could've been set in query text instead of data struct
					if (typeof(YAHOO.ELSA.localResults[iLocalResultId].results.query_meta_params) != 'undefined' &&
						typeof(YAHOO.ELSA.localResults[iLocalResultId].results.query_meta_params.archive) != 'undefined'){
						YAHOO.ELSA.currentQuery.metas.archive = YAHOO.ELSA.localResults[iLocalResultId].results.query_meta_params.archive;
					}
				}
				logger.log('current query: ' + YAHOO.lang.JSON.stringify(YAHOO.ELSA.currentQuery));
				logger.log('type of class: ' + typeof YAHOO.ELSA.currentQuery.metas['class']);
				logger.log('current archive:', YAHOO.ELSA.currentQuery.metas.archive);
				if (YAHOO.ELSA.currentQuery.metas.archive){
					oArchiveButton.set('label', YAHOO.ELSA.Labels.archive);
					oArchiveButton.set('checked', true);
					oArchiveButton.addClass('elsa-accent');
				}
				else {
					oArchiveButton.set('label', YAHOO.ELSA.Labels.index);
					oArchiveButton.set('checked', false);
					oArchiveButton.removeClass('elsa-accent');
				}
			}
			else {
				logger.log('no metas found from oQuery');
				oGroupButton.set('label', YAHOO.ELSA.Labels.defaultGroupBy);
				oArchiveButton.set('label', YAHOO.ELSA.Labels.index);
				oArchiveButton.set('checked', false);
			}
		}
		else {
			logger.log('iLocalResultId was undefined');
		}
	}
	var oTabViewDiv = YAHOO.util.Dom.get('tabView');
	YAHOO.util.Dom.addClass(oTabViewDiv, 'hiddenElement');
	YAHOO.ELSA.tabView = new YAHOO.widget.TabView(oTabViewDiv);
	YAHOO.ELSA.tabView.subscribe('activeTabChange', setActiveQuery);
	
	YAHOO.util.Event.addListener('query_submit', 'click', submitQuery);
	
	// Check for query_string given in URI
    oRegExp = new RegExp('\\Wquery_string=([^&]+)');
    oMatches = oRegExp.exec(location.search);
    if (oMatches){
            var oGivenQueryString = decodeURIComponent(oMatches[1]);
            
            // Detect start/end times provided in query_string URI param and fill out dom value to preserve across delMeta during submission
            var oStartRegExp = new RegExp('\\Wstart[:=][\'"]([^[\'"]+)');
            oMatches = oStartRegExp.exec(oGivenQueryString);
            if (oMatches){
            	YAHOO.util.Dom.get('start_time').value = oMatches[1];
            }
            var oEndRegExp = new RegExp('\\Wend[:=][\'"]([^[\'"]+)');
            oMatches = oEndRegExp.exec(oGivenQueryString);
            if (oMatches){
            	YAHOO.util.Dom.get('end_time').value = oMatches[1];
            }
            
            YAHOO.util.Dom.get('q').value = oGivenQueryString;
            YAHOO.ELSA.currentQuery.queryString = oGivenQueryString;
            submitQuery();
    }

};

