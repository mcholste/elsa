var viz_map = {
  sankey: build_sankey
};

var TRANSCRIPT = [];
var RESULT_HISTORY = [];

function AnalysisTree(){
  var self = this;
  self.tree = {
    name: '',
    data: {},
    children: [],
    parent: null
  };
  self.last = self.tree;
}
AnalysisTree.prototype.propagate = function(scope, data, branch){
  var self = this;
  var node = {
    name: scope,
    data: data,
    children: []
  };
  if (branch){
    // get parent
    console.log('last', self.last);
    if (self.last.parent){
      var parent = self.last.parent;
      node.parent = parent;
      parent.children.push(node);
    }
    else {
      node.parent = self.tree;
      self.tree.children.push(node);
    }
  }
  else {
    // Link to previous
    node.parent = self.last;
    self.last.children.push(node);
  }
  self.last = node;
  console.log(self.tree);
};

AnalysisTree.prototype.visualize = function(dom_element){
  var self = this;
  function clean_data(data){
    delete data.data;
    delete data.parent;
    for (var i = 0, len = data.children.length; i < len; i++){
      clean_data(data.children[i]);
    }
    return data;
  }
  var data = clean_data(_.cloneDeep(self.tree));
  
  console.log('clean data', data);
  $(dom_element).empty();
  var margin = {top: 20, right: 190, bottom: 30, left: 190},
  width = 660 - margin.left - margin.right,
  height = 500 - margin.top - margin.bottom;

  // declares a tree layout and assigns the size
  var treemap = d3.tree()
  .size([height, width]);

  //  assigns the data to a hierarchy using parent-child relationships
  var nodes = d3.hierarchy(data, function(d) {
    return d.children;
    });

  // maps the node data to the tree layout
  nodes = treemap(nodes);

  // append the svg object to the body of the page
  // appends a 'group' element to 'svg'
  // moves the 'group' element to the top left margin
  var svg = d3.select(dom_element).append("svg")
      .attr("width", width + margin.left + margin.right)
      .attr("height", height + margin.top + margin.bottom),
    g = svg.append("g")
      .attr("transform",
        "translate(" + margin.left + "," + margin.top + ")");

  // adds the links between the nodes
  var link = g.selectAll(".link")
    .data( nodes.descendants().slice(1))
    .enter().append("path")
    .attr("class", "link")
    .attr("d", function(d) {
       return "M" + d.y + "," + d.x
       + "C" + (d.y + d.parent.y) / 2 + "," + d.x
       + " " + (d.y + d.parent.y) / 2 + "," + d.parent.x
       + " " + d.parent.y + "," + d.parent.x;
       });

  // adds each node as a group
  var node = g.selectAll(".node")
    .data(nodes.descendants())
    .enter().append("g")
    .attr("class", function(d) { 
      return "node" + 
      (d.children ? " node--internal" : " node--leaf"); })
    .attr("transform", function(d) { 
      return "translate(" + d.y + "," + d.x + ")"; });

  // adds the circle to the node
  node.append("circle")
    .attr("r", 2.5);

  var last_pos = false;
  // adds the text to the node
  node.append("text")
    .attr("dy", 3)
    .attr("y", function(d) { 
      if (last_pos){ last_pos = false; return 13 } 
      else { last_pos = true; return -13 } })
    .attr("x", function(d) { return d.children ? -13 : 13; })
    .style("text-anchor", function(d) { 
      return d.children ? "end" : "start"; })
    .text(function(d) { return d.data.name; });

}

function Transcript(callbacks){
  var self = this;
  self.result_history = [];
  self.transcript = [];
  self.callbacks = callbacks;
  // Load initial values from ELSA
  $.get('transcript', null, function(data, status, xhr){
    //data = JSON.parse(data);

    console.log('data', data);
    for (var i = data.length - 1; i >= 0; i--){
      self.transcript.push(data.pop());
    }
    self.render();
  }, 'json');
}

Transcript.prototype.counter = function(){
  return this.result_history.length;
}

Transcript.prototype.log_query = function (data){
  this.result_history.push(data);
};

Transcript.prototype.update = function(action, data){
  var self = this;
  // Searches are automatically added to the transcript server-side
  if (action === 'SEARCH'){
    console.log('search update data', data);
    self.transcript.push({id: data.transcript_id, action:action, scope:data.scope, ref_id:data.id});
    self.render();
  }
  else {
    console.log('put transcript', data);
    //var put_data = { action:action, scope:data.scope };
    data.action = action;
    if (typeof(data.id) !== 'undefined') data.ref_id = data.id;
    // Write to the server
    $.ajax('transcript', {
      method: 'PUT',
      data: data, 
      success: function(data, status, xhr){
        console.log(data, status);
        self.transcript.push(data);
        self.render();  
      }
    }).fail(function(e){
      console.error(e);
      var errstr = 'Unable to update transcript';
      console.error(errstr);
      self.callbacks.error(errstr);
    });
  }
  
  return action + ' ' + data.scope;
}

// Transcript.prototype.update_checked = function(ref_id){
//   var self = this;
//   for (var i = 0, len = self.transcript.length; i < len; i++){
//     var item = self.transcript[i];
//     if 
// }

Transcript.prototype.get_id = function(id){
  var self = this;
  for (var i = 0, len = self.transcript.length; i < len; i++){
    if (self.transcript[i].id === id) return i;
  }
  throw Error('Id ' + id + ' not found.');
};

Transcript.prototype.latest_search = function(){
  var self = this;
  for (var i = self.transcript.length - 1; i >= 0; i--){
    if (self.transcript[i].action === 'SEARCH') return self.transcript[i];
  }
}

// Not a standard method, this is expected to be called with a .bind([self,item])
Transcript.prototype.load_item = function(e){
  console.log('load_item this', this);
  if (!this.length === 2) throw Error('load_item called without bind([self,item])');
  var self = this[0];
  var item = this[1];
  if (e && e.hasOwnProperty('preventDefault')) e.preventDefault();
  self.selected = item.ref_id;
  $.get('results/' + item.ref_id, null, 
    function(data, status, xhr){ render_search_result(data, status); self.render(); }, 'json')
  .fail(function(e){
    console.error(e);
    var errstr = 'Unable to get result';  
    console.error(errstr);
    self.callbacks.error(errstr);
  });
};

Transcript.prototype.render = function(){
  var self = this;
  $('#transcript_container').empty();
  $('#transcript_container').addClass('respect-whitespace');
  var h1 = document.createElement('h1');
  $(h1).addClass('overwatch');
  h1.innerText = 'Transcript';
  $('#transcript_container').append(h1);
  var table = document.createElement('table');
  var tbody = document.createElement('tbody');
  // var indent_level = 0;
  for (var i = 0, len = self.transcript.length; i < len; i++){
    var item = self.transcript[i];
    if (item.visible === 0) continue;
    // if (item.action === 'PIVOT') indent_level++;
    var row = document.createElement('tr');
    var cell = document.createElement('td');
    $(cell).attr('data_field', 'transcript_id');
    $(cell).attr('data_value', item.id);
    // var tabs = '';
    // for (var j = 0, jlen = indent_level; j < jlen; j++){
    //   tabs += '    ';
    // }
    
    var text = document.createTextNode(item.action + ' ' + item.scope);
    if (item.action === 'PIVOT'){
      // Indent and print ref_id
      var a = document.createElement('a');
      $(a).text(item.ref_id);
      console.log('pivot item', item);
      a.title  = self.transcript[ self.get_id(item.ref_id) ].scope;
      $(a).click(self.load_item.bind([self, item]));
      $(a).addClass('pivot_reference');
      var span = document.createElement('span');
      span.appendChild(document.createTextNode('\t'));
      span.appendChild(a);
      span.appendChild(document.createTextNode(' '));
      span.appendChild(document.createTextNode(item.scope));
      cell.appendChild(span);
    }
    // var text = document.createTextNode(tabs + item.action + ' ' + item.scope);
    // Create link if this is a search
    else if (item.action === 'SEARCH'){
      console.log('rendering search');
      var a = document.createElement('a');
      a.appendChild(text)
      $(a).click(self.load_item.bind([self, item]));
      var span = document.createElement('i');
      $(span).addClass('fa fa-close fa-fw');
      $(span).click(function(e){
        var item = this; // use bound scope for item
        console.log(item);
        console.log('hiding transcript id ' + item.id);
        $.post('transcript', {action:'HIDE', id:item.id}, function(e){
          var item = this;
          item.visible = 0;
          self.render();
          self.callbacks.notify('Transcript ' + item.id + ' hidden');
        }.bind(item)).fail(function(e){
          console.error(e);
          var errstr = 'Unable to set visibility';
          console.error(errstr);
          self.callbacks.error(errstr);
        });

      }.bind(item));
      // console.log('selected: ' + self.selected + ', item ref: ' + item.ref_id);
      // if (self.selected === item.ref_id)
      //   $(span).addClass('fa fa-check-square-o fa-fw');
      // else
      //   $(span).addClass('fa fa-square-o fa-fw');
      cell.appendChild(span);
      cell.appendChild(a);
    }
    else {
      cell.appendChild(text);
    }
    
    //cell.appendChild(text);  
    
    row.appendChild(cell);
    tbody.appendChild(row);
  }
  table.appendChild(tbody);
  $('#transcript_container').append(table);
};

var ANALYSIS_TREE = new AnalysisTree();
var TRANSCRIPT = new Transcript({
  error: function(s){ set_current_action('ERROR: ' + s) },
  notify: function(s){ notify(s); }
});
var TAGS = {};
var FAVORITES = {};

$( document ).ajaxStart(function() {
  $('#modal').empty();
  var icon = document.createElement('span');
  $('#modal_outer').removeClass('background').addClass('foreground');
  $(icon).addClass('fa fa-cloud fa-fw');
  $('#modal').append(icon);
});

$( document ).ajaxComplete(function() {
  $('#modal').empty();
  $('#modal_outer').removeClass('foreground').addClass('background');
});

$(document).on('ready', function(){
  $.get('tags', null, function(data, status, xhr){
    console.log('tags', data);
    for (var i = 0, len = data.length; i < len; i++){
      if (typeof(TAGS[ data[i].tag ]) === 'undefined'){
        TAGS[ data[i].tag ] = {};
      }
      TAGS[ data[i].tag ][ data[i].value ] = 1;
    }
    update_tags();
  }, 'json');
  $.get('favorites', null, function(data, status, xhr){
    console.log('favorites', data);
    for (var i = 0, len = data.length; i < len; i++){
      FAVORITES[ data[i].value ] = 1;
    }
    update_favorites();
  }, 'json');
  TRANSCRIPT.render();
  //$('#sidebar').height($(window).height());
  //$('#transcript_container').height($(window).height());
  $('#start_date').val('2016-09-06T14:46:00');
  $('#end_date').val('2016-09-06T14:47:00');
  //$('#start_date').datepicker();
  //$('#end_date').datepicker();
  $('#search_form input[name="query"]').val('_exists_:proto | groupby srcip,name,dstip | sankey');
  $('#query_submit').on('click', submit_form);
});

function clean_record(item){
  for (var k in item){
    if (typeof(item[k]) === 'object'){
      for (var j in item[k]){
        if (typeof(item[k][j]) === 'object'){
          var subrecord = clean_record(item[k][j]);
          for (var l in subrecord){
            item[k + '.' + j + '.' + l] = subrecord[l];
          }
        }
        else {
          item[k + '.' + j] = item[k][j];
        }
      }
      delete item[k];
    }
  }
  
  return item;
}

function render_search_result(data, status, xhr){
  console.log(data, xhr, status);
  if (typeof(xhr) !== 'undefined'){
    TRANSCRIPT.log_query(data);
    var action = TRANSCRIPT.update('SEARCH', data);
    ANALYSIS_TREE.propagate(action);
  }
  $('#search_form input[name="search"]').val(data.raw_query);
  //build_histogram(data);
  if (typeof(data.results.aggregations) !== 'undefined' &&
    typeof(data.results.aggregations.date_histogram) !== 'undefined')
    build_c3_multi_histogram(data);
    //build_c3_histogram(data);
  else if (typeof(data.results.aggregations) !== 'undefined') 
    build_c3_bar_chart(data);
  // Draw grid of results
  // var grid_el = document.createElement('div');
  // grid_el.id = 'grid';
  // $('body').append(grid_el);
  var raw_data = [];
  for (var i = 0, len = data.results.hits.hits.length; i < len; i++){
    raw_data.push(clean_record(data.results.hits.hits[i]._source));
  }
  
  $('#grid_container').empty();
  $('#grid_container').append(get_table(raw_data, raw_data));

  if (typeof(data.query.viz) !== 'undefined'){
    $('#viz_container').height(500);
    console.log(data.query.viz);
    for (var i = 0, len = data.query.viz.length; i < len; i++){
      var viz = data.query.viz[i][0];
      for (var k in data.results.aggregations){
        if (k === 'date_histogram') continue;
        var graph = build_graph_from_hits(data.results.aggregations[k].buckets);
        viz_map[viz](graph);
      }
    }
  }
}

function submit_form(e){
  if (e) e.preventDefault();

  //if (transcript.length) $('#transcript_container').style('height:500px;');

  var query = $('#search_form input[name="query"]').val();
  var start_date = moment($('#start_date').val()).unix();
  var end_date = moment($('#end_date').val()).unix();
  console.log('query: ' + query);
  var query_string = 'http://localhost:8080/search?q=' + query;
  if (start_date) query_string += '&start=' + start_date;
  if (end_date) query_string += '&end=' + end_date;

  $.get(query_string, render_search_result);
}


function key_as_string(datum){
  if (typeof(datum.key_as_string) !== 'undefined') return datum.key_as_string;
  return datum.key;
}

function build_c3_bar_chart(data){
  for (var k in data.results.aggregations){
    var new_el = document.createElement('div');
    new_el.id = 'histogram_' + k;
    $('#viz_container').append(new_el);
    var columns = [];
    for (var i = 0, len = data.results.aggregations[k].buckets.length; i < len; i++){
      console.log('bucket', data.results.aggregations[k].buckets[i]);

      var col = [ 
        key_as_string(data.results.aggregations[k].buckets[i]),
        data.results.aggregations[k].buckets[i].doc_count
      ];
      columns.push(col);
    }
    console.log('columns', columns);
    var chart = c3.generate({
      bindto: new_el,
      data: {
        columns: columns,
        type: 'bar'
      }
    });
    console.log('chart', chart);
  }
}

function build_bar_chart(result_data){
  if (typeof(result_data.results.aggregations) === 'undefined') return;
  console.log('building bar chart with ', data);
  var data = [];
  for (var i = 0, len = result_data.results.aggregations.date_histogram.buckets.length; i < len; i++){
    var item = result_data.results.aggregations.date_histogram.buckets[i];
    console.log('bucket', item);
    data.push({
      date: new Date(item.key),
      count: item.doc_count
    });
  }
  // set the dimensions and margins of the graph
  var margin = {top: 10, right: 30, bottom: 30, left: 40},
      width = 960 - margin.left - margin.right,
      height = 500 - margin.top - margin.bottom;

  // parse the date / time
  var parseDate = d3.timeParse("%Y-%d-%mT%H:%M:%SZ");

  // set the ranges
  var min_time = new Date(d3.min(data, function(d){ return new Date(d.date).getTime()}));
  var max_time = new Date(d3.max(data, function(d){ return new Date(d.date).getTime()}));
  var x = d3.scaleTime()
    .domain([min_time, max_time])
    .rangeRound([0, width]);
  var y = d3.scaleLinear()
    .range([height, 0]);

  // set the parameters for the histogram
  var tick_unit = d3.timeSecond;
  var time_range = (max_time - min_time)/1000;
  var min_ticks = 100;
  if (time_range / 86400 * 30 > min_ticks) tick_unit = d3.timeMonth;
  else if (time_range / 86400 > min_ticks) tick_unit = d3.timeDay;
  else if (time_range / 3600 > min_ticks) tick_unit = d3.timeHour;
  else if (time_range / 60 > min_ticks) tick_unit = d3.timeMinute;
  var histogram = d3.histogram()
      .value(function(d) { return d.date; })
      .domain(x.domain())
      .thresholds(x.ticks(tick_unit));

  // append the svg object to the body of the page
  // append a 'group' element to 'svg'
  // moves the 'group' element to the top left margin
  var svg = d3.select("#histogram_container").append("svg")
    .attr("width", width)// + margin.left + margin.right)
    .attr("height", height)// + margin.top + margin.bottom)
  .append("g")
    .attr("transform", 
          "translate(" + margin.left + "," + margin.top + ")");

  // format the data
  // data.forEach(function(d) {
  //   d.date = parseDate(d.date);
  // });

  // group the data for the bars
  var bins = histogram(data);

  // Scale the range of the data in the y domain
  y.domain([0, d3.max(data, function(d) { return d.count; })]);

  // append the bar rectangles to the svg element
  svg.selectAll("rect")
      .data(data)
    .enter().append("rect")
      .attr("class", "bar")
      .attr("x", 1)
      .attr("transform", function(d) {
        return "translate(" + x(d.date) + "," + y(d.count) + ")"; })
      .attr("width", function(d) { return x(d.date); })
      .attr("height", function(d) { return height - y(d.count); });

  // add the x Axis
  svg.append("g")
      .attr("transform", "translate(0," + height + ")")
      .call(d3.axisBottom(x));

  // add the y Axis
  svg.append("g")
      .call(d3.axisLeft(y));

}

function build_line_chart(result_data){
  $('#histogram_container').empty();
  if (typeof(result_data.results.aggregations) === 'undefined' ||
    typeof(result_data.results.aggregations.date_histogram) === 'undefined') return;
  console.log('building histo with ', data);
  var data = [];
  for (var i = 0, len = result_data.results.aggregations.date_histogram.buckets.length; i < len; i++){
    var item = result_data.results.aggregations.date_histogram.buckets[i];
    console.log('bucket', item);
    data.push({
      date: new Date(item.key),
      count: item.doc_count
    });
  }
  // set the dimensions and margins of the graph
  var margin = {top: 10, right: 30, bottom: 30, left: 40},
      width = $('#histogram_container').width() - margin.left - margin.right,
      height = 300 - margin.top - margin.bottom;

  // parse the date / time
  var parseDate = d3.timeParse("%Y-%d-%mT%H:%M:%SZ");

  // set the ranges
  var min_time = new Date(d3.min(data, function(d){ return new Date(d.date).getTime()}));
  var max_time = new Date(d3.max(data, function(d){ return new Date(d.date).getTime()}));
  var x = d3.scaleTime()
    .domain([min_time, max_time])
    .rangeRound([0, width]);
  var y = d3.scaleLinear()
    .range([height, 0]);

  var line = d3.line()
    .x(function(d) { return x(d.date); })
    .y(function(d) { return y(d.count); });

  x.domain(d3.extent(data, function(d) { return d.date; }));
  y.domain(d3.extent(data, function(d) { return d.count; }));

  var svg = d3.select("#histogram_container").append("svg")
    .attr('class', 'linechart')
    .attr("width", width + margin.left + margin.right)
    .attr("height", height + margin.top + margin.bottom)
  .append("g")
    .attr("transform", 
          "translate(" + margin.left + "," + margin.top + ")");
  

  svg.append("g")
      .attr("class", "axis axis--x")
      .attr("transform", "translate(0," + height + ")")
      .call(d3.axisBottom(x));

  svg.append("g")
      .attr("class", "axis axis--y")
      .call(d3.axisLeft(y))
    .append("text")
      .attr("class", "axis-title")
      .attr("transform", "rotate(-90)")
      .attr("y", 6)
      .attr("dy", ".71em")
      .style("text-anchor", "end")
      .text("Price ($)");

  svg.append("path")
      .datum(data)
      .attr("class", "line")
      .attr("d", line);

}

function build_histogram(result_data){
  if (typeof(result_data.results.aggregations) === 'undefined' ||
    typeof(result_data.results.aggregations.date_histogram) === 'undefined') return;
  console.log('building histo with ', data);
  var data = [];
  for (var i = 0, len = result_data.results.aggregations.date_histogram.buckets.length; i < len; i++){
    var item = result_data.results.aggregations.date_histogram.buckets[i];
    console.log('bucket', item);
    data.push({
      date: new Date(item.key),
      count: item.doc_count
    });
  }
  // set the dimensions and margins of the graph
  var margin = {top: 10, right: 30, bottom: 30, left: 40},
      width = 960 - margin.left - margin.right,
      height = 500 - margin.top - margin.bottom;
  console.log('histo width ' + width);

  // parse the date / time
  var parseDate = d3.timeParse("%Y-%d-%mT%H:%M:%SZ");

  // set the ranges
  var min_time = new Date(d3.min(data, function(d){ return new Date(d.date).getTime()}));
  var max_time = new Date(d3.max(data, function(d){ return new Date(d.date).getTime()}));
  var x = d3.scaleTime()
    .domain([min_time, max_time])
    .rangeRound([0, width]);
  var y = d3.scaleLinear()
    .range([height, 0]);

  // set the parameters for the histogram
  var tick_unit = d3.timeSecond;
  var time_range = (max_time - min_time)/1000;
  var min_ticks = 100;
  if (time_range / 86400 * 30 > min_ticks) tick_unit = d3.timeMonth;
  else if (time_range / 86400 > min_ticks) tick_unit = d3.timeDay;
  else if (time_range / 3600 > min_ticks) tick_unit = d3.timeHour;
  else if (time_range / 60 > min_ticks) tick_unit = d3.timeMinute;
  var histogram = d3.histogram()
      .value(function(d) { return d.date; })
      .domain(x.domain())
      .thresholds(x.ticks(tick_unit));

  // append the svg object to the body of the page
  // append a 'group' element to 'svg'
  // moves the 'group' element to the top left margin
  var svg = d3.select("#histogram_container").append("svg")
    .attr('width', '100%')
    //.attr("width", width + margin.left + margin.right)
    .attr("height", height + margin.top + margin.bottom)
  .append("g")
    .attr("transform", 
          "translate(" + margin.left + "," + margin.top + ")");

  // format the data
  // data.forEach(function(d) {
  //   d.date = parseDate(d.date);
  // });

  // group the data for the bars
  var bins = histogram(data);

  // Scale the range of the data in the y domain
  y.domain([0, d3.max(data, function(d) { return d.count; })]);

  // append the bar rectangles to the svg element
  svg.selectAll("rect")
      .data(data)
    .enter().append("rect")
      .attr("class", "bar")
      .attr("x", 1)
      .attr("transform", function(d) {
        return "translate(" + x(d.date) + "," + y(d.count) + ")"; })
      .attr("width", function(d) { return x(d.date); })
      .attr("height", function(d) { return height - y(d.count); });

  // add the x Axis
  svg.append("g")
      .attr("transform", "translate(0," + height + ")")
      .call(d3.axisBottom(x));

  // add the y Axis
  svg.append("g")
      .call(d3.axisLeft(y));

}

function build_c3_multi_histogram(data){
  $('#histogram_container').empty();
  if (typeof(data.results.aggregations) === 'undefined' ||
    typeof(data.results.aggregations.date_histogram) === 'undefined' ||
    data.results.aggregations.date_histogram.buckets.length === 0) return;
  console.log('building histo with ', data);
  
  
  var div = document.createElement('div');
  div.id = 'date_histogram';
  //$(div).width('100%');
  $(div).width($('#histogram_container').width() - 10);
  
  $('#histogram_container').append(div);
  var json_data = [];
  var fields = {
    host: {}
  };
  fields['class'] = {};
  for (var i = 0, len = data.results.aggregations.date_histogram.buckets.length; i < len; i++){
    var item = data.results.aggregations.date_histogram.buckets[i];
    var to_push = {
      date: item.key
    };
  
    ['host', 'class'].forEach(function(field){
      for (var j = 0, jlen = item[field].buckets.length; j < jlen; j++){
        var subitem = item[field].buckets[j];
        //if (!subitem.doc_count) continue;
        fields[field][field + '.' + subitem.key] = 'spline';
        to_push[field + '.' + subitem.key] = subitem.doc_count;
      }
    });
    
    if (Object.keys(to_push).length < 2) continue;
    //console.log('to_push', to_push);
    json_data.push(to_push);
  }

  var combined_fields = [];
  for (var field in fields){
    for (var subfield in fields[field]){
      combined_fields.push(subfield);
    }
  }

  var chart = c3.generate({
    bindto: div,
    data: {
      json: json_data,
      keys: {
        x: 'date',
        value: combined_fields
      },
      xFormat: '%Y-%m-%dT%H:%M:%S.%LZ',
      type: 'bar',
      types: fields['host'],
      groups: [
        Object.keys(fields['host']),
        Object.keys(fields['class'])
      ]
    },
    axis: {
      x: {
        type: 'timeseries',
        tick: {
          format: '%Y-%m-%dT%H:%M:%S.%LZ'
        }
      }
    }
  });
}

function build_c3_histogram(data){
  if (typeof(data.results.aggregations) === 'undefined' ||
    typeof(data.results.aggregations.date_histogram) === 'undefined') return;
  console.log('building histo with ', data);
  
  
  var div = document.createElement('div');
  div.id = 'date_histogram';
  $(div).width('100%');
  $('#histogram_container').empty();
  $('#histogram_container').append(div);
  var x = ['x'], y = ['count'];
  for (var i = 0, len = data.results.aggregations.date_histogram.buckets.length; i < len; i++){
    var item = data.results.aggregations.date_histogram.buckets[i];
    console.log('bucket', item);
    x.push(new Date(item.key));
    y.push(item.doc_count);
  }
  console.log('x', JSON.stringify(x), 'y', JSON.stringify(y));
  var chart = c3.generate({
    bindto: div,
    data: {
      x: 'x',
      xFormat: '%Y-%m-%dT%H:%M:%S.%LZ',
      columns: [x, y]
    },
    axis: {
      x: {
        type: 'timeseries',
        tick: {
          format: '%Y-%m-%dT%H:%M:%S.%LZ'
        }
      }
    }
  });
}

function set_current_action(action){
  console.log('action: ' + action);
  $('#action_container').text(action);
}

function notify(s){
  console.log('notify: ' + s);
  $('#notification_container').text(s);
}

function get_table(data, full_data, onclicks, onhovers, reorder, sortby, sortdir, filter_field, filter_text){
  console.log('get_table', onclicks, onhovers, reorder, sortby, sortdir);
  if (typeof(onclicks) === 'undefined'){
    onclicks = {};
  }
  if (typeof(onhovers) === 'undefined'){
    onhovers = {};
  }
  if (typeof(reorder) === 'undefined'){
    reorder = true;
  }
  
  if (typeof(sortdir) === 'undefined') sortdir = 'asc';

  // Loop once to get all cols
  var cols = Array();
  for (var i = 0, len = full_data.length; i < len; i++){
    for (var j in full_data[i]){
      if (_.indexOf(cols, j) < 0){
        if (j === 'timestamp' || j === 'meta_ts'){
          cols.unshift(j);
        }
        else {
          cols.push(j);
        }
      }
    }
  }
  console.log('cols', cols.join(','));

  console.log('reorder', reorder);
  if (reorder){
    console.log('reordering');
    var preferredCols = ['@timestamp', 'class', 'program', 'srcip', 'srcport', 'dstip', 'dstport', 'rawmsg'];

    var ret = [];
    var others = [];
    for (var i = 0, len = cols.length; i < len; i++){
      var preferredPosition = _.indexOf(preferredCols, cols[i]);
      if (preferredPosition > -1){
        ret[preferredPosition] = preferredCols[preferredPosition];
        console.log('spliced ' + preferredCols[preferredPosition] + ' to ' + preferredPosition);
      }
      else {
        others.push(cols[i]);
      }
    }
    ret.push.apply(ret, others.sort());
    ret = _.filter(ret, function(item){ return typeof(item) !== 'undefined'; });
    cols = ret;
  }

  console.log('reordered cols', cols);

  // Now lay out the table
  var table_el = document.createElement('table');
  $(table_el).addClass('pure-table');
  
  function sortTable(l_sortby){
    console.log('sorting by ' + l_sortby);
    var parent = table_el.parentNode;
    $(table_el).empty();
    
    if (l_sortby === sortby){
      if (sortdir === 'asc'){
        sortdir = 'desc';
      }
      else {
        sortdir = 'asc';
      }
    }
    if (sortdir === 'asc'){
      $(parent).append(get_table(_.sortBy(data, l_sortby), full_data, onclicks, onhovers, reorder, l_sortby, sortdir));
    }
    else {
      $(parent).append(get_table(_.sortBy(data, l_sortby).reverse(), full_data, onclicks, onhovers, reorder, l_sortby, sortdir));      
    }
    return;
  }

  function onkeyup(e){
    var l_filter_text = this.value;
    var l_filter_field = this.name;
    console.log('filter_text', filter_text);
    
    var l_data;
    // Avoid unnecessary and unhelpful early filtering
    if (l_filter_text.length > 0 && l_filter_text.length < 3) return;
    if (l_filter_text === ''){
      l_data = full_data;
    }
    else {
      l_data = _.filter(data.slice(), function(n){
        if (n[l_filter_field] && n[l_filter_field].match(l_filter_text)) return true;
        return false;
      });
    }

    $(table_el).empty();
    var parent = table_el.parentNode;
    $(parent).append(get_table(l_data, full_data, onclicks, onhovers, reorder, sortby, sortdir, l_filter_field, l_filter_text));
    var input_el = $('input[name="' + l_filter_field + '"]')[0];
    input_el.focus();
    var val = input_el.value; //store the value of the element
    input_el.value = ''; //clear the value of the element
    input_el.value = val; 
  }

  var thead_el = document.createElement('thead');
  //$(thead_el).addClass('etch-complex-table__thead');
  var tr_el = document.createElement('tr');
  //$(tr_el).addClass('etch-complex-table__thead__row');
  for (var i = 0, len = cols.length; i < len; i++){
    var field = cols[i];
    // Figure out if we are sorting by this col and if it is desc
    // var sortclass = 'etch-complex-table__cell--sortasc';
    // if (field === sortby && sortdir !== 'asc') 
    //   sortclass = 'etch-complex-table__cell--sortdesc';
    var th_el = document.createElement('th');
    // $(th_el).addClass('etch-complex-table__thead__th '
    //   + 'etch-complex-table__cell '
    //   + 'etch-complex-table__cell--sortable '
    //   + 'etch-complex_table__cell--alignright '
    //   + sortclass);
    var text_el = document.createTextNode(field);
    var span_el = document.createElement('span');
    // $(span_el).addClass('etch-column__title');
    $(span_el).append(text_el);
    span_el.data = field;
    $(span_el).on('click', function(e){
      console.log('click', this.data);
      sortTable(this.data);
    })
    $(th_el).append(span_el);
    var div_el = document.createElement('div');
    // $(div_el).addClass('etch-field');
    var input_el = document.createElement('input');
    input_el.type = 'text';
    input_el.name = field;
    input_el.size = 4;
    if (field === filter_field){
      input_el.value = filter_text;
    }
    $(div_el).append(input_el);
    $(input_el).on('keyup', function(e){
      if (e.keyCode !== 13) return;
      console.log('keypress');
      onkeyup.bind(this).call(e)
      // clearTimeout(EVENT_ON_KEYUP);
      // EVENT_ON_KEYUP = setTimeout(onkeyup.bind(this).call(e), 1500);
    });
    $(th_el).append(div_el);
    th_el.appendChild(text_el);
    tr_el.appendChild(th_el);
  }
  thead_el.appendChild(tr_el);
  table_el.appendChild(thead_el);

  var tbody_el = document.createElement('tbody');
  $(tbody_el).addClass('context-menu-one');

  for (var i = 0, len = data.length; i < len; i++){
    var tr_el = document.createElement('tr');
    //$(tr_el).addClass('etch-complex-table__tbody__row');
    if (i % 2 === 0){
      $(tr_el).addClass('pure-table-even');
    }
    else {
      $(tr_el).addClass('pure-table-odd');
    }
    var row = Array();
    for (var j in data[i]){
      if (_.indexOf(cols, j) > -1)
        row[_.indexOf(cols, j)] = data[i][j];
    }
    for (var j = 0; j < row.length; j++){
      var td_el = document.createElement('td');
      // $(td_el).addClass('etch-complex-table__cell '
      //   + 'etch-complex-table__cell--filtered '
      //   + 'etch-complex-table__cell--nowrap');
      var text = row[j];
      if (typeof(text) === 'undefined'){
        text = '';
      }
      $(td_el).attr('data_field', cols[j]);
      $(td_el).attr('data_value', encodeURIComponent(text));
      $(td_el).addClass('grid-cell');
      var text_el = document.createTextNode(text);
      if (typeof(onclicks[ cols[j] ]) !== 'undefined' 
        || typeof(onhovers[ cols[j] ]) !== 'undefined'){
        var a_el = document.createElement('a');
        // $(a_el).addClass('etch-anchor');
        a_el.href = 'javascript:void(0)';
        if (typeof(onclicks[ cols[j] ]) !== 'undefined'){
          $(a_el).on('click', onclicks[ cols[j] ]);  
        }
        if (typeof(onhovers[ cols[j] ]) !== 'undefined'){
          $(a_el).on('mouseenter', onhovers[ cols[j] ]);
        }
        a_el.appendChild(text_el);
        td_el.appendChild(a_el);
      }
      else {
        td_el.appendChild(text_el);
      }

      for (var tag in TAGS){
        for (var tag_value in TAGS[tag]){
          if (text == tag_value){
            tag_text = document.createElement('span');
            tag_text.innerText = '#' + tag + ' ';
            $(tag_text).addClass('tag');
            td_el.appendChild(tag_text);
          }
        }
      }
      
      tr_el.appendChild(td_el);
    }
    tbody_el.appendChild(tr_el);
  }

  table_el.appendChild(tbody_el);
  $(table_el).contextMenu({
    selector: 'td',
    callback: handle_context_menu_callback,
    items: {
      pivot: {name: 'Pivot', icon: function(){ return 'fa fa-level-down fa-fw'} },
      sep: '-----',
      scope: {name: 'Scope', icon: function(){ return 'fa fa-binoculars fa-fw'} },
      sep1: '-----',
      note: {name: 'Note', icon: function(){ return 'fa fa-comment fa-fw'} },
      sep2: '-----',
      tag: {name: 'Tag', icon: function(){ return 'fa fa-hashtag fa-fw'} },
      sep3: '-----',
      favorite: {name: 'Favorite', icon: function(){ return 'fa fa-star fa-fw'} },
    }
  });

  // $(table_el).on('click', function(e){
  //   console.log('clicked', this);
  // });
  return table_el;
}

function handle_context_menu_callback(key, options) {
  var content = $(this).text();
  content = content.split('\n')[0];
  var item = TRANSCRIPT.latest_search();
  console.log(this, content, key, options);
  var key = key.toUpperCase();
  
  if (key === 'PIVOT'){
    var scope = TRANSCRIPT.update(key, {scope:content, id:item.id});
    ANALYSIS_TREE.propagate(content, 
      TRANSCRIPT.transcript[TRANSCRIPT.transcript.length - 1], true);
    set_current_action(scope);
    TRANSCRIPT.load_item.bind([TRANSCRIPT, item]).call();
    // $('#search_form input[name="search"]').val(content);
    // submit_form();
  }
  else if (key === 'NOTE'){
    create_note_dialog(content);
  }
  else if (key === 'SCOPE'){
    var scope = TRANSCRIPT.update(key, {scope:content});
    set_current_action(scope);
    ANALYSIS_TREE.propagate(content, 
      TRANSCRIPT.transcript[TRANSCRIPT.transcript.length - 1]);
  }
  else if (key === 'TAG'){
    create_tag_dialog(content);
  }
  else if (key === 'FAVORITE'){
    FAVORITES[content] = TRANSCRIPT.counter();
    update_favorites();
    var scope = TRANSCRIPT.update(key, {scope:content});
  }
  else {
    var scope = TRANSCRIPT.update(key, {scope:content});
    ANALYSIS_TREE.propagate(content, 
      TRANSCRIPT.transcript[TRANSCRIPT.transcript.length - 1]);
  }
};

function create_note_dialog(content){
  var div = document.createElement('div');
  div.id = 'write-note';
  div.title = 'Create Note';
  var span = document.createElement('h1');
  span.innerText = content;
  $(span).addClass('overwatch');
  div.appendChild(span);
  var form = document.createElement('form');
  div.appendChild(form);
  var fieldset = document.createElement('fieldset');
  form.appendChild(fieldset);
  var label = document.createElement('label');
  label.innerHTML = 'Note';
  fieldset.appendChild(label);
  var input = document.createElement('input');
  input.type = 'text';
  input.size = 80;
  input.name = 'note';
  input.id = 'note';
  $(input).attr('class', 'text ui-widget-content ui-corner-all');
  fieldset.appendChild(input);
  var submit = document.createElement('input');
  submit.type = 'submit';
  $(submit).attr('tabindex', -1);
  $(submit).attr('style', 'position:absolute; top:-1000px');
  fieldset.appendChild(submit);
  
  $('#transcript_container').append(div);

  function on_submit(event){
    event.preventDefault();
    console.log('SUBMIT', this);
    TRANSCRIPT.update('NOTE', content + ' ' + $('#note').val());
    dialog.dialog('close');
    $('#write-note').remove();
  }
  // modal
  var dialog; dialog = $( "#write-note" ).dialog({
    autoOpen: false,
    height: 400,
    width: 900,
    modal: true,
    buttons: {
      "Ok": on_submit,
      Cancel: function() {
        dialog.dialog( "close" );
      }
    },
    close: function() {
      form[ 0 ].reset();
    }
  });

  var form; form = dialog.find( "form" ).on( "submit", on_submit);

  //$( "#create-user" ).button().on( "click", function() {
    dialog.dialog( "open" );
  //});
}

function create_tag_dialog(content){
  var div = document.createElement('div');
  div.id = 'create-tag';
  var form = document.createElement('form');
  div.appendChild(form);
  var fieldset = document.createElement('fieldset');
  form.appendChild(fieldset);
  var label = document.createElement('label');
  label.innerHTML = 'Tag';
  fieldset.appendChild(label);
  var input = document.createElement('input');
  input.type = 'text';
  input.size = 20;
  input.name = 'tag';
  input.id = 'tag';
  $(input).attr('class', 'text ui-widget-content ui-corner-all');
  fieldset.appendChild(input);
  var submit = document.createElement('input');
  submit.type = 'submit';
  $(submit).attr('tabindex', -1);
  $(submit).attr('style', 'position:absolute; top:-1000px');
  fieldset.appendChild(submit);
  
  document.body.appendChild(div);

  function on_submit(event){
    event.preventDefault();
    console.log('TAG', this);
    var tagval = $('#tag').val();
    TRANSCRIPT.update('TAG', {
      scope:tagval + ' ' + content,
      tag: tagval,
      value: content
    });
    if (typeof(TAGS[tagval]) === 'undefined') TAGS[tagval] = {};
    TAGS[tagval][content] = TRANSCRIPT.counter();
    update_tags();
    dialog.dialog('close');
    //document.body.removeChild(div);
  }
  // modal
  var dialog; dialog = $( "#create-tag" ).dialog({
    autoOpen: false,
    height: 200,
    width: 300,
    modal: true,
    buttons: {
      "Ok": on_submit,
      Cancel: function() {
        dialog.dialog( "close" );
      }
    },
    close: function() {
      form[ 0 ].reset();
    }
  });

  var form; form = dialog.find( "form" ).on( "submit", on_submit);
  dialog.dialog( "open" );
}

function update_tags(){
  $('#tags').empty();
  for (var tag in TAGS){
    var div = document.createElement('div');
    var span = document.createElement('span');
    span.innerText = '#' + tag;
    $(span).addClass('tag-label');
    div.appendChild(span);
    var table = document.createElement('table');
    div.appendChild(table);
    var tbody = document.createElement('tbody');
    table.appendChild(tbody);
    for (var item in TAGS[tag]){
      var row = document.createElement('tr');
      tbody.appendChild(row);
      var cell = document.createElement('td');
      $(cell).addClass('tag');
      row.appendChild(cell);
      var text = document.createTextNode(item + ' ' + TAGS[tag][item]);
      cell.appendChild(text);
    }
    $('#tags').append(div);
  }
}

function update_favorites(content){
  $('#favorites').empty();
  var div = document.createElement('div');
  var table = document.createElement('table');
  div.appendChild(table);
  var tbody = document.createElement('tbody');
  table.appendChild(tbody);
  for (var favorite in FAVORITES){  
    var row = document.createElement('tr');
    tbody.appendChild(row);
    var cell = document.createElement('td');
    $(cell).addClass('tag');
    row.appendChild(cell);
    var img = document.createElement('i');
    $(img).addClass('fa fa-star golden');
    cell.appendChild(img);
    var text = document.createTextNode(favorite + ' ' + FAVORITES[favorite]);
    cell.appendChild(text);
  }
  $('#favorites').append(div);
}

function add_node(graph, name){
  // See if value already exists in nodes
  for (var i = 0, len = graph.nodes.length; i < len; i++){
    if (graph.nodes[i].label === name){
      return graph.nodes[i].name;
    }
  }
  
  graph.nodes.push({
    name: graph.nodes.length,
    label: name
  });
  console.log('added new node', graph.nodes[graph.nodes.length - 1]);

  return graph.nodes.length - 1;
}

function add_link(graph, src_id, dst_id, value){
  // See if this link already exists so we can add to the value
  for (var i = 0, len = graph.links.length; i < len; i++){
    if (graph.links[i].source === src_id && graph.links[i].target === dst_id){
      graph.links[i].value += value;
      return;
    }
  }
  // console.log('linking ' + src_id + ' to ' + dst_id + ' with values ' +
  //   graph.nodes[src_id] + ' and ' + graph.nodes[dst_id]);
  graph.links.push({
    source: src_id,
    target: dst_id,
    value: value
  });
}

function build_graph_from_hits(data){
  // data should point to results.aggregations.bucketname
  var graph = {
    nodes: [],
    links: []
  };

  // Build nodes/links
  for (var i = 0, len = data.length; i < len; i++){
    for (var j = 0, jlen = data[i].keys.length; j < jlen - 1; j++){
      var src = data[i].keys[j];
      var dst = data[i].keys[j + 1];
      graph.links.push({
        source: data[i].keys[j],
        target: data[i].keys[j + 1],
        value: data[i].doc_count
      });
      // var src_id = add_node(graph, src);
      // var dst_id = add_node(graph, dst);
      // add_link(graph, src_id, dst_id, data[i].doc_count); 
    }
  }

  // Add in nodes
  for (var i = 0, len = graph.links.length; i < len; i++){
    var found = false;
    for (var j = 0, jlen = graph.nodes.length; j < jlen; j++){
      if (graph.nodes[j].name === graph.links[i].source){
        found = true;
        break;
      }
    }
    if (!found){
      //console.log('Did not find ' + graph.links[i].source);
      graph.nodes.push({name: graph.links[i].source});
    }

    found = false;
    for (var j = 0, jlen = graph.nodes.length; j < jlen; j++){
      if (graph.nodes[j].name === graph.links[i].target){
        found = true;
        break;
      }
    }
    if (!found){
      //console.log('Did not find ' + graph.links[i].target);
      graph.nodes.push({name: graph.links[i].target});
    }
  }

  return graph;
}

function build_sankey(graph){
  var units = "Count";
 
  var margin = {top: 10, right: 10, bottom: 10, left: 10},
      width = $('#viz_container').width() - margin.left - margin.right,
      height = $('#viz_container').height() - margin.top - margin.bottom;
  console.log('width', width, 'height', height);
   
  var formatNumber = d3.format(",.0f"),    // zero decimal places
      format = function(d) { return formatNumber(d) + " " + units; },
      //color = d3.scaleOrdinal(d3.schemeCategory20);
      color = d3.scale.category20();

  // var el = document.createElement('div');
  // el.id = 'chart';
  // $('body').append(el);
   
  // append the svg canvas to the page
  $("#viz_container").empty();
  var svg = d3.select("#viz_container").append("svg")
      .attr("width", width + margin.left + margin.right)
      .attr("height", height + margin.top + margin.bottom)
    .append("g")
      .attr("transform", 
            "translate(" + margin.left + "," + margin.top + ")");
   
  // Set the sankey diagram properties
  var sankey = d3.sankey()
      .nodeWidth(36)
      .nodePadding(10)
      .size([width, height]);
   
  var path = sankey.link();
   
  
   
  var nodeMap = {};
  graph.nodes.forEach(function(x) { nodeMap[x.name] = x; });
  graph.links = graph.links.map(function(x) {
    return {
      source: nodeMap[x.source],
      target: nodeMap[x.target],
      value: x.value
    };
  });

  console.log('graph', graph);

  sankey
    .nodes(graph.nodes)
    .links(graph.links)
    .layout(32);
  console.log('graph.links', graph.links);

  // add in the links
  var link = svg.append("g").selectAll(".link")
      .data(graph.links)
    .enter().append("path")
      .attr("class", "link")
      .attr("d", path)
      .style("stroke-width", function(d) { return Math.max(1, d.dy); })
      .sort(function(a, b) { return b.dy - a.dy; });

  // add the link titles
  link.append("title")
    .text(function(d) {
      return d.source.name + " → " + d.target.name + "\n" + format(d.value); 
    });

  // add in the nodes
  var node = svg.append("g").selectAll(".node")
      .data(graph.nodes)
    .enter().append("g")
      .attr("class", "node")
      .attr("transform", function(d) { 
      return "translate(" + d.x + "," + d.y + ")"; })
    //.call(d3.drag()
    //  .subject(function(d) { return d; })
    //.on("start", function() {
    // this.parentNode.appendChild(this); })
    // .on("drag", dragmove));
    .call(d3.behavior.drag()
      .origin(function(d) { return d; })
      .on("dragstart", function() {
      this.parentNode.appendChild(this); })
      .on("drag", dragmove));

  // add the rectangles for the nodes
  node.append("rect")
    .attr("height", function(d) { return d.dy; })
    .attr("width", sankey.nodeWidth())
    .style("fill", function(d) { 
      return d.color = color(d.name.replace(/ .*/, "")); })
    .style("stroke", function(d) { 
      return d3.rgb(d.color).darker(2); })
    .append("title").text(function(d) { 
      return d.name + "\n" + format(d.value); 
    });

  // add in the title for the nodes
  node.append("text")
    .attr("x", -6)
    .attr("y", function(d) { return d.dy / 2; })
    .attr("dy", ".35em")
    .attr("text-anchor", "end")
    .attr("transform", null)
      .text(function(d) { return d.name; })
    .filter(function(d) { return d.x < width / 2; })
    .attr("x", 6 + sankey.nodeWidth())
    .attr("text-anchor", "start");

  $.contextMenu({
    selector: '#viz_container rect',
    callback: handle_context_menu_callback,
    items: {
      pivot: {name: 'Pivot', icon: function(){ return 'fa fa-level-down fa-fw'} },
      sep: '-----',
      scope: {name: 'Scope', icon: function(){ return 'fa fa-binoculars fa-fw'} },
      sep1: '-----',
      note: {name: 'Note', icon: function(){ return 'fa fa-comment fa-fw'} },
      sep2: '-----',
      tag: {name: 'Tag', icon: function(){ return 'fa fa-hashtag fa-fw'} },
      sep3: '-----',
      favorite: {name: 'Favorite', icon: function(){ return 'fa fa-star fa-fw'} },
    }
  });

  // the function for moving the nodes
  function dragmove(d) {
    d3.select(this).attr("transform", 
        "translate(" + (
             d.x = Math.max(0, Math.min(width - d.dx, d3.event.x))
          ) + "," + (
                   d.y = Math.max(0, Math.min(height - d.dy, d3.event.y))
            ) + ")");
    sankey.relayout();
    link.attr("d", path);
  }
}