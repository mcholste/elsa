<!DOCTYPE html>
<meta charset="utf-8">
<style>

.force .node {
  stroke: #fff;
  stroke-width: 1.5px;
}

.force .link {
  stroke: #999;
  stroke-opacity: .6;
}

.flare .node {
  cursor: pointer;
}

.flare .node circle {
  fill: #fff;
  stroke: steelblue;
  stroke-width: 1.5px;
}

/*.flare .node text {
  font-family: 'open_sans', "Helvetica Neue", "Helvetica", "Roboto", "Arial", sans-serif;
  font-weight: 400;
  font-style: normal;
  border-spacing: 0;
  border-collapse: collapse;
  font-size: 10px;
  stroke: black;
}*/

.flare .node text {
  font: 10px sans-serif;
}

.flare .link {
  fill: none;
  stroke: #ccc;
  stroke-width: 1.5px;
}

.control {
  position: absolute;
  top: 0px;
  left: 0px;
}

</style>
<body>
<script src="https://cdnjs.cloudflare.com/ajax/libs/lodash.js/3.10.1/lodash.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/d3/3.5.2/d3.min.js"></script>
<script src="https://ajax.googleapis.com/ajax/libs/jquery/2.1.3/jquery.min.js"></script>
<div>
  <form id="search_form" style="display:inline-block">
    <input name="query"></input>
    <button id="submit">Search</button>
  </form>
</div>
<div id="container" style="position:relative">
  <div id="force" class="force" style="position:absolute;top:0px;"></div>
  <div id="flare" class="flare" style="position:absolute;top:0px;"></div>
</div>
<script>

var new_window = null;
var last_data = null;
var last_svg = null;

function identifyClusters(graph){
  var clusters = [];
  var done = {};
  for (var i = 0, len = graph.links.length; i < len; i++){
    var target = graph.links[i].target;
    var children = [graph.links[i]];
    var key = graph.links[i].source + ':' + target;
    if (done[key]) continue;
    done[key] = true;
    children.push.apply(children, getChildren(graph, target, done));
    clusters.push(children);
  }

  // Sort by link count
  clusters = clusters.sort(function(a,b){
    return a.length > b.length ? -1 : 1
  });
  return clusters;
}

function getChildren(graph, id, done){
  if (typeof(done) === 'undefined') done = {};
  var children = [];
  for (var i = 0, len = graph.links.length; i < len; i++){
    var src = graph.links[i].source;
    var dst = graph.links[i].target;
    var key = src + ':' + dst;
    if (done[key]) continue;
    
    if (src == id){
      done[key] = true;
      children.push(graph.links[i]);  
      //console.log('src, key:' + key);
      children.push.apply(children, getChildren(graph, dst, done));
    }
    else if (dst == id){
      done[key] = true;
      children.push(graph.links[i]); 
      children.push.apply(children, getChildren(graph, src, done));
      //console.log('dst, key:' + key);
      //children.push(getChildren(graph, src, done));
    }
  }
  return children;
}

function getClusterSummary(graph, links, limit, with_scores){
  if (typeof(with_scores) === 'undefined') with_scores = false;
  // Find all unique terms
  var ret = {};
  for (var i = 0, len = links.length; i < len; i++){
    var linkSummary = getLinkSummary(graph, links[i]);
    for (var field in linkSummary){
      if (typeof(ret[field]) === 'undefined') ret[field] = 0;
      ret[field] += linkSummary[field];
    }
  }
  if (with_scores) return ret;
  var sorted = Object.keys(ret).sort(function(a,b){
    return ret[a] > ret[b] ? -1 : 1;
  });
  return sorted.slice(0, limit).sort().join('\n');
}

function getLinkSummary(graph, link){
  var summary = {};
  var src = graph.data[ link.source ];
  var dst = graph.data[ link.target ];
  for (var field in link.explanation){
    var key = field + ':' + src[field];
    if (typeof(summary[key]) === 'undefined') summary[key] = 0;
    summary[key] += 1
    key = field + ':' + dst[field];
    if (typeof(summary[key]) === 'undefined') summary[key] = 0;
    summary[key] += 1
  }
  return summary;
}

function buildFlare(graph, id, done){
  if (typeof(done) === 'undefined') done = {};
  //console.log('id', id);
  var ret = {
    name: id
  };
  var children = [];
  for (var i = 0, len = graph.links.length; i < len; i++){
    var src = graph.links[i].source.name;
    var dst = graph.links[i].target.name;
    if (typeof(src) === 'undefined'){
      src = graph.links[i].source;
      dst = graph.links[i].target;
    }
    var key = src + ':' + dst;

    if (done[key]) continue;

    if (src == id){
      done[key] = true;  
      //console.log('src, key:' + key);
      children.push(buildFlare(graph, dst, done));
    }
    else if (dst == id){
      done[key] = true;  
      //console.log('dst, key:' + key);
      children.push(buildFlare(graph, src, done));
    }
  }
  if (children.length){
    ret['children'] = children;
  }
  return ret;
}

function drawFlareTextNode(graph, el, node, indents){
  if (typeof(indents) === 'undefined') indents = 0;
  var indent = '';
  for (var i = 0; i < indents; i++){
    indent += '  ';
  }
  var item = graph.data[parseInt(node.name)];
  var item_text = '';
  for (var i = 0, len = graph.fields.length; i < len; i++){
    if (graph.fields[i] === 'rawmsg') continue;
    var value = item[ graph.fields[i] ];
    if (!value) continue;
    item_text += value + ' ';
  }
  var text = document.createTextNode(indent + item_text + '\n');
  $(el).append(text);
  if (!node.children) return;
  var next_level = ++indents;
  node.children.forEach(function(d){
    drawFlareTextNode(graph, el, d, next_level);
  });
}

function drawFlareSummary(graph, node, result){
  if (typeof(result) === 'undefined') result = {};
  var id = parseInt(node.name);
  console.log('id', id);
  var item = graph.data[id];
  for (var i = 0, len = graph.fields.length; i < len; i++){
    var field = graph.fields[i];
    if (graph.fields[i] === 'rawmsg') continue;
    var value = item[field];
    if (!value) continue;
    var key = field + ':' + value;
    if (typeof(result[key]) === 'undefined') result[key] = 0;
    result[key] += linkFieldScore(graph, id, field);
  }
  if (!node.children) return;
  node.children.forEach(function(d){
    drawFlareSummary(graph, d, result);
  });
  return result;
}

function linkFieldScore(graph, id, field){
  var ret = 0;
  for (var i = 0, len = graph.links.length; i < len; i++){
    if (graph.links[i].source.name == id || graph.links[i].target.name === id){
      console.log('graph.links[i]', graph.links[i]);
      console.log('graph.links[i].explanation[field]', graph.links[i].explanation[field]);
      ret += graph.links[i].explanation[field];
    }
  }
  return ret;
}

function drawFlareText(graph, root){
  $('#flare').empty().show();
  var div = document.createElement('div');
  var pre = document.createElement('pre');
  drawFlareTextNode(graph, pre, root);
  $(div).append(pre);
  var summary_div = document.createElement('div');
  var summary = drawFlareSummary(graph, root);
  console.log('summary', summary);
  var sorted = Object.keys(summary).sort(function(a,b){
    return summary[a] > summary[b] ? -1 : 1;
  });
  summary_div.appendChild(document.createTextNode(sorted.slice(0,6).join(' ')));
  // var a = document.createElement('a');
  // a.href = '#';
  // a.appendChild(document.createTextNode('Back'));
  // $(a).click(function(){
  //   $('#flare').show();
  //   $(document.body).remove(div);
  // });
  // $(document.body).append(a);
  if (!new_window){
    new_window = window.open('', '', 'height=800,width=1000');
  }
  else {
    $(new_window.document.body).empty();
  }
  $(new_window.document.body).append(div);
  $(new_window.document.body).append(summary_div);
}

function get_item_text(graph, id){
  var item = graph.data[parseInt(id)];
  var item_text = '';
  for (var i = 0, len = graph.fields.length; i < len; i++){
    if (graph.fields[i] === 'rawmsg') continue;
    var value = item[ graph.fields[i] ];
    if (!value) continue;
    item_text += value + ' ';
  }
  return item_text;
}

function drawLinks(graph){
  $('#flare').empty().show();
  var table = document.createElement('table');
  var tbody = document.createElement('tbody');

  var all = [];
  for (var i = 0, len = graph.links.length; i < len; i++){
    var src = graph.links[i].source;
    var dst = graph.links[i].target;
    // var a = get_item_text(graph, src);
    // var b = get_item_text(graph, dst);
    var a = graph.data[parseInt(src)];
    var b = graph.data[parseInt(dst)];
    var value = graph.links[i].value;
    var explanation = graph.links[i].explanation;
    all.push([a,b,explanation, value]);
  }
  var sorted = all.sort(function(a,b){
    return a[3] > b[3] ? -1 : 1;
  });
  var tr, td;

  for (var i = 0, len = sorted.length; i < len; i++){
    tr = document.createElement('tr');
    td = document.createElement('td');
    $(td).attr('colspan', graph.fields.length - 1);
    $(td).text(sorted[i][3]);
    $(tr).css('background-color', 'gray');
    $(tr).append(td);
    $(tbody).append(tr);
    for (var j = 0; j < 3; j++){
      tr = document.createElement('tr');
      for (var k = 0, klen = graph.fields.length; k < klen; k++){
        if (graph.fields[k] === 'rawmsg') continue;
        td = document.createElement('td');
        if (j == 2){
          var val = sorted[i][j][ graph.fields[k] ];
          
          if (typeof(val) === 'undefined'){
            $(td).text("1");
          }
          else {
            $(td).text(val);  
          }
        }
        else {
          $(td).text(sorted[i][j][ graph.fields[k] ]);  
        }
        
        $(tr).append(td);
      }
      $(tbody).append(tr);
    }
  }
  $(table).append(tbody);
  $('#flare').append(table);
}

function drawFlare(graph, root){

  // var div = document.createElement('div');
  // $(div).on('click', function(){
  //   $('#flare').empty();
  //   $('#force').show();
  //   $(this).remove();
  // });
  // $(div).text('Close');
  // $(div).addClass('control');
  // document.body.appendChild(div);

  console.log('root', root);
  var width = 1900,
    height = 3000;

  var cluster = d3.layout.cluster()
    .size([height, width - 1000]);
    //.nodeSize([100,100]);

  var diagonal = d3.svg.diagonal()
      .projection(function(d) { return [d.y, d.x]; });

  $('#flare').empty().show();

  var svg = d3.select("#flare").append("svg")
    .attr("width", width)
    .attr("height", height)
    .on('click', function(){
      $('#flare').empty();
      $('#force').show();
      $(this).remove();
    })
    .append("g")
    .attr("transform", "translate(500,0)");


  var nodes = cluster.nodes(root),
    links = cluster.links(nodes);

  var link_lookup = {};
  for (var i = 0, len = graph.links.length; i < len; i++){
    var link = graph.links[i];
    if (typeof(link_lookup[ link.source ]) === 'undefined'){
      link_lookup[ link.source ] = {};
    }
    link_lookup[ link.source ][ link.target ] = link.score;
  }

  var link = svg.selectAll(".flare .link")
      .data(links)
    .enter().append("path")
      .attr("class", "link")
      .style("stroke-width", function(d) {
        if (typeof(link_lookup[d.source.name]) !== 'undefined'
          && typeof(link_lookup[d.source.name][d.target.name]) !== 'undefined')
          return 10 * link_lookup[d.source.name][d.target.name];
        if (typeof(link_lookup[d.target.name]) !== 'undefined'
          && typeof(link_lookup[d.target.name][d.source.name]) !== 'undefined')
          return 10 * link_lookup[d.target.name][d.source.name];
        console.log(d.source.name, d.target.name);
        return 1.5;
      })
      .attr("d", diagonal);

  var node = svg.selectAll(".flare .node")
      .data(nodes)
    .enter().append("g")
      .attr("class", "node")
      .attr("transform", function(d) { return "translate(" + d.y + "," + d.x + ")"; })

  node.append("circle")
      .attr("r", 4.5);

  node.append("text")
    .attr("dx", function(d) { return d.children ? -8 : 8; })
    .attr("dy", 3)
    .style("text-anchor", function(d) { return d.children ? "end" : "start"; })
    .text(function(d) { 
      var data = graph.data[ parseInt(d.name) ];
      //console.log('data', data, 'name', d.name, 'fields', graph.fields);
      var text = '';
      for (var i = 0, len = graph.fields.length; i < len; i++){
        var field = graph.fields[i];
        if (field === 'rawmsg') continue;
        var add = data[field];
        if (typeof(add) !== 'undefined') text += ' ' + add;
      }
      //console.log('text', text);
      return text;
    });

  // var insertLinebreaks = function (d) {
  //   console.log('d', d);
  //   var el = d3.select(this);
  //   console.log('el', el[0][0].textContent);
  //   var words = el[0][0].textContent.split(' ');
  //   el[0][0].textContent = '';

  //   for (var i = 0; i < words.length; i++) {
  //       var tspan = el.append('tspan').text(words[i]);
  //       if (i > 0)
  //         tspan.attr('x', 0).attr('dy', '15');
  //   }
  // };

  // d3.selectAll('g.node text').each(insertLinebreaks);


  d3.select(self.frameElement).style("height", height + "px");
  // $('#flare').on('click', function(d){
  //   $('#flare').hide();
  //   $('#force').show();
  // });
}

// via http://jsfiddle.net/hiddenloop/tpejt/
function stddev(a) {
  var r = {mean: 0, variance: 0, deviation: 0}, t = a.length;
  for(var m, s = 0, l = t; l--; s += a[l]);
  for(m = r.mean = s / t, l = t, s = 0; l--; s += Math.pow(a[l] - m, 2));
  return r.deviation = Math.sqrt(r.variance = s / t), r;
}

function textualClusterSummary(graph){
  var clusters = identifyClusters(graph);
  
  var cluster_lengths = [];
  for (var i = 0, len = clusters.length; i < len; i++){
    cluster_lengths.push(clusters[i].length);
  }
  var deviations = stddev(cluster_lengths);

  var ret = [];
  for (var i = 0, len = clusters.length; i < len; i++){
    var summary = getClusterSummary(graph, clusters[i], null, true);
    ret.push([cluster_lengths[i], summary]);
  }

  return ret;
}

function drawTextualClusterSummaryTable(graph){
  var data = textualClusterSummary(graph);

  var table = document.createElement('table');
  var tbody = document.createElement('tbody');

  var sorted = data.sort(function(a,b){
    return a[0] > b[0] ? -1 : 1;
  });
  var tr, td;

  for (var i = 0, len = sorted.length; i < len; i++){
    tr = document.createElement('tr');
    td = document.createElement('td');
    $(td).text(sorted[i][0]);
    $(tr).append(td);
    td = document.createElement('td');
    $(td).attr('colspan', Object.keys(sorted[i][1]).length - 1);
    //$(td).text(sorted[i][3]);
    //$(tr).css('background-color', 'gray');
    $(tr).append(td);
    $(tbody).append(tr);
    var summary_sorted = Object.keys(sorted[i][1]).sort(function(a,b){ return sorted[i][1][a] > sorted[i][1][b] ? -1 : 1 });
    for (var j = 0, jlen = summary_sorted.length; j < jlen; j++){
      tr = document.createElement('tr');
      td = document.createElement('td');
      $(td).text(summary_sorted[j]);
      $(tr).append(td);

      td = document.createElement('td');
      $(td).text(sorted[i][1][ summary_sorted[j] ]);
      $(tr).append(td);

      $(tbody).append(tr);
    }
  }
  $(table).append(tbody);
  $('#force').append(table);
}

function drawForce(graph){

  var clusters = identifyClusters(graph);
  var cluster_labels = {};
  console.log('clusters', clusters);
  var cluster_lengths = [];
  for (var i = 0, len = clusters.length; i < len; i++){
    cluster_lengths.push(clusters[i].length);
  }
  var deviations = stddev(cluster_lengths);
  for (var i = 0, len = clusters.length; i < len; i++){
    var summary = getClusterSummary(graph, clusters[i]);
    if (clusters[i].length > deviations.mean + deviations.deviation){
      console.log('i', i, 'clusters[i].length', clusters[i].length, 'looking for', deviations.mean + deviations.deviation, 'summary', summary);
      // no source.name here yet, that is added below in the enter() step
      
    }
    graph.nodes[ parseInt(clusters[i][0].source) ]['cluster_label'] = summary;
    graph.nodes[ parseInt(clusters[i][0].source) ]['cluster_id'] = i;
    cluster_labels[i] = summary;
  }
  
   var width = 1960,
    height = 1500;
  var top_margin = 100;
  var left_margin = 100;
  var cluster_width = 300;
  var cluster_height = 300;
  var min_cluster_width = 20;

  function build_layout(clusters, max_row_width){
    // Build foci based on how many clusters there are
    
    var layout = [[]];
    
    var current_x = left_margin + (cluster_width/2), 
      current_y = top_margin + (cluster_height/2);
    for (var i = 0, len = clusters.length; i < len; i++){
      var cluster = clusters[i];
      var num_links = cluster.length;
      // Try to divine the shape of this based on how many sources share the same target
      var link_count = {};
      for (var j = 0, jlen = clusters[i].length; j < jlen; j++){
        var target = clusters[i][j].target.name;
        if (typeof(link_count[target]) === 'undefined') link_count[target] = 0;
        link_count[target]++;
      }
      // Calculate the ratio of unique sources to overall count
      var ratio = Object.keys(link_count).length / num_links;
      // Apply this ratio to our standard width and height
      var this_width = cluster_width * (1 - ratio);
      if (this_width < min_cluster_width) this_width = min_cluster_width;
      var this_height = cluster_height * ratio;

      var focus = {
        x: current_x + this_width,
        y: current_y,
        width: this_width,
        height: this_height
      };
      current_x += this_width;
      layout[ layout.length - 1].push({id: i, focus: focus});
      console.log(i, current_x, focus.x, this_width);
      if (current_x + this_width >= max_row_width){
        layout.push([]);
        current_y += cluster_height;
        current_x = left_margin + (cluster_width/2);
      }
    }
    console.log('layout', layout);
    return layout;
  }


  var layout = build_layout(clusters, width);



  var color = d3.scale.category20();

  var force = d3.layout.force()
    //.charge(-120)
    .gravity(0)
    .charge(-60)
    .linkDistance(30)
    .size([width, height]);

  var svg = d3.select("#force").append("svg")
    .attr("width", width)
    .attr("height", height);
  last_svg = svg;

  var background_layer = svg.append('g').attr('class', 'background');
  var standard_layer = svg.append('g');
  var text_layer = svg.append('g').attr('class', 'text');
  
  force
    .nodes(graph.nodes)
    .links(graph.links)
    .start();

  var link = standard_layer.selectAll(".link")
    .data(graph.links)
    .enter().append("line")
    // .filter(function(d){
    //   return d.value >= .2;
    // })
    .attr("class", "link")
    
    // .on('mouseover', function(d){
    //   console.log(d);
    //   console.log(d3.select(this))
    // })
    
    .style("stroke-width", function(d) { return 10 * d.value; });

  link.append("title")
    .text(function(d) { return d.value + ' ' + JSON.stringify(d.explanation); })

  var node = standard_layer.selectAll(".node")
    .data(graph.nodes)
    .enter()
    .append("g")
    .attr('class', 'node')
    .append("circle")
    .attr("r", 5)
    .style("fill", function(d) { return color(graph.data[ parseInt(d.name) ]['class']); })
    .on('click', function(d){
      console.log(d);
      var flare = buildFlare(graph, d.name);
      //$('#force').hide();
      //drawFlare(graph, flare);
      drawFlareText(graph, flare);
    })
    .call(force.drag)
    //.attr("transform", function(d) { return "translate(" + d.y + "," + d.x + ")"; })

  node.append("title")
    .text(function(d) { return graph.data[d.name].class + ' ' + graph.data[d.name].rawmsg; });
    //.text(function(d){ return d.x + ' ' + d.y })

  var cluster_by_layout = [];
  for (var i = 0, len = layout.length; i < len; i++){
    for (var j = 0, jlen = layout[i].length; j < jlen; j++){
      var cluster_data = clusters[ layout[i][j].id ];
      var item = layout[i][j];

      cluster_by_layout.push({
        id: item.id,
        layout: item,
        data: cluster_data,
        summary: cluster_labels[ item.id ],
        x: item.focus.x - (item.focus.width/2),
        y: item.focus.y + (item.focus.height/2)
      });
    }
  }

  text_layer.selectAll('g.text')
    .data(cluster_by_layout)
    .enter()
    .append("text")
    //.attr("dx", 10)
    //.attr("dy", ".35em")
    .attr("x", function (d) {
      return d.x;
      // if (typeof(d) !== 'undefined') return d.x;
      // for (var i = 0, len = layout.length; i < len; i++){
      //   for (var j = 0, jlen = layout[i].length; j < jlen; j++){
      //     if (layout[i][j].id == d.cluster_id){
      //       return layout[i][j].focus.x + (cluster_width/2);
      //     }
      //   }
      // }
    })
    .attr("y", function (d) {
      return d.y;
      // if (typeof(d) !== 'undefined') return d.y;
      // for (var i = 0, len = layout.length; i < len; i++){
      //   for (var j = 0, jlen = layout[i].length; j < jlen; j++){
      //     if (layout[i][j].id == d.cluster_id){
      //       return layout[i][j].focus.y + cluster_height + top_margin;
      //     }
      //   }
      // }
    })
    .attr('cluster_id', function(d){
      return d.id;
    })
    // .attr('cluster_label', function(d){
    //   return d.summary;
    // })
    .text(function(d) { 
      //return d.summary;
      console.log('text adding label', d.cluster_label);
      //if (!d.cluster_label) return;
      return d.cluster_label ? d.cluster_label : '' 
    })
    .style("stroke", "gray");

  text_layer.selectAll('g.text text').each(function(d){
    if (!d.summary) return;
    var items = d.summary.split('\n');
    var el = d3.select(this);
    el.text('')
    var x = el.attr('x')
    for (var i = 0, len = items.length; i < len; i++){
      var tspan = el.append('tspan').text(items[i].slice(0, 20));
      tspan.attr('x', x);
      if (i > 0) tspan.attr('dy', 15);
        //tspan.attr('x', 0).attr('dy', '15');
    }
  });

  var node_by_cluster = {},
    cluster_bounds = {},
    foci = {},
    force_done = false;

  for (var i = 0, len = clusters.length; i < len; i++){
    for (var j = 0, jlen = clusters[i].length; j < jlen; j++){
      node_by_cluster[ clusters[i][j].source.name ] = i;
      node_by_cluster[ clusters[i][j].target.name ] = i;
    }
  }

  function getMeasures(cluster_id){
    var bounds = cluster_bounds[cluster_id];
    return {
      width: bounds.left - bounds.right,
      height: bounds.top - bounds.bottom,
      center: {
        x: (bounds.left - bounds.right)/2,
        y: (bounds.top - bounds.bottom)/2
      }
    }
  }

  function getHeight(cluster_id){
    return getMeasures(cluster_id).height;
  }

  function getWidth(cluster_id){
    return getMeasures(cluster_id).width;
  }

  function setBounds(bounds, x, y){
    if (y > bounds.top) bounds.top = y;
    if (y < bounds.bottom) bounds.bottom = y;
    if (x > bounds.left) bounds.left = x;
    if (x < bounds.right) bounds.right = x;
  }
  function getArea(a){
    return (a.left - a.right) * (a.top - a.bottom);
  }

  
  function on_tick(e) {
    var k = .1 * e.alpha;

    // if (Object.keys(cluster_bounds).length){
    //   var all = Object.keys(node_by_cluster);
    //   graph.nodes.forEach(function(o, i){
    //     //console.log('o', o, 'o.id', o.id, 'node_by_cluster', node_by_cluster[o.id], 'cluster_bounds[ node_by_cluster[o.id] ]', cluster_bounds[ node_by_cluster[o.id] ]);
    //     var good_enough = Math.sqrt(getArea(cluster_bounds[ node_by_cluster[o.index] ]));
    //     var target_y = foci[ node_by_cluster[o.index] ].y;
    //     var target_x = foci[ node_by_cluster[o.index] ].x;
    //     console.log('targetx', target_x, 'o.x', o.x, 'targety', target_y, 'o.y', o.y);
    //     if (target_y - o.y > good_enough || target_x - o.x > good_enough){
    //       //o.y += (target_y - o.y) * k;
    //       //o.x += (target_x - o.x) * k;
    //       o.y = target_y;
    //       o.x = target_x;
    //     }
    //     else {
    //       all.splice(all.indexOf(o.index), 1);
    //     }
    //   });
    //   if (!all.length){
    //     force_done = true;
    //   }
    // }

    link.attr("x1", function(d) { return d.source.x; })
      .attr("y1", function(d) { return d.source.y; })
      .attr("x2", function(d) { return d.target.x; })
      .attr("y2", function(d) { return d.target.y; });

    // Push the nodes towards foci
    graph.nodes.forEach(function(o, i) {
      var id = o.index;
      var cluster_id = node_by_cluster[id];
      var focus = null
      for (var i = 0, len = layout.length; i < len; i++){
        for (var j = 0, jlen = layout[i].length; j < jlen; j++){
          if (layout[i][j].id == cluster_id){
            focus = layout[i][j].focus;
            break;
          }
        }
      }
      if (!focus){
        console.log('no focus for cluster ' + id);
      }
      o.y += (focus.y - o.y) * k;
      o.x += (focus.x - o.x) * k;
    });

    node.attr("cx", function(d) { return d.x; })
      .attr("cy", function(d) { return d.y; });

    text_layer.selectAll('g.text text')
      .attr("x", function (d) {
        if (typeof(d) !== 'undefined') return d.x;
      })
      .attr("y", function (d) {
        if (typeof(d) !== 'undefined') return d.y;
      })
  };

  force.on("tick", on_tick);
  
  var n = 100;
  for (var i = n * n; i > 0; --i) force.tick();
  force.stop();
  //on_tick(force);
  var counter = 0;
  text_layer.selectAll('g.text text').each(function(d){
    counter++;
    // var cx = 0, cy = 0;
    // for (var i = 0, len = layout.length; i < len; i++){
    //   for (var j = 0, jlen = layout[i].length; j < jlen; j++){
    //     if (layout[i][j].id == d.name){
    //       //cx = layout[i][j].focus.x + (cluster_width/2);
    //       cx = layout[i][j].focus.x;
    //       //cy = layout[i][j].focus.y + cluster_height;
    //       cy = layout[i][j].focus.y;
    //       break;
    //     }
    //   }
    // }
    //console.log('text:', d.x, d.y, 'cluster:', cx, cy, d, this);
    
    var self = this;
    setTimeout(function(){
      var scale = .9 / Math.max(width / d.x, width / d.y);
    scale = 4;
      translate = [(width / 2) - d.x, (height / 2) - d.y];
      console.log("translate(" + translate + ")scale(" + 4 + ")");
      svg.selectAll('g')
      .transition()
      .duration(3750)
      .attr("transform", "translate(" + translate + ")scale(" + scale + ")");

      svg.selectAll('g')
      .transition()
      .duration(750)
      .attr("transform", "");
    }, 1000 * counter);
  })

  var total = 0;
  for (var i = 0, len = layout.length; i < len; i++){
    for (var j = 0, jlen = layout[i].length; j < jlen; j++){
      total++;
    }
  }
  console.log('total', total);
  
  
  // var zoom_factor = 4;
  // for (var i = 0, len = layout.length; i < len; i++){
  //   for (var j = 0, jlen = layout[i].length; j < jlen; j++){
  //     var item = layout[i][j].focus;
  //     setTimeout(function(){
  //       d3.select('g.text').transition()
  //       .duration(750)
  //       .attr("transform", function(d){
  //         "translate(" + width / 2 + "," + height / 2 + ")scale(" + 4 + ")translate(" + -item.x + "," + -item.y + ")");
  //         console.log("translate(" + width / 2 + "," + height / 2 + ")scale(" + 4 + ")translate(" + -item.focus.x + "," + -item.focus.y + ")");
  //       });
  //     }, 1000); 
  //   }
  // }
  
  //force.on('end', force_end);

  function force_end(){
    if (force_done) return;
    for (var i = 0, len = clusters.length; i < len; i++){
      cluster_bounds[i] = {
        top: 0,
        left: 0,
        right: width,
        bottom: height
      };
    }

    // Iterate through all drawn clusters and establish best bounds for rectangles
    standard_layer.selectAll(".node").each(function(d){
      var cluster_id = node_by_cluster[ d.name ];
      setBounds(cluster_bounds[cluster_id], d.x, d.y);
    });

    // Decide how to arrange these clusters based on their width/breadth
    var grid_layout = [[]];
    for (var cluster_id in Object.keys(cluster_bounds).sort(function(a, b){
      return getArea(cluster_bounds[a]) > getArea(cluster_bounds[b]) ? -1 : 1;
    })){
      var row = grid_layout[ grid_layout.length - 1 ];
      var current_total_row_width = 0;
      var this_width = getWidth(cluster_id);
      for (var i = 0, len = row.length; i < len; i++){
        current_total_row_width += getWidth(row[i])
      }
      // Start a new row if we are over width
      if (row.length && 
        (current_total_row_width > width || (current_total_row_width + this_width) > width)){
        grid_layout.push([]);
        current_total_row_width = 0;
        row = grid_layout[ grid_layout.length - 1 ];
      }
      row.push(cluster_id);
    }

    

    // Order rows by height
    grid_layout = grid_layout.sort(function(a,b){
      var a_total = 0;
      a.forEach(function(d){
        a_total += getHeight(d);
      });

      var b_total = 0;
      b.forEach(function(d){
        b_total += getHeight(d);
      });

      return a_total > b_total ? -1 : 1;
    });

    // Sort each row so the one with the largest area is first
    for (var i = 0, len = grid_layout.length; i < len; i++){
      var row = grid_layout[i];
      row = row.sort(function(a,b){
        return getArea(cluster_bounds[a]) > getArea(cluster_bounds[b]) ? -1 : 1;
      });
    }    

    var margin = 30;
    var y_cursor = 0;
    for (var i = 0, len = grid_layout.length; i < len; i++){
      var max_y = 0;
      for (var j = 0, jlen = grid_layout[i].length; j < jlen; j++){
        var cluster_id = grid_layout[i][j];
        if (getHeight(cluster_id) > max_y) max_y = getHeight(cluster_id);  
      }
      console.log('row height should be ' + max_y);
      y_cursor += max_y/2;
      console.log('y_cursor', y_cursor);
      var x_cursor = 0;
      for (var j = 0, jlen = grid_layout[i].length; j < jlen; j++){
        var cluster_id = grid_layout[i][j];
        x_cursor += getWidth(cluster_id) + margin;
        console.log('x_cursor', x_cursor, 'width', getWidth(cluster_id));
        foci[cluster_id] = { 
          id:cluster_id, 
          x: x_cursor - getWidth(cluster_id), 
          y: y_cursor
        };
      }
    }

    //if (!force_done) force.start();
    
    
    for (var node_id in node_by_cluster){
      d3.selectAll('.node')
        .filter(function(d){ return d.index == node_id})
        .children()
        .attr('transform', function(d){
          var node_id = d.name;
          var cluster_id = node_by_cluster[node_id];
          console.log('translate(-' + 
            (d.x - foci[cluster_id].x) + ',-' + 
            (d.y + foci[cluster_id].y) + ')');
          return 'translate(-' + 
            (d.x - foci[cluster_id].x) + ',-' + 
            (d.y + foci[cluster_id].y) + ')';
        });
        // .attr('transform', 'translate(-' + 
        //   (graph.nodes[node_id].x - foci[cluster_id].x) + ',-' +
        //   (graph.nodes[node_id].y - foci[cluster_id].y) + ')');
      // graph.nodes[node_id].x -= foci[cluster_id].x;
      // graph.nodes[node_id].y -= foci[cluster_id].y;
    }
    // d3.selectAll('.node')
    //   .attr("cx", function(d) { return d.x; })
    //   .attr("cy", function(d) { return d.y; });
    // force_done = true;
    // force.start();

    
    console.log('foci', foci);
    for (var f in foci){
      background_layer
        .append('rect')
        .attr('label', cluster_labels[f])
        .attr('x', foci[f].x)
        .attr('y', foci[f].y)
        .attr("width", getWidth(f) + margin)
        .attr("height", getHeight(f) + margin)
        .attr('fill', 'lightgrey')
        .attr('opacity', .5)
    }
    // standard_layer.selectAll(".node")
    //   .data(graph.nodes)
    //   .enter()
    //   .append("g")
    //   .attr('class', 'node')
    //   .append("circle")
    //   .attr("r", 5)
    //   .attr("cx", function(d) { return d.x; })
    //   .attr("cy", function(d) { return d.y; })
    //   .style("fill", function(d) { console.log('adding node'); return 'black'; })

    // Draw background layer rectangles for each cluster
    
    for (var cluster_id in cluster_bounds){
      var bounds = cluster_bounds[cluster_id];
      background_layer
        .append('rect')
        .attr('cluster_id', cluster_id)
        .attr('label', cluster_labels[cluster_id])
        .attr('x', bounds.right - (margin/2))
        .attr('y', bounds.bottom - (margin/2))
        .attr("width", (bounds.left - bounds.right) + margin)
        .attr("height", (bounds.top - bounds.bottom) + margin)
        .attr('fill', 'lightgrey')
        .attr('opacity', .5)
        .on('mouseover', function(d){
          console.log('width', cluster_bounds[ d3.select(this).attr('cluster_id') ].left - cluster_bounds[ d3.select(this).attr('cluster_id') ].right);
          console.log('this', this, 'd', d, d3.select(this).attr('label'));
        })
    }
  }
}

//$('#force').empty();
// var flare = buildFlare(638);
// drawFlare(flare);

$(document).on('ready', function(){
  $('#search_form input').val('123 OR 456 | groupby srcip');
});
$('#submit').on('click', function(e){
  e.preventDefault();

  var query = $('#search_form input').val();
  console.log('query: ' + query);
  $.get('http://localhost:8080/search?q=' + query
   //+ '&alerts=1'
   , 
   function(data, status, xhr){
    console.log(data, xhr, status);
    data.data = data.hits.hits;
    console.log('data.length', data.data.length);
    last_data = data;
    //var similarity = new Similarity(data);
    //$('#flare').hide();
    //console.log('similarity', similarity.graph);
    //drawForce(similarity.graph);
    //drawForce(data);
    drawTextualClusterSummaryTable(data);
    //drawLinks(data);
  });
});



</script>