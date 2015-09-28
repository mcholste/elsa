YAHOO.namespace('YAHOO.ELSA.Chart');
YAHOO.ELSA.Charts = [];
YAHOO.ELSA.Chart = function(){};
YAHOO.ELSA.Chart.registeredCallbacks = {};

YAHOO.namespace('YAHOO.ODE.Chart');

var rgb2hsl = function(r, g, b) {
  var r1 = r / 255;
  var g1 = g / 255;
  var b1 = b / 255;
  var min, max;
  if (r1 < g1) {
    min = r1;
    max = g1;
  } else {
    min = g1;
    max = r1;
  }
  if (b1 < min) {
    min = b1;
  } else if (b1 > max) {
    max = b1;
  }
  var delta = max - min;
  var s = 0;
  var h;
  var l = (max + min) / 2;
  if (delta > 0) {
    if (l < 0.5) {
      s = delta / (max + min);
    } else {
      s = delta / (2 - max - min);
    }
    if (r1 == max) {
      h = ((g1 - b1) / delta) % 6;
    } else if (g1 == max) {
      h = (b1 - r1) / delta + 2;
    } else {
      h = (r1 - g1) / delta + 4;
    }
    h *= 60;
    if (h < 0) {
      h += 360;
    }
  } else {
    h = 0;
  }
  l*= 100;
  s*= 100;
  var v = max*100;
  return [h,s,l];
};

YAHOO.ODE.Chart = function() {

	Chart.defaults.global.scaleOverride = true;
	Chart.defaults.global.scaleSteps = 4;
	Chart.defaults.global.scaleStepWidth = 250;
	Chart.defaults.global.scaleStartValue = 0;
	Chart.defaults.global.scaleLineColor = "rgba(0,0,0,0.5)";
	Chart.defaults.global.scaleFontSize = 10;
	Chart.defaults.global.tooltipTemplate = "<%if (label){%>Key:<%=label%>; Count:<%}%><%= value %>";
	Chart.defaults.Bar.barStrokeWidth = 1;
	Chart.defaults.Bar.barValueSpacing = 2;
	Chart.defaults.Bar.scaleShowGridLines = true;
	Chart.defaults.Bar.scaleShowVerticalLines = false;
	Chart.defaults.Bar.scaleGridLineColor = "rgba(0,0,0,0.2)";
	Chart.defaults.Bar.showXLabels = 10;
	Chart.defaults.HorizontalBar.scaleShowGridLines = true;
	Chart.defaults.HorizontalBar.scaleGridLineColor = "rgba(0,0,0,0.2)";
	Chart.defaults.Line.showXLabels = 10;
	Chart.defaults.Line.pointDotRadius = 1;
	Chart.defaults.Line.pointHitDetectionRadius = 2;
	Chart.defaults.Line.scaleShowGridLines = true;
	Chart.defaults.Line.scaleShowVerticalLines = false;
	Chart.defaults.Line.legendTemplate =
	Chart.defaults.Bar.legendTemplate = "<ul class=\"<%=name.toLowerCase()%>-legend\"><% for (var i=0; i<datasets.length; i++){%><li><span style=\"background-color:<%=datasets[i].strokeColor%>\"></span><%if(datasets[i].label){%><%=datasets[i].label%><%}%></li><%}%></ul>";;
	Chart.defaults.Pie.segmentStrokeWidth = 1;
	Chart.defaults.Pie.legendTemplate = "<ul class=\"<%=name.toLowerCase()%>-legend\"><% for (var i=0; i<segments.length; i++){%><li><span style=\"background-color:<%=segments[i].fillColor%>\"></span><%if(segments[i].label){%><%=segments[i].label%><%}%></li><%}%></ul>";
	Chart.defaults.Doughnut.segmentStrokeWidth = 1;
	Chart.defaults.Doughnut.legendTemplate = "<ul class=\"<%=name.toLowerCase()%>-legend\"><% for (var i=0; i<segments.length; i++){%><li><span style=\"background-color:<%=segments[i].fillColor%>\"></span><%if(segments[i].label){%><%=segments[i].label%><%}%></li><%}%></ul>";

	return {
		getPalette_a: function() {
			var a_rgb = [ "#43A5B6", "#aec7e8", "#ff7f0e", "#ffbb78", "#2ca02c", "#98df8a", "#d62728", "#ff9896" ].reverse();

			var mk_hsla = function(h,s,l,a) {
				return "hsla(" + h + "," + s + "%," + l + "%," + a + ")";
			};

			return a_rgb.map(function(c) {
				var r = parseInt(c.substr(1, 2), 16);
				var g = parseInt(c.substr(3, 2), 16);
				var b = parseInt(c.substr(5, 2), 16);
				var c = rgb2hsl(r, g, b);
				var h = c[0];
				var s = c[1];
				var l = c[2];
				return [mk_hsla(h,s,l,0.9), mk_hsla(h,s,l*0.8,0.9), mk_hsla(h,s,l*0.6,0.9), mk_hsla(h,s,l*0.4,1)];
			} );
		},
		getPalette: function() {
			return YAHOO.ODE.Chart.getPalette_a().map(function(c) {
				return {
					fillColor: c[0],
					strokeColor: c[1],
					highlightFill: c[2],
					highlightStroke: c[3]
				};
			} );
		},
		getSteps: function(ymax) {
			var stepBase = Math.pow(10, Math.floor(Math.log10(ymax)) - 1);
			var fact = [1, 2, 5, 10];
			var steps;
			var stepVal;
			for(var i = 0; i < fact.length; ++i) {
				steps = Math.floor(ymax / stepBase / fact[i]);
				if (steps < 10) {
					stepVal = stepBase * fact[i] * 2;
					break;
				}
			}
			steps /= 2;
			return {
				scaleStepWidth: stepVal,
				scaleSteps: steps
			};
		}
	};
}();

// Auto-graph given a graph type, title, and AoH of data
YAHOO.ELSA.Chart.Auto = function(p_oArgs){
    if (typeof p_oArgs.callback == 'undefined'){
        p_oArgs.callback = function(){};
    }
    YAHOO.ELSA.Chart.registeredCallbacks[p_oArgs.container] = p_oArgs.callback;
    logger.log('given container id: ' + p_oArgs.container);
    var id = YAHOO.ELSA.Charts.length;
    this.id = id;
    this.colorPalette = YAHOO.ODE.Chart.getPalette();

    this.type = p_oArgs.type;
    // Scrub nulls
    // Figure out columns using the first row
    var aElements = [];
    var iCounter = 0;
    var iColorPaletteLength = this.colorPalette.length;
    var ymax = null;
    var barCount = 0;
    for (var key in p_oArgs.data){
        if (key == 'x'){
            continue;
        }
        var aValues = [];
        for (var i in p_oArgs.data[key]){
            var val = p_oArgs.data[key][i];
            if (typeof val == 'object'){
                var iSum = 0;
                for (var j in p_oArgs.data[key][i]){
                    if (j == 'val'){
                        continue;
                    }
                    logger.log('iSum: ' + iSum + ', j: ' + j + ', val: ' + p_oArgs.data[key][i][j]);
                    iSum = iSum + parseInt(p_oArgs.data[key][i][j]);
                }
                aValues.push(iSum);
            }
            else {
                aValues.push(val);
            }
        }
	ymax = Math.max.apply(ymax, aValues);
	barCount += aValues.length;
        var thisColor = this.colorPalette[((iColorPaletteLength - (iCounter % iColorPaletteLength)) - 1)];
        aElements.push({
            fillColor: thisColor.fillColor,
            strokeColor: thisColor.strokeColor,
            highlightFill: thisColor.highlightFill,
            highlightStroke: thisColor.highlightStroke,
            label: key,
            data: aValues
        });
        iCounter++;
    }
    var opts = YAHOO.ODE.Chart.getSteps(ymax);

    // calculate label steps
    var iXLabelSteps = 1;
    if (p_oArgs.data.x.length > 10){
        iXLabelSteps = parseInt(p_oArgs.data.x.length / 10);
    }
    var aLabels = [];
    for (var i = 0; i < p_oArgs.data.x.length; ++i){
        aLabels.push(p_oArgs.data.x[i]);
    }

    var chartCfg = {
        title: {
            text:unescape(p_oArgs.title),
            style:'{font-size:16px;}'
        },
        elements: aElements,
        x_axis:{
            labels:{
                labels:p_oArgs.data.x,
                rotate:330,
                'visible-steps': iXLabelSteps
            }
        }
    }
    if (p_oArgs.bgColor){
        chartCfg.bg_colour = p_oArgs.bgColor;
    }
    this.cfg = chartCfg;

    // create a div within the given container so we can append the "Save As..." link
    var outerContainerDiv = YAHOO.util.Dom.get(p_oArgs.container);
	outerContainerDiv.setAttribute('class', 'outer-chart-div');
    var linkDiv = document.createElement('div');
    linkDiv.id = p_oArgs.container + '_link';
	/*
    var saveLink = document.createElement('a');
    saveLink.setAttribute('href', '#');
    saveLink.innerHTML = 'Save Chart As...';
    var aEl = new YAHOO.util.Element(saveLink);
    aEl.on('click', YAHOO.ELSA.Chart.saveImage, this.id);
    linkDiv.appendChild(saveLink);
	*/
	var titleEl = document.createElement('h3');
	titleEl.innerHTML = p_oArgs.title;
	linkDiv.appendChild(titleEl);
    outerContainerDiv.appendChild(linkDiv);

    var containerDiv = document.createElement('div');
    containerDiv.id = p_oArgs.container + '_container';
//	containerDiv.setAttribute('class', 'chart-div');
	var legendDiv = document.createElement('div');
	containerDiv.appendChild(legendDiv);
    var canvasEl = document.createElement('canvas');
    canvasEl.id = p_oArgs.container + '_canvas';
    containerDiv.appendChild(canvasEl);
	outerContainerDiv.style.float = 'none';
	outerContainerDiv.style.display = 'inline-block';
    outerContainerDiv.appendChild(containerDiv);
	var tblEl = outerContainerDiv.previousSibling;
	tblEl.style['overflow-y'] = 'auto';
	tblEl.style.overflow = 'auto';
	tblEl.style.display = 'inline-block';
	tblEl.style['max-height'] = '300px';
	tblEl.style.float = 'none';
	tblEl.style['vertical-align'] = 'top';
	var isFirefox = typeof InstallTrigger !== 'undefined';
	setTimeout(function() {
		sbWidth = 15;
		if (isFirefox) sbWidth += 5;
		tblEl.style.width = (sbWidth + tblEl.offsetWidth) + 'px';
		if (tblEl.offsetHeight > 300) {
			tblEl.style.height = '300px';
		}
	}, 100);
    this.container = containerDiv.id;
	var parDiv = outerContainerDiv.parentElement;
	parDiv.style['white-space'] = 'nowrap';
	var wrapperDiv = parDiv.parentElement;
	wrapperDiv.style.width = '99%';
	wrapperDiv.style.overflow = 'auto';

    var ctx = canvasEl.getContext("2d");
    var data = {
        labels: aLabels,
        datasets: aElements
    };

    logger.log('outerContainerDiv', outerContainerDiv);
    try {
        var iWidth = 1000;
        if (p_oArgs.width){
            iWidth = p_oArgs.width;
        }
		var cWidth = iWidth;
		if (barCount <= 10) {
			if (barCount <= 5) {
				cWidth = 40 + barCount * 190;
				if (cWidth < 350) {
					cWidth = 350;
				}
				iWidth = cWidth;
			}
			opts['barValueSpacing'] = (cWidth - 40) * 0.2 / barCount;
		}
		if (40 + barCount * 9.6 > iWidth) {
			cWidth = 40 + barCount * 9.6;
		}
        var iHeight = 250;
        if (p_oArgs.height){
            iHeight = p_oArgs.height;
        }
        ctx.canvas.height = iHeight;
        ctx.canvas.width = cWidth;
		canvasEl.style.width = cWidth + "px";
		outerContainerDiv.style.width = iWidth + "px";
    }
    catch (e){
        YAHOO.ELSA.Error(e);
    }

    var chart = YAHOO.ODE.Chart.makeChart(ctx, this.type, data, opts);
	legendDiv.innerHTML = chart.generateLegend();
    logger.log('element: ', YAHOO.util.Dom.get(this.container));
};

YAHOO.ODE.Chart.makeChart = function(ctx, type, data, opts) {
	opts = opts || {};
	if ('bar' == type) {
		return new Chart(ctx).Bar(data, opts);
	}
	return new Chart(ctx).Line(data, opts);
};

YAHOO.ELSA.Chart.saveImage = function (p_oEvent, p_iId){
    logger.log('save image with id ' + p_iId);
    try {
        var sImageData = YAHOO.util.Dom.get(YAHOO.ELSA.Charts[p_iId].container).get_img_binary();
        var oEl = document.createElement('img');
        oEl.id = 'save_image';
        oEl.src = 'data:image/png;base64,' + sImageData;
        win = window.open('', 'SaveChart', 'left=20,top=20,width=700,height=500,toolbar=0,resizable=1,status=0');
        win.document.body.appendChild(oEl);
    }
    catch (e){
        YAHOO.ELSA.Error(e);
    }
}
