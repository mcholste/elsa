{
   "charts" : [
      {
         "y" : "1",
         "options" : {
            "width" : 1000,
            "isStacked" : false,
            "title" : "Snort Events Over Time",
            "vAxes" : [
               {
                  "minValue" : null,
                  "viewWindowMode" : "pretty",
                  "maxValue" : null,
                  "viewWindow" : {
                     "min" : null,
                     "max" : null
                  },
                  "title" : null,
                  "useFormatFromData" : true
               },
               {
                  "viewWindowMode" : "pretty",
                  "viewWindow" : {},
                  "useFormatFromData" : true
               }
            ],
            "hAxis" : {
               "viewWindowMode" : "pretty",
               "viewWindow" : {},
               "useFormatFromData" : true
            },
            "animation" : {
               "duration" : 500
            },
            "booleanRole" : "certainty"
         },
         "queries" : [
            {
               "query" : " classification -sig_msg:trojan -sig_msg:scan class:snort",
               "label" : "Other"
            },
            {
               "query" : "+sig_msg:scan class:snort",
               "label" : "Scan"
            },
            {
               "query" : "+sig_msg:trojan -sig_msg:scan class:snort",
               "label" : "Trojan"
            }
         ],
         "x" : "0",
         "type" : "ColumnChart"
      },
      {
         "y" : "2",
         "options" : {
            "hAxis" : {
               "viewWindowMode" : "pretty",
               "viewWindow" : {},
               "useFormatFromData" : true
            },
            "vAxes" : [
               {
                  "minValue" : null,
                  "viewWindowMode" : "pretty",
                  "maxValue" : null,
                  "viewWindow" : {
                     "min" : null,
                     "max" : null
                  },
                  "title" : null,
                  "useFormatFromData" : true
               },
               {
                  "viewWindowMode" : "pretty",
                  "viewWindow" : {},
                  "useFormatFromData" : true
               }
            ],
            "booleanRole" : "certainty"
         },
         "queries" : [
            {
               "query" : " sig_msg:trojan groupby:SNORT.sig_msg limit:20",
               "label" : "Count"
            }
         ],
         "x" : "0",
         "type" : "Table"
      },
      {
         "y" : "3",
         "options" : {
            "width" : 1000,
            "is3D" : false,
            "hAxis" : {
               "viewWindowMode" : "pretty",
               "viewWindow" : {},
               "useFormatFromData" : true
            },
            "vAxes" : [
               {
                  "minValue" : null,
                  "viewWindowMode" : "pretty",
                  "maxValue" : null,
                  "viewWindow" : {
                     "min" : null,
                     "max" : null
                  },
                  "title" : null,
                  "useFormatFromData" : true
               },
               {
                  "viewWindowMode" : "pretty",
                  "viewWindow" : {},
                  "useFormatFromData" : true
               }
            ],
            "booleanRole" : "certainty",
            "colors" : [
               "#3366CC",
               "#DC3912",
               "#FF9900",
               "#109618",
               "#990099",
               "#0099C6",
               "#DD4477",
               "#66AA00",
               "#B82E2E",
               "#316395",
               "#994499",
               "#22AA99",
               "#AAAA11",
               "#6633CC",
               "#E67300",
               "#8B0707",
               "#651067",
               "#329262",
               "#5574A6",
               "#3B3EAC",
               "#B77322",
               "#16D620",
               "#B91383",
               "#F4359E",
               "#9C5935",
               "#A9C413",
               "#2A778D",
               "#668D1C",
               "#BEA413",
               "#0C5922",
               "#743411"
            ],
            "pieHole" : "0.5",
            "title" : "Scanning Hosts"
         },
         "queries" : [
            {
               "query" : "+sig_msg:scan class:snort groupby:SNORT.srcip",
               "label" : "+sig_msg:scan class:snort"
            }
         ],
         "x" : "0",
         "type" : "PieChart"
      },
      {
         "y" : "4",
         "options" : {
            "title" : null
         },
         "queries" : [
            {
               "query" : "+classification class:snort groupby:srcip | geoip | sum(cc)",
               "label" : "Source Hosts"
            },
            {
               "query" : "+classification class:snort groupby:dstip | geoip | sum(cc)",
               "label" : "Dest Hosts"
            }
         ],
         "x" : "0",
         "type" : "GeoChart"
      },
      {
         "y" : "3",
         "options" : {
            "title" : null
         },
         "queries" : [
            {
               "query" : "+classification groupby:snort.sig_classification",
               "label" : "Class"
            }
         ],
         "x" : "1",
         "type" : "PieChart"
      },
      {
         "y" : "3",
         "options" : {
            "title" : null
         },
         "queries" : [
            {
               "query" : "+classification groupby:snort.sig_priority",
               "label" : "Priority"
            }
         ],
         "x" : "2",
         "type" : "ColumnChart"
      }
   ],
   "auth_required" : "0",
   "title" : "Snort",
   "alias" : "snort"
}