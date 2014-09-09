{
   "charts" : [
      {
         "y" : "0",
         "options" : {
            "width" : 1000,
            "legend" : "in",
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
            "booleanRole" : "certainty",
            "isStacked" : false,
            "title" : "Bro Events",
            "titleTextStyle" : {
               "color" : "#000",
               "bold" : true,
               "fontSize" : "20"
            },
            "animation" : {
               "duration" : 500
            }
         },
         "queries" : [
            {
               "query" : "+Vulnerable_Version class:bro_notice",
               "label" : "Vulnerable_SW"
            },
            {
               "query" : "+md5 class:bro_notice",
               "label" : "Downloads"
            },
            {
               "query" : "+sql_injection_attacker class:bro_notice",
               "label" : "SQLi"
            },
            {
               "query" : "+self class:bro_ssl",
               "label" : "Self-Signed SSL"
            },
            {
               "query" : "+SERVFAIL class:bro_dns",
               "label" : "DNS SERVFAIL"
            }
         ],
         "x" : "0",
         "type" : "ColumnChart"
      },
      {
         "y" : "1",
         "options" : {
            "title" : "Self-signed SSL Certificates"
         },
         "queries" : [
            {
               "query" : " self class:bro_ssl groupby:subject limit:10",
               "label" : "Count"
            }
         ],
         "x" : "0",
         "type" : "Table"
      },
      {
         "y" : "1",
         "options" : {
            "title" : "Self-Signed SSL Destinations"
         },
         "queries" : [
            {
               "query" : " self class:bro_ssl groupby:dstip limit:10",
               "label" : "Destination"
            }
         ],
         "x" : "1",
         "type" : "PieChart"
      },
      {
         "y" : "1",
         "options" : {
            "title" : "Self-Signed SSL by Country"
         },
         "queries" : [
            {
               "query" : " self class:bro_ssl groupby:dstip limit:10 | geoip | sum(cc)",
               "label" : "Country"
            }
         ],
         "x" : "2",
         "type" : "PieChart"
      }
   ],
   "auth_required" : "0",
   "title" : "Bro IDS",
   "alias" : "bro"
}