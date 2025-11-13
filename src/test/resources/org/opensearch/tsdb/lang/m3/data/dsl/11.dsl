{
  "size" : 0,
  "track_total_hits": -1,
  "query" : {
    "bool" : {
      "filter" : [
        {
          "bool" : {
            "should" : [
              {
                "term" : {
                  "labels" : {
                    "value" : "name:controller_commit_response_count",
                    "boost" : 1.0
                  }
                }
              },
              {
                "term" : {
                  "labels" : {
                    "value" : "name:controller_failed_response_count",
                    "boost" : 1.0
                  }
                }
              }
            ],
            "adjust_pure_negative" : true,
            "minimum_should_match" : "1",
            "boost" : 1.0
          }
        },
        {
          "range" : {
            "min_timestamp" : {
              "from" : null,
              "to" : 1001000000,
              "include_lower" : true,
              "include_upper" : true,
              "boost" : 1.0
            }
          }
        },
        {
          "range" : {
            "max_timestamp" : {
              "from" : 1000000000,
              "to" : null,
              "include_lower" : true,
              "include_upper" : true,
              "boost" : 1.0
            }
          }
        }
      ],
      "must_not" : [
        {
          "bool" : {
            "should" : [
              {
                "term" : {
                  "labels" : {
                    "value" : "region:us",
                    "boost" : 1.0
                  }
                }
              },
              {
                "term" : {
                  "labels" : {
                    "value" : "region:eu",
                    "boost" : 1.0
                  }
                }
              }
            ],
            "adjust_pure_negative" : true,
            "minimum_should_match" : "1",
            "boost" : 1.0
          }
        }
      ],
      "adjust_pure_negative" : true,
      "boost" : 1.0
    }
  },
  "aggregations" : {
    "0_unfold" : {
      "time_series_unfold" : {
        "min_timestamp" : 1000000000,
        "max_timestamp" : 1001000000,
        "step" : 100000,
        "stages" : [
          {
            "type" : "sum"
          }
        ]
      }
    },
    "0_coordinator" : {
      "coordinator_pipeline" : {
        "buckets_path" : [ ],
        "stages" : [
          {
            "type" : "alias",
            "pattern" : "total_count"
          }
        ],
        "references" : {
          "0_unfold" : "0_unfold"
        },
        "inputReference" : "0_unfold"
      }
    }
  }
}
