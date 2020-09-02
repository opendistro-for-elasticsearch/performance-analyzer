[![Java CI](https://github.com/opendistro-for-elasticsearch/performance-analyzer/workflows/Java%20CI/badge.svg)](https://github.com/opendistro-for-elasticsearch/performance-analyzer/actions?query=workflow%3A%22Java+CI%22)
[![CD](https://github.com/opendistro-for-elasticsearch/performance-analyzer/workflows/CD/badge.svg)](https://github.com/opendistro-for-elasticsearch/performance-analyzer/actions?query=workflow%3ACD)
[![Documentation](https://img.shields.io/badge/api-reference-blue.svg)](https://opendistro.github.io/for-elasticsearch-docs/docs/pa/)
[![Chat](https://img.shields.io/badge/chat-on%20forums-blue)](https://discuss.opendistrocommunity.dev/c/performance-analyzer/)
![PRs welcome!](https://img.shields.io/badge/PRs-welcome!-success)

# Performance Analyzer
Performance Analyzer exposes a REST API that allows you to query numerous performance metrics for your cluster, including aggregations of those metrics, independent of the Java Virtual Machine (JVM). PerfTop is the default command line interface (CLI) for displaying those metrics.

## Setup



## Performance Analyzer API
Performance Analyzer uses a single HTTP method and URI for all requests:

GET `<endpoint>/_opendistro/_performanceanalyzer/metrics`

Then you provide parameters for metrics, aggregations, dimensions, and nodes (optional):

```
?metrics=<metrics>&agg=<aggregations>&dim=<dimensions>&nodes=all"
```

* metrics - comma separated list of metrics you are interested in. For a full list of metrics, see Metrics Reference.
* agg - comma separated list of agg to be used on each metric. Possible values are sum, avg, min and max. Length of the list should be equal to the number of metrics specified.
* dim - comma separated list of dimensions. For the list of dimensions supported by each metric, see Metrics Reference.
* nodes - If the string all is passed, metrics from all nodes in the cluster are returned. For any other value, metrics from only the local node is returned.

### SAMPLE REQUEST
GET `_opendistro/_performanceanalyzer/metrics?metrics=Latency,CPU_Utilization&agg=avg,max&dim=ShardID&nodes=all`


## Batch Metrics API
While the basic metrics api associated with performance analyzer provies the last 5 seconds worth of metrics, the batch metrics api provides more detailed metrics and from longer periods of time. See the [design doc](https://github.com/opendistro-for-elasticsearch/performance-analyzer-rca/blob/master/docs/batch-metrics-api.md) for more information.

The Batch Metrics API uses a single HTTP method and URI for all requests:

GET `<endpoint>/_opendistro/_performanceanalyzer/batch`

Then you provide parameters for metrics, starttime, endtime, and samplingperiod (optional):

```
?metrics=<metrics>&starttime=<starttime>&endtime=<endtime>&samplingperiod=5"
```

* metrics - comma separated list of metrics you are interested in. For a full list of metrics, see Metrics Reference.
* starttime - Unix timestamp (difference between the current time and midnight, January 1, 1970 UTC) determining the oldest data point to return. starttime is inclusive — data points from at or after the starttime will be returned. Note, the starttime and endtime supplied by the user with both be rounded down to the nearest samplingperiod.
* endtime - Unix timestamp determining the freshest data point to return. endtime is exclusive — only datapoints from before the endtime will be returned.
* samplingperiod - The sampling period in seconds. The requested time range will be partitioned according to the sampling period, and data from the first available 5s interval in each partition will be returned to the user. Must be at least 5s, must be less than the retention period, and must be a multiple of 5. The default is 5s.

Note, the maximum number of datapoints that a single query can request for via API is capped at 100,800 datapoints. If a query exceeds this limit, an error is returned. The query parameters can be adjusted on such queries to request for fewer datapoints at a time.

The retention period for batch metrics can be adjusted by setting batch-metrics-retention-period-minutes in performance-analyzer.properties. The default value is 7, and values can range from 1 to 60 (inclusive).

Note, the default retention period is 7 minutes because a typical use-case would be to query for 5 minutes worth of data from the node. In order to do this, a client would actually select a starttime of now-6min and an endtime of now-1min (this one minute offset will give sufficient time for the metrics in the time range to be available at the node). Atop this 6 minutes of retention, we need an extra 1 minute of retention to account for the time that would have passed by the time the query arrives at the node, and for the fact that starttime and endtime will be rounded down to the nearest sampling-period.

### SAMPLE REQUEST
GET `_opendistro/_performanceanalyzer/batch?metrics=CPU_Utilization,IO_TotThroughput&starttime=1594412650000&endtime=1594412665000&samplingperiod=5`


## Documentation

Please refer to the [technical documentation](https://opendistro.github.io/for-elasticsearch-docs/) for detailed information on installing and configuring Performance Analyzer.

## Code of Conduct

This project has adopted an [Open Source Code of Conduct](https://opendistro.github.io/for-elasticsearch/codeofconduct.html).


## Security issue notifications

If you discover a potential security issue in this project we ask that you notify AWS/Amazon Security via our [vulnerability reporting page](http://aws.amazon.com/security/vulnerability-reporting/). Please do **not** create a public GitHub issue.


## Licensing

See the [LICENSE](./LICENSE) file for our project's licensing. We will ask you to confirm the licensing of your contribution.

## Copyright

Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.



