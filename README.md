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



