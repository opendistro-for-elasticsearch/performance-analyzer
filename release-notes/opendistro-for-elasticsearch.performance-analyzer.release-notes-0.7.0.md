## Version 0.7.0 (2019-01-31)

Version compatible with elasticsearch 6.5.4

### New Features

This is the first release of the Open Distro Performance Analyzer.

Performance Analyzer has two components:
Performance Analyzer Plugin: Collects metrics across the stack and writes them to shared memory. As this is the plug-in, this will run within the Elasticsearch process.
Performance Analyzer Agent: A Java process that takes periodic snapshots of metrics collected by Plugin, and indexes them to provide a REST interface. The interface supports aggregations on metrics grouped by dimensions for a given time interval. More details of supported metrics, aggregations and dimensions can be found in the documentation.
