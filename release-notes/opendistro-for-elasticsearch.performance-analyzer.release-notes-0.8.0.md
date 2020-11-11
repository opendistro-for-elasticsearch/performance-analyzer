## Version 0.8.0

Version compatible with elasticsearch 6.6.2

### New Features

This is the release of the Open Distro Performance Analyzer that will work with elasticsearch 6.6.2

### Improvements

* Better measurement granularity for Master Metrics [#16](https://github.com/opendistro-for-elasticsearch/performance-analyzer/pull/16)

### Bug fixes

* Bad File Descriptor Fix: Close not handled properly on sun.tools.attach.VirtualMachineImpl$SocketInputStream; fixed it on the Client side [#20](https://github.com/opendistro-for-elasticsearch/performance-analyzer/pull/20)
* Close DB Connection on reader restart [ab841cd]( https://github.com/opendistro-for-elasticsearch/performance-analyzer/commit/ab841cd462717d1a05da08028e63414887d8d71a)
