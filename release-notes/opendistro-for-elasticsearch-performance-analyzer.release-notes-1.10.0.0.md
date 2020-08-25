## 2020-08-24 Version 1.10.0.0 (Current)

Supported Elasticsearch version 7.9.0

### Features
* cache max size metric collector ([#145](https://github.com/opendistro-for-elasticsearch/performance-analyzer/pull/145))
* Add initial support for dynamic config overriding ([#148](https://github.com/opendistro-for-elasticsearch/performance-analyzer/pull/148))
* Node collector split ([#162](https://github.com/opendistro-for-elasticsearch/performance-analyzer/pull/162))
* Add required mutual auth to gRPC Server/Client ([#254](https://github.com/opendistro-for-elasticsearch/performance-analyzer-rca/pull/254))
* Add NodeConfigCollector to collect node configs(threadpool capacity etc.) from ES ([#252](https://github.com/opendistro-for-elasticsearch/performance-analyzer-rca/pull/252))
* cache max size metrics ([#297](https://github.com/opendistro-for-elasticsearch/performance-analyzer-rca/pull/297))
* Implement cool off handling for the Publisher ([#272](https://github.com/opendistro-for-elasticsearch/performance-analyzer-rca/pull/272))
* FieldData and Shard Request Cache RCA ([#265](https://github.com/opendistro-for-elasticsearch/performance-analyzer-rca/pull/265))
* Add a cluster level collector for node config settings ([#298](https://github.com/opendistro-for-elasticsearch/performance-analyzer-rca/pull/298))
* Add cache decider and modify cache action ([#303](https://github.com/opendistro-for-elasticsearch/performance-analyzer-rca/pull/303))
* Implement Action Flip Flop Detection in the Publisher ([#287](https://github.com/opendistro-for-elasticsearch/performance-analyzer-rca/pull/287))
* Add listeners for publisher actions ([#295](https://github.com/opendistro-for-elasticsearch/performance-analyzer-rca/pull/295))
* Reader changes for dynamic enable/disable of RCA graph components ([#325](https://github.com/opendistro-for-elasticsearch/performance-analyzer-rca/pull/325))
* Populate default workload type and cache priority for the decider to base default actions ([#340](https://github.com/opendistro-for-elasticsearch/performance-analyzer-rca/pull/340))

### Enhancements
* IT improvements ([#143](https://github.com/opendistro-for-elasticsearch/performance-analyzer/pull/143))
* Add an IT which verifies that the RCA REST endpoint can be queried ([#157](https://github.com/opendistro-for-elasticsearch/performance-analyzer/pull/157))
* Use absolute path for configFilePath ([#389](https://github.com/opendistro-for-elasticsearch/performance-analyzer/pull/389))

### Bug fixes
* Use the correct ctor for NodeDetailsCollector ([#166](https://github.com/opendistro-for-elasticsearch/performance-analyzer/pull/166))
* Fix invalid cluster state ([#177](https://github.com/opendistro-for-elasticsearch/performance-analyzer/pull/177))
* Fix performance-analyzer-agent configFilePath ([#268](https://github.com/opendistro-for-elasticsearch/performance-analyzer-rca/pull/268))
* Rest mutual auth fix ([#279](https://github.com/opendistro-for-elasticsearch/performance-analyzer-rca/pull/279))
* Persistance concurrency bug ([#323](https://github.com/opendistro-for-elasticsearch/performance-analyzer-rca/pull/323))
* Fix rca.conf structure error ([#338](https://github.com/opendistro-for-elasticsearch/performance-analyzer-rca/pull/338))
* Fixing the summary serialization issue for cache RCAs ([#348](https://github.com/opendistro-for-elasticsearch/performance-analyzer-rca/pull/348))
* Fix bug in NodeConfigFlowUnit to add resource summary into protobuf ([#349](https://github.com/opendistro-for-elasticsearch/performance-analyzer-rca/pull/349))
* Fix bug in publisher to support cool off period on a per node basis ([#351](https://github.com/opendistro-for-elasticsearch/performance-analyzer-rca/pull/351))

### Infrastructure
* Integration test framework to test RCAs and decision Makers ([#301](https://github.com/opendistro-for-elasticsearch/performance-analyzer-rca/pull/301)

### Documentation
* Add release notes for 1.10 release ([#182](https://github.com/opendistro-for-elasticsearch/performance-analyzer/pull/182))

### Maintenance
* Build against elasticsearch 7.9 and resolve dependency conflicts ([#179](https://github.com/opendistro-for-elasticsearch/performance-analyzer/pull/179))
* Update jackson and bouncycastle artifacts ([#307](https://github.com/opendistro-for-elasticsearch/performance-analyzer-rca/pull/307))
* Add integ test for queue rejection cluster RCA ([#370](https://github.com/opendistro-for-elasticsearch/performance-analyzer-rca/pull/370))
* Add IT for cache tuning ([#382](https://github.com/opendistro-for-elasticsearch/performance-analyzer-rca/pull/382))
* Match dependencies with writer ([#393](https://github.com/opendistro-for-elasticsearch/performance-analyzer-rca/pull/393))

### Refactoring
* Make RCA framework NOT use ClusterDetailsEventProcessor ([#274](https://github.com/opendistro-for-elasticsearch/performance-analyzer-rca/pull/274))
* Refactor ModifyQueueCapacityAction to follow builder pattern ([#365](https://github.com/opendistro-for-elasticsearch/performance-analyzer-rca/pull/365))
* Refactor ModifyCacheCapacityAction to follow builder pattern ([#385](https://github.com/opendistro-for-elasticsearch/performance-analyzer-rca/pull/385))