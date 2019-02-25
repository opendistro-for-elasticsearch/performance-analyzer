# Performance Analyzer
Performance Analyzer exposes a REST API that allows you to query numerous performance metrics for your cluster, including aggregations of those metrics, independent of the Java Virtual Machine (JVM). PerfTop is the default command line interface (CLI) for displaying those metrics.

## Performance Analyzer API
Performance Analyzer uses a single HTTP method and URI for all requests:

GET `<endpoint>/_performanceanalyzer/metrics`

Then you provide parameters for metrics, aggregations, dimensions, and nodes (optional):

```
?metrics=<metrics>&agg=<aggregations>&dim=<dimensions>&nodes=all"
```

* metrics - comma separated list of metrics you are interested in. For a full list of metrics, see Metrics Reference.
* agg - comma separated list of agg to be used on each metric. Possible values are sum, avg, min and max. Length of the list should be equal to the number of metrics specified.
* dim - comma separated list of dimensions. For the list of dimensions supported by each metric, see Metrics Reference.
* nodes - If the string all is passed, metrics from all nodes in the cluster are returned. For any other value, metrics from only the local node is returned.

### SAMPLE REQUEST
GET `_performanceanalyzer/metrics?metrics=Latency,CPU_Utilization&agg=avg,max&dim=ShardID&nodes=all`
### SAMPLE RESPONSE
```
{
  "pFbP1h4mRW6svN-wjtO8Pg": {
    "timestamp": 1548283205000,
    "data": {
      "fields": [{
          "name": "ShardID",
          "type": "VARCHAR"
        },
        {
          "name": "Latency",
          "type": "DOUBLE"
        },
        {
          "name": "CPU_Utilization",
          "type": "DOUBLE"
        }
      ],
      "records": [
        [
          1,
          13,
          0.018
        ]
      ]
    }
  },
  "MKp4Sp9jS1ism4gwIcBuqg": {
    "timestamp": 1548283205000,
    "data": {
      "fields": [{
          "name": "ShardID",
          "type": "VARCHAR"
        },
        {
          "name": "Latency",
          "type": "DOUBLE"
        },
        {
          "name": "CPU_Utilization",
          "type": "DOUBLE"
        }
      ],
      "records": [
        [
          2,
          19,
          0.018792012026887697
        ]
      ]
    }
  }
}
```
Performance Analyzer updates its data every five seconds. If you create a custom client, we recommend using that same interval for calls to the API.

## Metrics Reference

This page contains all Performance Analyzer metrics. All metrics support the avg, sum, min, and max aggregations, although certain metrics only measure one thing, making the choice of aggregation irrelevant.

|Metric|Dimensions|Description|
|------|----------|-----------|
|CPU_Utilization|ShardID, IndexName, Operation, ShardRole|CPU usage ratio. CPU time (in milliseconds) used by the associated thread(s) in the past five seconds, divided by 5000 milliseconds.|
|Paging_MajfltRate|ShardID, IndexName, Operation, ShardRole|The number of major faults per second in the past five seconds. A major fault requires the process to load a memory page from disk.|
|Paging_MinfltRate|ShardID, IndexName, Operation, ShardRole|The number of minor faults per second in the past five seconds. A minor fault does not requires the process to load a memory page from disk.|
|Paging_RSS|ShardID, IndexName, Operation, ShardRole|The number of pages the process has in real memory- the pages that count towards text, data, or stack space. This number does not include pages that have not been demand-loaded in or swapped out.|
|Sched_Runtime|ShardID, IndexName, Operation, ShardRole|Time (seconds) spent executing on the CPU per context switch.|
|Sched_Waittime|ShardID, IndexName, Operation, ShardRole|Time (seconds) spent waiting on a run queue per context switch.|
|Sched_CtxRate|ShardID, IndexName, Operation, ShardRole|Number of times run on the CPU per second in the past five seconds.|
|Heap_AllocRate|ShardID, IndexName, Operation, ShardRole|An approximation of the heap memory allocated, in bytes, per second in the past five seconds|
|IO_ReadThroughtput|ShardID, IndexName, Operation, ShardRole|Number of bytes read per second in the last five seconds.|
|IO_WriteThroughput|ShardID, IndexName, Operation, ShardRole|Number of bytes written per second in the last five seconds.|
|IO_TotThroughput|ShardID, IndexName, Operation, ShardRole|Number of bytes read or written per second in the last five seconds.|
|IO_ReadSyscallRate|ShardID, IndexName, Operation, ShardRole|Read system calls per second in the last five seconds.|
|IO_WriteSyscallRate|ShardID, IndexName, Operation, ShardRole|Write system calls per second in the last five seconds.|
|IO_TotalSyscallRate|ShardID, IndexName, Operation, ShardRole|Read and write system calls per second in the last five seconds.|
|Thread_Blocked_Time|ShardID, IndexName, Operation, ShardRole|Average time (seconds) that the associated thread(s) blocked to enter or reenter a monitor.|
|Thread_Blocked_Event|ShardID, IndexName, Operation, ShardRole|The total number of times that the associated thread(s) blocked to enter or reenter a monitor (i.e. the number of times a thread has been in the blocked state).|
|Indexing_ThrottleTime|ShardID, IndexName, Operation, ShardRole|ShardID, IndexName	Time (milliseconds) that the index has been under merge throttling control in the past five seconds.|
|Cache_Query_Hit|ShardID, IndexName, Operation, ShardRole|The number of successful lookups in the query cache in the past five seconds.|
|Cache_Query_Miss|ShardID, IndexName, Operation, ShardRole|The number of lookups in the query cache that failed to retrieve a `DocIdSet` in the past five seconds. `DocIdSet` is a set of document IDs in Lucene.|
|Cache_Query_Size|ShardID, IndexName, Operation, ShardRole|Query cache memory size in bytes.|
|Cache_FieldData_Eviction|ShardID, IndexName, Operation, ShardRole|The number of times Elasticsearch has evicted data from the fielddata heap space (occurs when the heap space is full) in the past five seconds.|
|Cache_FieldData_Size|ShardID, IndexName, Operation, ShardRole|Fielddata memory size in bytes.|
|Cache_Request_Hit|ShardID, IndexName, Operation, ShardRole|The number of successful lookups in the shard request cache in the past five seconds.|
|Cache_Request_Miss|ShardID, IndexName, Operation, ShardRole|The number of lookups in the request cache that failed to retrieve the results of search requests in the past five seconds.|
|Cache_Request_Eviction|ShardID, IndexName, Operation, ShardRole|The number of times Elasticsearch evicts data from shard request cache (occurs when the request cache is full) in the past five seconds.|
|Cache_Request_Size|ShardID, IndexName, Operation, ShardRole|Shard request cache memory size in bytes.|
|Refresh_Event|ShardID, IndexName, Operation, ShardRole|The total number of refreshes executed in the past five seconds.|
|Refresh_Time|ShardID, IndexName, Operation, ShardRole|The total time (milliseconds) spent executing refreshes in the past five seconds|
|Flush_Event|ShardID, IndexName, Operation, ShardRole|The total number of flushes executed in the past five seconds.|
|Flush_Time|ShardID, IndexName, Operation, ShardRole|The total time (milliseconds) spent executing flushes in the past five seconds.|
|Merge_Event|ShardID, IndexName, Operation, ShardRole|The total number of merges executed in the past five seconds.|
|Merge_Time|ShardID, IndexName, Operation, ShardRole|The total time (milliseconds) spent executing merges in the past five seconds.|
|Merge_CurrentEvent|ShardID, IndexName, Operation, ShardRole|The current number of merges executing.|
|Indexing_Buffer|ShardID, IndexName, Operation, ShardRole|Index buffer memory size in bytes.|
|Segments_Total|ShardID, IndexName, Operation, ShardRole|The number of segments.|
|Segments_Memory|ShardID, IndexName, Operation, ShardRole|Estimated memory usage of segments in bytes.|
|Terms_Memory|ShardID, IndexName, Operation, ShardRole|Estimated memory usage of terms dictionaries in bytes.|
|StoredFields_Memory|ShardID, IndexName, Operation, ShardRole|Estimated memory usage of stored fields in bytes.|
|TermVectors_Memory|ShardID, IndexName, Operation, ShardRole|Estimated memory usage of term vectors in bytes.|
|Norms_Memory|ShardID, IndexName, Operation, ShardRole|Estimated memory usage of norms (normalization factors) in bytes.|
|Points_Memory|ShardID, IndexName, Operation, ShardRole|Estimated memory usage of points in bytes.|
|DocValues_Memory|ShardID, IndexName, Operation, ShardRole|Estimated memory usage of doc values in bytes.|
|IndexWriter_Memory|ShardID, IndexName, Operation, ShardRole|Estimated memory usage by the index writer in bytes.|
|Bitset_Memory|ShardID, IndexName, Operation, ShardRole|Estimated memory usage for the cached bit sets in bytes.|
|VersionMap_Memory|ShardID, IndexName, Operation, ShardRole|Estimated memory usage of the version map in bytes.|
|ShardEvents|ShardID, IndexName, Operation, ShardRole|The total number of events executed on a shard in the past five seconds.|
|ShardBulkDocs|ShardID, IndexName, Operation, ShardRole|The total number of documents indexed in the past five seconds.|
|Latency|Operation, Exception, Indices, HTTPRespCode, ShardID, IndexName, ShardRole|Latency (milliseconds) of a request.|
|GC_Collection_Event|MemType|The number of garbage collections that have occurred in the past five seconds.|
|GC_Collection_Time|MemType|The approximate accumulated time (milliseconds) of all garbage collections that have occurred in the past five seconds.|
|Heap_Committed|MemType|The amount of memory (bytes) that is committed for the JVM to use.|
|Heap_Init|MemType|The amount of memory (bytes) that the JVM initially requests from the operating system for memory management.|
|Heap_Max|MemType|The maximum amount of memory (bytes) that can be used for memory management.|
|Heap_Used|MemType|The amount of used memory in bytes.|
|Disk_Utilization|DiskName|Disk utilization rate: percentage of disk time spent reading and writing by the Elasticsearch process in the past five seconds.|
|Disk_WaitTime|DiskName|Average duration (milliseconds) of read and write operations in the past five seconds.|
|Disk_ServiceRate|DiskName|Service rate: MB read or written per second in the past five seconds. This metric assumes that each disk sector stores 512 bytes.|
|Net_TCP_NumFlows|DestAddr|Number of sample collected. Performance Analyzer collects one sample every five seconds.|
|Net_TCP_TxQ|DestAddr|Average number of TCP packets in the send buffer.|
|Net_TCP_RxQ|DestAddr|Average number of TCP packets in the receive buffer.|
|Net_TCP_Lost|DestAddr|Average number of unrecovered recurring timeouts. This number is reset when the recovery finishes or `SND.UNA` is advanced. `SND.UNA` is the sequence number of the first byte of data that has been sent, but not yet acknowledged.|
|Net_TCP_SendCWND|DestAddr|Average size (bytes) of the sending congestion window.|
|Net_TCP_SSThresh|DestAddr|Average size (bytes) of the slow start size threshold.|
|Net_PacketRate4|Direction|The total number of IPv4 datagrams transmitted/received from/by interfaces per second, including those transmitted or received in error|
|Net_PacketDropRate4|Direction|The total number of IPv4 datagrams transmitted or received in error per second.|
|Net_PacketRate6|Direction|The total number of IPv6 datagrams transmitted or received from or by interfaces per second, including those transmitted or received in error.|
|Net_PacketDropRate6|Direction|The total number of IPv6 datagrams transmitted or received in error per second.|
|Net_Throughput|Direction|The number of bytes of data transmitted or received per second by all network interfaces.|
|ThreadPool_QueueSize|ThreadPoolType|The size of the task queue.|
|ThreadPool_RejectedReqs|ThreadPoolType|The number of rejected executions.|
|ThreadPool_TotalThreads|ThreadPoolType|The current number of threads in the pool.|
|ThreadPool_ActiveThreads|ThreadPoolType|The approximate number of threads that are actively executing tasks.|
|Master_PendingQueueSize|N/A|The current number of pending tasks in the cluster state update thread. Each node has a cluster state update thread that submits cluster state update tasks (create index, update mapping, allocate shard, fail shard, etc.).|
|HTTP_RequestDocs|Operation, Exception, Indices, HTTPRespCode|The number of items in the request (only for `_bulk` request type).|
|HTTP_TotalRequests|Operation, Exception, Indices, HTTPRespCode|The number of finished requests in the past five seconds.|
|CB_EstimatedSize|CBType|The current number of estimated bytes.|
|CB_TrippedEvents|CBType|The number of times the circuit breaker has tripped.|
|CB_ConfiguredSize|CBType|The limit (bytes) for how much memory operations can use.|
