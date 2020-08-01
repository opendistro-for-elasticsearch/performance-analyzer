package com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.ESResources;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.admin.indices.stats.IndexShardStats;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;

public class NodeStatsUtils {

    public NodeStatsUtils() {

    }

    /**
     * This function is copied directly from IndicesService.java in elastic search as the original function is not public
     * we need to collect stats per shard based instead of calling the stat() function to fetch all at once(which increases
     * cpu usage on data nodes dramatically).
     * @param indicesService Indices Services which keeps tracks of the indexes on the node
     * @param indexShard Shard to fetch the metrics for
     * @param flags  The Metrics Buckets which needs to be fetched.
     * @return stats given in the flags param for the shard given in the indexShard param.
     */
    public static IndexShardStats indexShardStats(final IndicesService indicesService, final IndexShard indexShard,
                                            final CommonStatsFlags flags) {
        if (indexShard.routingEntry() == null) {
            return null;
        }

        return new IndexShardStats(
                indexShard.shardId(),
                new ShardStats[]{
                        new ShardStats(
                                indexShard.routingEntry(),
                                indexShard.shardPath(),
                                new CommonStats(indicesService.getIndicesQueryCache(), indexShard, flags),
                                null,
                                null,
                                null)
                });
    }

    public static HashMap<String, IndexShard> getShards() {
        HashMap<String, IndexShard> shards =  new HashMap<>();
        Iterator<IndexService> indexServices = ESResources.INSTANCE.getIndicesService().iterator();
        while (indexServices.hasNext()) {
            Iterator<IndexShard> indexShards = indexServices.next().iterator();
            while (indexShards.hasNext()) {
                IndexShard shard = indexShards.next();
                shards.put(getUniqueShardIdKey(shard.shardId()), shard);
            }
        }
        return shards;
    }

    public static String getUniqueShardIdKey(ShardId shardId) {
        return "[" + shardId.getIndex().getUUID() + "][" + shardId.getId() + "]";
    }

    public static final EnumSet<IndexShardState> CAN_WRITE_INDEX_BUFFER_STATES = EnumSet.of(
            IndexShardState.RECOVERING, IndexShardState.POST_RECOVERY, IndexShardState.STARTED);
}
