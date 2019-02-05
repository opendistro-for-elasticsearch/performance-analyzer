/*
 * Copyright <2019> Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistro.performanceanalyzer.reader;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.NodeDetailColumns;
import com.amazon.opendistro.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.amazon.opendistro.performanceanalyzer.util.JsonConverter;

public class ClusterLevelMetricsReader {

    /**
     *  Almost the same as NodeDetailsCollector.NodeDetailsStatus.
     *  Consider keeping only one of them for easy maintenance.  Don't do it now
     *  as we may separate reader and writer code later and we don't want many
     *  refactoring before release.
     *
     */
    public static class NodeDetails {
        private String id;
        private String hostAddress;

        NodeDetails(String stringifiedMetrics) {
            Map<String, Object> map = JsonConverter
                    .createMapFrom(stringifiedMetrics);
            id = (String) map.get(NodeDetailColumns.ID.toString());
            hostAddress = (String) map.get(NodeDetailColumns.HOST_ADDRESS
                    .toString());
        }

        @Override
        public String toString() {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("{");
            stringBuilder.append("id:" + id);
            stringBuilder.append(" hostAddress:" + hostAddress);
            stringBuilder.append("}");
            return stringBuilder.toString();
        }

        public String getId() {
            return id;
        }

        public String getHostAddress() {
            return hostAddress;
        }
    }

    private static int sPollTimeInterval = 60000;
    private static final Logger LOG = LogManager.getLogger(ClusterLevelMetricsReader.class);
    private static int sBuckets = 60;

    private static NodeDetails[] nodesDetails = new NodeDetails[0];

    public static NodeDetails[] getNodes() {
        return nodesDetails;
    }

    public static void collectNodeMetrics(long startTime) throws Exception {
        String sNodesDetails = PerformanceAnalyzerMetrics.getMetric(startTime, PerformanceAnalyzerMetrics.sNodesPath);

        if(sNodesDetails != null) {
            String lines[] = sNodesDetails.split("\\r?\\n");


            if(lines.length < 2) {
                LOG.error("Skip parsing. Number of lines: {}.", lines.length);
                return;
            }

            NodeDetails[] tmpNodesDetails = new NodeDetails[lines.length-1];

            // line 0 is last modified time of the file

            tmpNodesDetails[0] = new NodeDetails(lines[1]);
            int tmpNodeDetailsIndex = 1;

            for(int i = 2; i < lines.length; i++) {
                NodeDetails tmp = new NodeDetails(lines[i]);

                if(!tmp.id.equals(tmpNodesDetails[0].id)) {
                    tmpNodesDetails[tmpNodeDetailsIndex++] = tmp;
                }
            }

            nodesDetails = tmpNodesDetails;
        }
    }
}
