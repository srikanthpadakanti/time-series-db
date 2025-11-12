/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.framework.models;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

/**
 * Cluster configuration for time series testing.
 *
 * <p>This configuration is used by the framework to dynamically create the test cluster
 * during the setup phase. The framework will start the specified number of nodes before
 * running tests.
 *
 * <h3>Usage Example:</h3>
 *
 * <h4>Single Node Cluster (Default):</h4>
 * <pre>{@code
 * cluster_config:
 *   nodes: 1
 * }</pre>
 *
 * <h4>Multi-Node Cluster:</h4>
 * <pre>{@code
 * cluster_config:
 *   nodes: 3
 *   settings:
 *     cluster.routing.allocation.disk.threshold_enabled: false
 *     cluster.routing.rebalance.enable: "all"
 * }</pre>
 *
 * <p><strong>Note:</strong> Test classes must use {@code @ClusterScope} with {@code scope=TEST}
 * and {@code numDataNodes=0} to allow the framework to dynamically create nodes:
 * <pre>{@code
 * @ClusterScope(scope = Scope.TEST, numDataNodes = 0)
 * public class MyTest extends TimeSeriesTestFramework {
 *     // Framework will start nodes based on YAML cluster_config
 * }
 * }</pre>
 *
 * @param nodes Number of data nodes to create in the cluster (default: 1)
 * @param settings Optional cluster-level settings to apply to all nodes
 */
public record ClusterConfig(@JsonProperty("nodes") Integer nodes, @JsonProperty("settings") Map<String, Object> settings) {

    /**
     * Default single-node cluster configuration.
     */
    public static ClusterConfig singleNode() {
        return new ClusterConfig(1, null);
    }

    /**
     * Creates a multi-node cluster configuration.
     *
     * @param nodes Number of data nodes
     * @return A new ClusterConfig for a multi-node cluster
     */
    public static ClusterConfig multiNode(int nodes) {
        return new ClusterConfig(nodes, null);
    }

    /**
     * Returns the number of nodes, defaulting to 1 if not specified.
     */
    public int getNodes() {
        return nodes != null ? nodes : 1;
    }

    /**
     * Returns true if this is a multi-node configuration.
     */
    public boolean isMultiNode() {
        return getNodes() > 1;
    }
}
