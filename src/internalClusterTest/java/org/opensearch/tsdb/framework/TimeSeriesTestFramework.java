/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.framework;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.action.admin.indices.stats.IndexShardStats;
import org.opensearch.action.admin.indices.stats.IndexStats;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.tsdb.core.mapping.Constants;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.framework.models.IndexConfig;
import org.opensearch.tsdb.framework.models.InputDataConfig;
import org.opensearch.tsdb.framework.models.TestCase;
import org.opensearch.tsdb.framework.models.TestSetup;
import org.opensearch.tsdb.framework.models.TimeSeriesSample;
import org.opensearch.tsdb.utils.TSDBTestUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Base framework for time series testing
 * Extends OpenSearch's integration test infrastructure
 *
 * <p>This framework uses TSDBDocument format for ingestion to minimize duplication with TSDBEngine.
 * The TSDBDocument format is:
 * <pre>
 * {
 *   "labels": "name http_requests method GET status 200",  // space-separated key-value pairs
 *   "timestamp": 1234567890,                               // epoch millis
 *   "value": 100.5                                          // double value
 * }
 * </pre>
 *
 * <p>This format directly aligns with how TSDBEngine parses and indexes data, ensuring consistency
 * between test data ingestion and production data flow.
 *
 * <p>Index mapping is obtained directly from {@link Constants.Mapping#DEFAULT_INDEX_MAPPING} to ensure
 * it matches the actual TSDB engine mapping.
 *
 * <h2>Cluster Configuration</h2>
 * <p>This framework uses YAML-driven cluster creation. The {@code @ClusterScope} annotation is configured
 * to allow dynamic node creation based on YAML configuration:
 * <ul>
 *   <li>{@code scope=TEST} - Fresh cluster for each test method (ensures isolation)</li>
 *   <li>{@code numDataNodes=0} - No nodes started by default (framework starts them from YAML)</li>
 * </ul>
 *
 * <p>Specify the number of nodes in your YAML file:
 * <pre>{@code
 * cluster_config:
 *   nodes: 3  # Framework will start 3 data nodes
 * }</pre>
 */
@SuppressWarnings("unchecked")
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0, supportsDedicatedMasters = false, autoManageMasterNodes = true)
public abstract class TimeSeriesTestFramework extends OpenSearchIntegTestCase {

    // Default index settings as YAML constant
    // Note: Mapping is obtained directly from Constants.Mapping.DEFAULT_INDEX_MAPPING
    private static final String DEFAULT_INDEX_SETTINGS_YAML = """
        index.refresh_interval: "1s"
        index.tsdb_engine.enabled: true
        index.queries.cache.enabled: false
        index.requests.cache.enable: false
        """;

    protected TestSetup testSetup;
    protected TestCase testCase;
    protected SearchQueryExecutor queryExecutor;
    protected List<IndexConfig> indexConfigs;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // Note: Test methods should call loadTestConfigurationFromFile() before running tests
        // This allows different test methods to use different YAML configurations
    }

    /**
     * Loads test configuration from YAML file and initializes the test cluster.
     * This method should be called at the beginning of each test method.
     *
     * @param yamlFilePath Path to the YAML test configuration file
     * @throws Exception if configuration loading or cluster setup fails
     */
    protected void loadTestConfigurationFromFile(String yamlFilePath) throws Exception {
        testSetup = YamlLoader.loadTestSetup(yamlFilePath);
        testCase = YamlLoader.loadTestCase(yamlFilePath);

        startClusterNodes();
        initializeComponents();
        clearIndexIfExists();
    }

    /**
     * Starts cluster nodes based on the cluster configuration from YAML.
     * If cluster_config.nodes is specified, starts that many nodes.
     * Otherwise, starts a single node by default.
     */
    private void startClusterNodes() throws Exception {
        int nodesToStart = 1; // default

        if (testSetup != null && testSetup.clusterConfig() != null) {
            nodesToStart = testSetup.clusterConfig().getNodes();
        }

        if (nodesToStart > 0) {
            internalCluster().startNodes(nodesToStart);
            // Ensure cluster has formed with all nodes
            ensureStableCluster(nodesToStart);
        }
    }

    private void initializeComponents() {
        // Create the index config by merging test config with framework defaults
        try {
            // Parse the default settings YAML
            ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
            Map<String, Object> defaultSettings = yamlMapper.readValue(DEFAULT_INDEX_SETTINGS_YAML, Map.class);

            // Get the actual TSDB mapping from Constants (same as engine uses)
            // This ensures test mapping matches production mapping
            Map<String, Object> defaultMapping = parseMappingFromConstants();

            // Initialize index configs list
            indexConfigs = new ArrayList<>();

            // Validate that test setup and index configs are present
            if (testSetup == null) {
                throw new IllegalStateException("Test setup is required but was null");
            }

            if (testSetup.indexConfigs() == null || testSetup.indexConfigs().isEmpty()) {
                throw new IllegalStateException("Test setup must specify at least one index configuration in index_configs");
            }

            // Validate and create index configs
            for (IndexConfig testIndexConfig : testSetup.indexConfigs()) {
                if (testIndexConfig.name() == null || testIndexConfig.name().trim().isEmpty()) {
                    throw new IllegalArgumentException("Index configuration must specify a non-empty index name");
                }

                String indexName = testIndexConfig.name();
                int shards = testIndexConfig.shards();
                int replicas = testIndexConfig.replicas();
                indexConfigs.add(new IndexConfig(indexName, shards, replicas, defaultSettings, defaultMapping));
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to create index config", e);
        }

        // Initialize query executor after successful config creation
        queryExecutor = new SearchQueryExecutor(client());
    }

    /**
     * Parse the TSDB engine's default mapping from Constants.
     * This ensures we use the exact same mapping as the engine.
     */
    private Map<String, Object> parseMappingFromConstants() throws IOException {
        ObjectMapper jsonMapper = new ObjectMapper();
        String mappingJson = Constants.Mapping.DEFAULT_INDEX_MAPPING.trim();
        return jsonMapper.readValue(mappingJson, Map.class);
    }

    protected void runBasicTest() throws Exception {
        ingestTestData();
        executeAndValidateQueries();
    }

    protected void ingestTestData() throws Exception {

        // Create all indices
        for (IndexConfig indexConfig : indexConfigs) {
            createTimeSeriesIndex(indexConfig);
        }

        // Ingest data into respective indices
        if (testCase.inputDataList() != null && !testCase.inputDataList().isEmpty()) {
            for (InputDataConfig inputDataConfig : testCase.inputDataList()) {
                List<TimeSeriesSample> samples = TimeSeriesSampleGenerator.generateSamples(inputDataConfig);
                ingestSamples(samples, inputDataConfig.indexName());
            }
        }

        // Ensure all indices are green and refreshed
        for (IndexConfig indexConfig : indexConfigs) {
            ensureGreen(indexConfig.name());
            refresh(indexConfig.name());
        }
    }

    protected void executeAndValidateQueries() throws Exception {
        queryExecutor.executeAndValidateQueries(testCase);
    }

    protected void clearIndexIfExists() throws Exception {
        for (IndexConfig indexConfig : indexConfigs) {
            String indexName = indexConfig.name();
            if (client().admin().indices().prepareExists(indexName).get().isExists()) {
                client().admin().indices().prepareDelete(indexName).get();
                // Wait for the index to be fully deleted
                assertBusy(
                    () -> { assertFalse("Index should be deleted", client().admin().indices().prepareExists(indexName).get().isExists()); }
                );
            }
        }
    }

    protected void createTimeSeriesIndex(IndexConfig indexConfig) throws Exception {
        // Merge default settings with index configuration
        Map<String, Object> allSettings = new HashMap<>(indexConfig.settings());
        allSettings.put("index.number_of_shards", indexConfig.shards());
        allSettings.put("index.number_of_replicas", indexConfig.replicas());

        Settings settings = Settings.builder().loadFromMap(allSettings).build();

        Map<String, Object> mappingConfig = indexConfig.mapping();

        client().admin().indices().prepareCreate(indexConfig.name()).setSettings(settings).setMapping(mappingConfig).get();
    }

    /**
     * Ingest time series samples using TSDBDocument format.
     * This method minimizes duplication with TSDBEngine by using the same document format
     * that TSDBEngine expects and parses.
     *
     * <p>The TSDBDocument format used here is:
     * <pre>
     * {
     *   "labels": "name http_requests method GET status 200",  // space-separated key-value pairs
     *   "timestamp": 1234567890,                               // epoch millis
     *   "value": 100.5                                          // double value
     * }
     * </pre>
     *
     * <p>This directly maps to how TSDBEngine's TSDBDocument.fromParsedDocument() expects data,
     * ensuring that test ingestion follows the same code path as production ingestion.
     *
     * @param samples The list of samples to ingest
     * @param indexName The name of the index to ingest data into
     * @throws Exception if ingestion fails
     */
    protected void ingestSamples(List<TimeSeriesSample> samples, String indexName) throws Exception {
        // Use bulk request for better performance, but still one document per sample
        BulkRequest bulkRequest = new BulkRequest();

        // Send one document per sample to match production data shape
        for (TimeSeriesSample sample : samples) {
            Map<String, String> labels = sample.labels();
            ByteLabels byteLabels = ByteLabels.fromMap(labels);

            // Create TSDBDocument format JSON using the utility method
            // This ensures consistency with TSDBEngine's document parsing
            String documentJson = TSDBTestUtils.createTSDBDocumentJson(sample);
            String seriesId = byteLabels.toString();

            IndexRequest request = new IndexRequest(indexName).source(documentJson, XContentType.JSON).routing(seriesId);
            bulkRequest.add(request);
        }

        // Execute bulk request
        if (bulkRequest.numberOfActions() > 0) {
            BulkResponse bulkResponse = client().bulk(bulkRequest).actionGet();
            if (bulkResponse.hasFailures()) {
                throw new RuntimeException("Bulk ingestion failed: " + bulkResponse.buildFailureMessage());
            }
        }

        client().admin().indices().prepareFlush(indexName).get();
        client().admin().indices().prepareRefresh(indexName).get();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder settingsBuilder = Settings.builder().put(super.nodeSettings(nodeOrdinal));

        if (testSetup != null && testSetup.nodeSettings() != null) {
            for (Map.Entry<String, Object> entry : testSetup.nodeSettings().entrySet()) {
                settingsBuilder.put(entry.getKey(), entry.getValue().toString());
            }
        }

        return settingsBuilder.build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return super.nodePlugins();
    }

    @Override
    protected boolean addMockInternalEngine() {
        // Disable MockEngineFactoryPlugin to avoid conflicts with TSDBEngine
        return false;
    }

    /**
     * Validates that shards are properly distributed across nodes.
     * Useful for verifying multi-shard and multi-node test configurations.
     *
     * @param indexConfig The index configuration to validate
     * @throws AssertionError if shard distribution is invalid
     */
    protected void validateShardDistribution(IndexConfig indexConfig) {
        String indexName = indexConfig.name();
        int expectedShards = indexConfig.shards();
        int expectedReplicas = indexConfig.replicas();

        ClusterStateResponse stateResponse = client().admin().cluster().prepareState().setIndices(indexName).get();

        IndexRoutingTable indexRoutingTable = stateResponse.getState().routingTable().index(indexName);

        assertNotNull("Index routing table should exist", indexRoutingTable);
        assertEquals("Number of shards mismatch", expectedShards, indexRoutingTable.shards().size());

        for (int shardId = 0; shardId < expectedShards; shardId++) {
            IndexShardRoutingTable shardRoutingTable = indexRoutingTable.shard(shardId);
            assertNotNull("Shard routing table should exist for shard " + shardId, shardRoutingTable);

            int expectedCopies = 1 + expectedReplicas; // primary + replicas
            assertEquals("Shard " + shardId + " should have " + expectedCopies + " copies", expectedCopies, shardRoutingTable.size());

            // Check that all shard copies are active
            for (ShardRouting shardRouting : shardRoutingTable) {
                assertTrue("Shard " + shardId + " should be active: " + shardRouting, shardRouting.active());
            }
        }

    }

    /**
     * Gets statistics about document distribution across shards.
     * Useful for verifying that data is actually distributed across all shards.
     *
     * @param indexConfig The index configuration to get stats for
     * @return Map from shard ID to document count on that shard
     */
    protected Map<Integer, Long> getShardDocumentCounts(IndexConfig indexConfig) throws Exception {
        String indexName = indexConfig.name();
        Map<Integer, Long> shardCounts = new HashMap<>();

        // Get stats for the index
        IndicesStatsResponse statsResponse = client().admin().indices().prepareStats(indexName).clear().setDocs(true).get();

        IndexStats indexStats = statsResponse.getIndex(indexName);
        if (indexStats == null) {
            return shardCounts;
        }

        // Iterate through all shard stats and get primary shard document counts
        for (IndexShardStats shardStats : indexStats.getIndexShards().values()) {
            int shardId = shardStats.getShardId().id();

            // Get primary shard stats
            for (ShardStats shard : shardStats.getShards()) {
                if (shard.getShardRouting().primary()) {
                    long docCount = shard.getStats().getDocs().getCount();
                    shardCounts.put(shardId, docCount);
                    break;
                }
            }
        }

        return shardCounts;
    }

    /**
     * Validates that data is distributed across all shards.
     * This ensures that your test data actually uses all available shards.
     *
     * @param indexConfig The index configuration to validate
     * @param minDocsPerShard Minimum number of documents expected per shard (use 1 to ensure all shards have data)
     * @throws AssertionError if any shard has fewer than minDocsPerShard documents
     */
    protected void validateDataDistribution(IndexConfig indexConfig, int minDocsPerShard) throws Exception {
        Map<Integer, Long> shardCounts = getShardDocumentCounts(indexConfig);
        int expectedShards = indexConfig.shards();

        assertEquals("Should have document counts for all shards", expectedShards, shardCounts.size());

        for (int shardId = 0; shardId < expectedShards; shardId++) {
            long count = shardCounts.getOrDefault(shardId, 0L);
            assertTrue("Shard " + shardId + " has " + count + " documents, expected at least " + minDocsPerShard, count >= minDocsPerShard);
        }

    }

    /**
     * Validates that ALL shards have at least some data.
     * Convenience method that calls validateDataDistribution(1).
     *
     * @param indexConfig The index configuration to validate
     * @throws AssertionError if any shard is empty
     */
    protected void validateAllShardsHaveData(IndexConfig indexConfig) throws Exception {
        validateDataDistribution(indexConfig, 1);
    }

}
