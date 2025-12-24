/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.integrationTests;

import org.opensearch.tsdb.framework.RestTimeSeriesTestFramework;

/**
 * Integration test for multi-index scenarios with data migration.
 *
 * Tests query execution when data is split across multiple indices,
 * simulating scenarios where time series data is migrated between indices.
 * Validates that resolved partitions correctly route queries to appropriate
 * indices based on time windows.
 */
public class MultiIndexDataMigrationRestIT extends RestTimeSeriesTestFramework {

    private static final String TEST_YAML = "test_cases/multi_index_data_migration_rest_it.yaml";
    private static final String PUSHDOWN_COMPARISON_TEST_YAML = "test_cases/multi_index_pushdown_comparison_rest_it.yaml";
    private static final String OVERLAPPING_WINDOWS_TEST_YAML = "test_cases/multi_index_overlapping_windows_rest_it.yaml";

    /**
     * Tests moving window aggregation across data split between two indices.
     *
     * Validates:
     * - Data entirely in single index produces correct results
     * - Data migrated between indices (with resolved partitions) produces identical results
     * - Resolved partitions correctly route time windows to appropriate indices
     */
    public void testDataMigrationWithMovingSum() throws Exception {
        initializeTest(TEST_YAML);
        runBasicTest();
    }

    /**
     * Tests that queries with and without pushdown produce identical results across multiple indices.
     *
     * Validates:
     * - Simple sum aggregation works correctly with pushdown enabled
     * - Simple sum aggregation works correctly with pushdown disabled
     * - Both produce identical results matching expected values
     */
    public void testPushdownComparison() throws Exception {
        initializeTest(PUSHDOWN_COMPARISON_TEST_YAML);
        runBasicTest();
    }

    /**
     * Tests realistic multi-index scenario with overlapping time windows using generic metrics.
     *
     * Simulates data migration where:
     * - Old index contains data from 00:00 to 00:40
     * - New index contains data from 00:20 to 01:00
     * - During overlap (00:20-00:40), timestamps are split between indices
     * - Each timestamp exists in only one index (no duplicate timestamps)
     *
     * Validates:
     * - Queries with pushdown correctly merge data from both indices
     * - Queries without pushdown produce identical results
     * - Overlapping time windows are handled correctly
     */
    public void testOverlappingTimeWindows() throws Exception {
        initializeTest(OVERLAPPING_WINDOWS_TEST_YAML);
        runBasicTest();
    }
}
