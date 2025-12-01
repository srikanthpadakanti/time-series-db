/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.metrics;

import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.Histogram;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TSDBIndexMetricsTests extends OpenSearchTestCase {
    private MetricsRegistry registry;
    private TSDBIndexMetrics metrics;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        registry = mock(MetricsRegistry.class);
        metrics = new TSDBIndexMetrics();

        // Mock all counter creations
        when(registry.createCounter(anyString(), anyString(), anyString())).thenReturn(mock(Counter.class));

        // Mock all histogram creations
        when(registry.createHistogram(anyString(), anyString(), anyString())).thenReturn(mock(Histogram.class));
    }

    public void testInitializeCreatesAllCounters() {
        metrics.initialize(registry);

        // Verify all counters are created
        assertNotNull(metrics.indexCreatedTotal);
        assertNotNull(metrics.retentionSuccessTotal);
        assertNotNull(metrics.retentionFailureTotal);
        assertNotNull(metrics.compactionSuccessTotal);
        assertNotNull(metrics.compactionFailureTotal);
        assertNotNull(metrics.compactionDeletedTotal);

        // Verify registry calls for counters (6 counters total)
        verify(registry, times(6)).createCounter(anyString(), anyString(), anyString());
    }

    public void testInitializeCreatesAllHistograms() {
        metrics.initialize(registry);

        // Verify all histograms are created
        assertNotNull(metrics.indexSize);
        assertNotNull(metrics.indexOnlineAge);
        assertNotNull(metrics.indexOfflineAge);
        assertNotNull(metrics.retentionLatency);
        assertNotNull(metrics.retentionAge);
        assertNotNull(metrics.compactionLatency);

        // Verify registry calls for histograms (6 histograms total)
        verify(registry, times(6)).createHistogram(anyString(), anyString(), anyString());
    }

    public void testCleanupResetsAllMetrics() {
        metrics.initialize(registry);

        metrics.cleanup();

        // Verify all counters are reset to null
        assertNull(metrics.indexCreatedTotal);
        assertNull(metrics.retentionSuccessTotal);
        assertNull(metrics.retentionFailureTotal);
        assertNull(metrics.compactionSuccessTotal);
        assertNull(metrics.compactionFailureTotal);
        assertNull(metrics.compactionDeletedTotal);

        // Verify all histograms are reset to null
        assertNull(metrics.indexSize);
        assertNull(metrics.indexOnlineAge);
        assertNull(metrics.indexOfflineAge);
        assertNull(metrics.retentionLatency);
        assertNull(metrics.retentionAge);
        assertNull(metrics.compactionLatency);
    }

    public void testCleanupSafeWithoutInitialization() {
        // Should not throw when cleanup called before initialize
        metrics.cleanup();

        assertNull(metrics.indexCreatedTotal);
        assertNull(metrics.indexSize);
    }

    public void testCleanupIdempotent() {
        metrics.initialize(registry);

        metrics.cleanup();
        metrics.cleanup(); // Second call should not throw

        assertNull(metrics.indexCreatedTotal);
        assertNull(metrics.indexSize);
    }
}
