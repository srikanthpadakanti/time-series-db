/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.utils;

import org.opensearch.common.time.DateFormatter;
import org.opensearch.common.time.DateMathParser;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for parsing timestamp strings.
 * Supports relative timestamps (now, now-1h, now+30m) and absolute ISO-8601 timestamps.
 *
 */
public class TimestampUtils {

    // Use OpenSearch's default date formatter for consistent parsing
    private static final DateMathParser DATE_MATH_PARSER = DateFormatter.forPattern("strict_date_optional_time||epoch_millis")
        .toDateMathParser();

    /**
     * Parse a timestamp string relative to the current time using OpenSearch's DateMathParser.
     *
     * WARNING: This method calls Instant.now() each time it's invoked. For test code that parses
     * multiple timestamps with relative offsets (e.g., "now-50m", "now-40m"), prefer using
     * parseTimestampRelativeTo() with a shared base time to avoid flakiness from time drift.
     *
     * The TimestampDeserializer used by YamlLoader automatically handles this by using a shared
     * reference time for all timestamps in a single YAML file.
     *
     * @param timestampStr The timestamp string to parse (e.g., "now", "now-1h", "now+30m", "2023-01-01T00:00:00Z")
     * @return The parsed Instant
     * @throws IllegalArgumentException if the format is invalid
     */
    public static Instant parseTimestamp(String timestampStr) {
        return parseTimestampRelativeTo(timestampStr, Instant.now());
    }

    /**
     * Parse a timestamp string relative to a given base time using OpenSearch's DateMathParser.
     * This allows for consistent timestamp calculation across multiple calls and makes testing easier.
     *
     * @param timestampStr The timestamp string to parse (e.g., "now", "now-1h", "now+30m", "2023-01-01T00:00:00Z")
     * @param baseTime The base time to use for relative calculations
     * @return Instant representing the timestamp
     * @throws IllegalArgumentException if the format is invalid
     */
    public static Instant parseTimestampRelativeTo(String timestampStr, Instant baseTime) {
        if (timestampStr == null || timestampStr.trim().isEmpty()) {
            throw new IllegalArgumentException("Timestamp string cannot be null or empty");
        }

        try {
            // The LongSupplier provides the base time in milliseconds for "now" calculations
            return DATE_MATH_PARSER.parse(timestampStr.trim(), () -> baseTime.toEpochMilli());
        } catch (Exception e) {
            throw new IllegalArgumentException(
                "Invalid timestamp format: '"
                    + timestampStr
                    + "'. Expected 'now', 'now-<duration>', 'now+<duration>', or ISO-8601 timestamp. "
                    + "Supported duration units: y, M, w, d, h/H, m, s",
                e
            );
        }
    }

    /**
     * Generate timestamp range based on start, end, and step
     * @param start Start timestamp (inclusive)
     * @param end End timestamp (inclusive)
     * @param step Step duration
     * @return List of timestamps from start to end with given step
     * @throws IllegalArgumentException if parameters are invalid
     */
    public static List<Instant> generateTimestampRange(Instant start, Instant end, Duration step) {
        if (start == null || end == null || step == null) {
            throw new IllegalArgumentException("Start, end, and step cannot be null");
        }

        if (step.isZero() || step.isNegative()) {
            throw new IllegalArgumentException("Step must be positive and non-zero");
        }

        if (start.isAfter(end)) {
            throw new IllegalArgumentException("Start timestamp cannot be after end timestamp");
        }

        // Calculate the number of timestamps to pre-allocate the list
        // TODO: Update this to use the appropriate time unit when we add time unit as a config
        long durationMillis = end.toEpochMilli() - start.toEpochMilli();
        long stepMillis = step.toMillis();
        int capacity = (int) (durationMillis / stepMillis) + 1;

        List<Instant> timestamps = new ArrayList<>(capacity);
        Instant current = start;

        while (!current.isAfter(end)) {
            timestamps.add(current);
            current = current.plus(step);
        }

        return timestamps;
    }

}
