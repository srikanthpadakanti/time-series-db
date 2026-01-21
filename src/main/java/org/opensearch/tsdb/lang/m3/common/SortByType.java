/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.common;

public enum SortByType {
    /**
     * Sum sortBy type.
     */
    SUM("sum"),

    /**
     * Average sortBy type.
     */
    AVG("avg"),

    /**
     * Current sortBy type.
     */
    CURRENT("current"),

    /**
     * Minimum sortBy type.
     */
    MIN("min"),

    /**
     * Maximum sortBy type.
     */
    MAX("max"),
    /**
     * Standard Deviation sortBy type.
     */
    STDDEV("stddev"),

    /**
     * Name sortBy type.
     */
    NAME("name");

    private final String value;

    SortByType(String value) {
        this.value = value;
    }

    /**
     * Gets the string value for this sort by type.
     * @return The lowercase string value (e.g., "avg", "max", "sum")
     */
    public String getValue() {
        return value;
    }

    /**
     * Converts a string representation of a sort by type to the corresponding enum value.
     * @param sortByType The string representation of the sortBy type.
     * @return The corresponding sortByType enum value.
     * @throws IllegalArgumentException if the input string does not match any known sortBy type.
     */
    public static SortByType fromString(String sortByType) {
        switch (sortByType) {
            case Constants.Functions.Sort.AVG, Constants.Functions.Sort.AVERAGE:
                return AVG;
            case Constants.Functions.Sort.CURRENT:
                return CURRENT;
            case Constants.Functions.Sort.MAX, Constants.Functions.Sort.MAXIMUM:
                return MAX;
            case Constants.Functions.Sort.MIN, Constants.Functions.Sort.MINIMUM:
                return MIN;
            case Constants.Functions.Sort.SUM:
                return SUM;
            case Constants.Functions.Sort.STD_DEV:
                return STDDEV;
            case Constants.Functions.Sort.NAME:
                return NAME;
            default:
                throw new IllegalArgumentException(
                    "Invalid sortby type: " + sortByType + ", Supported: avg, current, max, min, name, stddev, sum"
                );
        }
    }
}
