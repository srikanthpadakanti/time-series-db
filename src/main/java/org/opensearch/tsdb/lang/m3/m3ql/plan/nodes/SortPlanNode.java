/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import org.opensearch.tsdb.lang.m3.common.SortByType;
import org.opensearch.tsdb.lang.m3.common.SortOrderType;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ValueNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.M3PlannerContext;
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

import java.util.Locale;

/**
 * SortPlanNode represents a plan node that handles sort operations in M3QL.
 *
 * The sort function sorts time series by avg, current, max, min, name, stddev, or sum.
 * It takes an optional second argument for sort order (asc or desc, defaulting to desc).
 *
 * This is a global aggregation that can only be executed at the coordinator aggregator,
 * similar to histogramPercentile.
 */
public class SortPlanNode extends M3PlanNode {
    private final SortByType sortBy;
    private final SortOrderType sortOrder;

    /**
     * Constructor for SortPlanNode.
     *
     * @param id        The node ID
     * @param sortBy    The sorting function (avg, current, max, min, name, stddev, sum)
     * @param sortOrder The sorting order (asc, desc)
     */
    public SortPlanNode(int id, SortByType sortBy, SortOrderType sortOrder) {
        super(id);
        this.sortBy = sortBy;
        this.sortOrder = sortOrder;
    }

    @Override
    public <T> T accept(M3PlanVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExplainName() {
        return String.format(Locale.ROOT, "SORT(%s, %s)", sortBy.getValue(), sortOrder.getValue());
    }

    /**
     * Returns the sorting function.
     *
     * @return The sorting function (avg, current, max, min, name, stddev, sum)
     */
    public SortByType getSortBy() {
        return sortBy;
    }

    /**
     * Returns the sorting order.
     *
     * @return The sorting order (asc, desc)
     */
    public SortOrderType getSortOrder() {
        return sortOrder;
    }

    /**
     * Creates a SortPlanNode from a FunctionNode.
     * Expected format:
     * - sort(avg) -> defaults to desc
     * - sort(avg, desc)
     * - sort(max, asc)
     * - sort(sum)
     *
     * @param functionNode The function node to parse
     * @return SortPlanNode instance
     * @throws IllegalArgumentException if the function arguments are invalid
     */
    public static SortPlanNode of(FunctionNode functionNode) {
        SortByType sortBy = getSortBy(functionNode);

        // Second argument (optional): sort order
        SortOrderType sortOrder = SortOrderType.DESC; // Default
        if (functionNode.getChildren().size() > 1) {
            if (!(functionNode.getChildren().get(1) instanceof ValueNode sortOrderValue)) {
                throw new IllegalArgumentException("Sort order argument must be a value (asc, desc)");
            }
            sortOrder = SortOrderType.fromString(sortOrderValue.getValue());
        }

        if (functionNode.getChildren().size() > 2) {
            throw new IllegalArgumentException("sort function accepts at most 2 arguments: sort function and order");
        }

        return new SortPlanNode(M3PlannerContext.generateId(), sortBy, sortOrder);
    }

    private static SortByType getSortBy(FunctionNode functionNode) {
        if (functionNode.getChildren().isEmpty()) {
            return SortByType.CURRENT;
        }

        // First argument: sort function
        if (!(functionNode.getChildren().getFirst() instanceof ValueNode valueNode)) {
            throw new IllegalArgumentException("Sort function argument must be a value (avg, current, max, min, name, stddev, sum)");
        }
        return SortByType.fromString(valueNode.getValue());
    }
}
