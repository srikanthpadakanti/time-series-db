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
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

/**
 * Unit tests for SortPlanNode.
 */
public class SortPlanNodeTests extends BasePlanNodeTests {

    public void testSortPlanNodeCreation() {
        SortPlanNode node = new SortPlanNode(1, SortByType.AVG, SortOrderType.DESC);

        assertEquals(1, node.getId());
        assertEquals(SortByType.AVG, node.getSortBy());
        assertEquals(SortOrderType.DESC, node.getSortOrder());
        assertEquals("SORT(avg, desc)", node.getExplainName());
        assertTrue(node.getChildren().isEmpty());
    }

    public void testSortPlanNodeWithAscendingOrder() {
        SortPlanNode node = new SortPlanNode(1, SortByType.MAX, SortOrderType.ASC);

        assertEquals(SortByType.MAX, node.getSortBy());
        assertEquals(SortOrderType.ASC, node.getSortOrder());
        assertEquals("SORT(max, asc)", node.getExplainName());
    }

    public void testSortPlanNodeVisitorAccept() {
        SortPlanNode node = new SortPlanNode(1, SortByType.SUM, SortOrderType.DESC);
        TestMockVisitor visitor = new TestMockVisitor();

        String result = node.accept(visitor);
        assertEquals("visit SortPlanNode", result);
    }

    public void testSortPlanNodeFactoryMethodWithBothArguments() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("sort");
        functionNode.addChildNode(new ValueNode("max"));
        functionNode.addChildNode(new ValueNode("asc"));

        SortPlanNode node = SortPlanNode.of(functionNode);

        assertEquals(SortByType.MAX, node.getSortBy());
        assertEquals(SortOrderType.ASC, node.getSortOrder());
    }

    public void testSortPlanNodeFactoryMethodWithDefaultOrder() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("sort");
        functionNode.addChildNode(new ValueNode("sum"));

        SortPlanNode node = SortPlanNode.of(functionNode);

        assertEquals(SortByType.SUM, node.getSortBy());
        assertEquals(SortOrderType.DESC, node.getSortOrder());
    }

    public void testSortPlanNodeFactoryMethodWithAllValidSortFunctions() {
        String[] validSortFunctions = { "avg", "current", "max", "min", "name", "stddev", "sum" };
        SortByType[] expectedSortByTypes = {
            SortByType.AVG,
            SortByType.CURRENT,
            SortByType.MAX,
            SortByType.MIN,
            SortByType.NAME,
            SortByType.STDDEV,
            SortByType.SUM };

        for (int i = 0; i < validSortFunctions.length; i++) {
            FunctionNode functionNode = new FunctionNode();
            functionNode.setFunctionName("sort");
            functionNode.addChildNode(new ValueNode(validSortFunctions[i]));

            SortPlanNode node = SortPlanNode.of(functionNode);
            assertEquals(expectedSortByTypes[i], node.getSortBy());
            assertEquals(SortOrderType.DESC, node.getSortOrder());
        }
    }

    public void testSortPlanNodeFactoryMethodWithAllValidSortOrders() {
        String[] validOrders = { "asc", "desc" };
        SortOrderType[] expectedOrderTypes = { SortOrderType.ASC, SortOrderType.DESC };

        for (int i = 0; i < validOrders.length; i++) {
            FunctionNode functionNode = new FunctionNode();
            functionNode.setFunctionName("sort");
            functionNode.addChildNode(new ValueNode("avg"));
            functionNode.addChildNode(new ValueNode(validOrders[i]));

            SortPlanNode node = SortPlanNode.of(functionNode);
            assertEquals(SortByType.AVG, node.getSortBy());
            assertEquals(expectedOrderTypes[i], node.getSortOrder());
        }
    }

    public void testSortPlanNodeFactoryMethodWithNameSortFunction() {
        // Test name sort with default desc order
        FunctionNode functionNode1 = new FunctionNode();
        functionNode1.setFunctionName("sort");
        functionNode1.addChildNode(new ValueNode("name"));

        SortPlanNode node1 = SortPlanNode.of(functionNode1);
        assertEquals(SortByType.NAME, node1.getSortBy());
        assertEquals(SortOrderType.DESC, node1.getSortOrder());

        // Test name sort with explicit asc order
        FunctionNode functionNode2 = new FunctionNode();
        functionNode2.setFunctionName("sort");
        functionNode2.addChildNode(new ValueNode("name"));
        functionNode2.addChildNode(new ValueNode("asc"));

        SortPlanNode node2 = SortPlanNode.of(functionNode2);
        assertEquals(SortByType.NAME, node2.getSortBy());
        assertEquals(SortOrderType.ASC, node2.getSortOrder());
    }

    public void testSortPlanNodeFactoryMethodWithNoArgumentsDefaultsToCurrent() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("sort");

        SortPlanNode node = SortPlanNode.of(functionNode);

        // Should default to CURRENT when no arguments provided
        assertEquals(SortByType.CURRENT, node.getSortBy());
        assertEquals(SortOrderType.DESC, node.getSortOrder());
    }

    public void testSortPlanNodeFactoryMethodThrowsOnInvalidSortFunction() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("sort");
        functionNode.addChildNode(new ValueNode("invalid"));

        expectThrows(IllegalArgumentException.class, () -> SortPlanNode.of(functionNode));
    }

    public void testSortPlanNodeFactoryMethodThrowsOnInvalidSortOrder() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("sort");
        functionNode.addChildNode(new ValueNode("avg"));
        functionNode.addChildNode(new ValueNode("invalid"));

        expectThrows(IllegalArgumentException.class, () -> SortPlanNode.of(functionNode));
    }

    public void testSortPlanNodeFactoryMethodThrowsOnTooManyArguments() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("sort");
        functionNode.addChildNode(new ValueNode("avg"));
        functionNode.addChildNode(new ValueNode("desc"));
        functionNode.addChildNode(new ValueNode("extra"));

        expectThrows(IllegalArgumentException.class, () -> SortPlanNode.of(functionNode));
    }

    public void testSortPlanNodeFactoryMethodThrowsOnNonValueNode() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("sort");
        functionNode.addChildNode(new FunctionNode()); // not a value node

        expectThrows(IllegalArgumentException.class, () -> SortPlanNode.of(functionNode));
    }

    public void testSortPlanNodeFactoryMethodThrowsOnNonValueNodeForSortOrder() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("sort");
        functionNode.addChildNode(new ValueNode("avg"));
        functionNode.addChildNode(new FunctionNode()); // not a value node

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> SortPlanNode.of(functionNode));
        assertEquals("Sort order argument must be a value (asc, desc)", exception.getMessage());
    }

    public void testSortPlanNodeFactoryMethodWithAlternativeNames() {
        // Test "average" instead of "avg"
        FunctionNode functionNode1 = new FunctionNode();
        functionNode1.setFunctionName("sort");
        functionNode1.addChildNode(new ValueNode("average"));
        SortPlanNode node1 = SortPlanNode.of(functionNode1);
        assertEquals(SortByType.AVG, node1.getSortBy());

        // Test "maximum" instead of "max"
        FunctionNode functionNode2 = new FunctionNode();
        functionNode2.setFunctionName("sort");
        functionNode2.addChildNode(new ValueNode("maximum"));
        SortPlanNode node2 = SortPlanNode.of(functionNode2);
        assertEquals(SortByType.MAX, node2.getSortBy());

        // Test "minimum" instead of "min"
        FunctionNode functionNode3 = new FunctionNode();
        functionNode3.setFunctionName("sort");
        functionNode3.addChildNode(new ValueNode("minimum"));
        SortPlanNode node3 = SortPlanNode.of(functionNode3);
        assertEquals(SortByType.MIN, node3.getSortBy());

        // Test "ascending" instead of "asc"
        FunctionNode functionNode4 = new FunctionNode();
        functionNode4.setFunctionName("sort");
        functionNode4.addChildNode(new ValueNode("avg"));
        functionNode4.addChildNode(new ValueNode("ascending"));
        SortPlanNode node4 = SortPlanNode.of(functionNode4);
        assertEquals(SortOrderType.ASC, node4.getSortOrder());

        // Test "descending" instead of "desc"
        FunctionNode functionNode5 = new FunctionNode();
        functionNode5.setFunctionName("sort");
        functionNode5.addChildNode(new ValueNode("sum"));
        functionNode5.addChildNode(new ValueNode("descending"));
        SortPlanNode node5 = SortPlanNode.of(functionNode5);
        assertEquals(SortOrderType.DESC, node5.getSortOrder());
    }

    public void testGetExplainNameWithNewSortTypes() {
        SortPlanNode currentNode = new SortPlanNode(1, SortByType.CURRENT, SortOrderType.DESC);
        assertEquals("SORT(current, desc)", currentNode.getExplainName());

        SortPlanNode minNode = new SortPlanNode(2, SortByType.MIN, SortOrderType.ASC);
        assertEquals("SORT(min, asc)", minNode.getExplainName());

        SortPlanNode stddevNode = new SortPlanNode(3, SortByType.STDDEV, SortOrderType.DESC);
        assertEquals("SORT(stddev, desc)", stddevNode.getExplainName());
    }

    public void testInvalidSortFunctionErrorMessage() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("sort");
        functionNode.addChildNode(new ValueNode("invalid"));

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> SortPlanNode.of(functionNode));
        assertTrue(exception.getMessage().contains("Invalid sortby type"));
        assertTrue(exception.getMessage().contains("avg, current, max, min, name, stddev, sum"));
    }

    public void testInvalidSortOrderErrorMessage() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("sort");
        functionNode.addChildNode(new ValueNode("avg"));
        functionNode.addChildNode(new ValueNode("invalid"));

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> SortPlanNode.of(functionNode));
        assertTrue(exception.getMessage().contains("Invalid sort order type"));
        assertTrue(exception.getMessage().contains("asc, ascending, desc, descending"));
    }

    private static class TestMockVisitor extends M3PlanVisitor<String> {
        @Override
        public String process(M3PlanNode planNode) {
            return "process called";
        }

        @Override
        public String visit(SortPlanNode planNode) {
            return "visit SortPlanNode";
        }
    }
}
