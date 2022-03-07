package org.apache.cassandra.spark.utils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import org.apache.spark.sql.sources.In;
import org.junit.Test;

import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

public class FilterUtilsTests
{
    @Test(expected = IllegalArgumentException.class)
    public void testPartialPartitionKeyFilter()
    {
        final Filter[] filters = new Filter[]{new EqualTo("a", "1")};
        final Map<String, List<String>> partitionKeyValues = FilterUtils.extractPartitionKeyValues(filters, ImmutableSet.of("a", "b"));
    }

    @Test
    public void testValidPartitionKeyValuesExtracted()
    {
        final Filter[] filters = new Filter[]{new EqualTo("a", "1"), new In("b", new String[]{"2", "3"}), new EqualTo("c", "2")};
        final Map<String, List<String>> partitionKeyValues = FilterUtils.extractPartitionKeyValues(filters, ImmutableSet.of("a", "b"));
        assertFalse(partitionKeyValues.containsKey("c"));
        assertTrue(partitionKeyValues.containsKey("a"));
        assertTrue(partitionKeyValues.containsKey("b"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCartesianProductOfInValidValues()
    {
        final List<List<String>> orderedValues = Arrays.asList(Arrays.asList("1", "2"), Arrays.asList("a", "b", "c"), Collections.emptyList());
        final List<List<String>> product = FilterUtils.cartesianProduct(orderedValues);
    }

    @Test
    public void testCartesianProductOfEmptyList()
    {
        final List<List<String>> orderedValues = Collections.emptyList();
        final List<List<String>> product = FilterUtils.cartesianProduct(orderedValues);
        assertFalse(product.isEmpty());
        assertEquals(1, product.size());
        assertTrue(product.get(0).isEmpty());
    }

    @Test
    public void testCartesianProductOfSingleton()
    {
        final List<List<String>> orderedValues = Collections.singletonList(Arrays.asList("a", "b", "c"));
        assertEquals(3, FilterUtils.cartesianProduct(orderedValues).size());
    }
}
