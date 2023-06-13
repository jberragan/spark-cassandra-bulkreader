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

package org.apache.cassandra.spark.reader.fourzero;

import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.Cell;

public class MapBuffer extends ComplexTypeBuffer
{
    MapBuffer(int cellCount)
    {
        super(cellCount, cellCount * 2);
    }

    @Override
    public void addCell(Cell cell)
    {
        this.add(cell.path().get(0)); // map - copy over key and value
        super.addCell(cell);
    }

    @Override
    protected int elements()
    {
        // divide 2 because we add key and value to the buffer, which makes it twice as big as the map entries.
        return super.elements() / 2;
    }
}
