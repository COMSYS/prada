/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db;

import java.net.InetAddress;
import java.util.List;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.service.IReadCommand;
import org.apache.cassandra.thrift.IndexExpression;

public abstract class AbstractRangeCommand implements IReadCommand
{
    public final String keyspace;
    public final String columnFamily;
    public final long timestamp;

    public final AbstractBounds<RowPosition> keyRange;
    public final IDiskAtomFilter predicate;
    public final List<IndexExpression> rowFilter;

    public InetAddress origin;
    public int originId = -1;

    public AbstractRangeCommand(String keyspace, String columnFamily, long timestamp, AbstractBounds<RowPosition> keyRange, IDiskAtomFilter predicate, List<IndexExpression> rowFilter)
    {
        this.keyspace = keyspace;
        this.columnFamily = columnFamily;
        this.timestamp = timestamp;
        this.keyRange = keyRange;
        this.predicate = predicate;
        this.rowFilter = rowFilter;
    }

    public String getKeyspace()
    {
        return keyspace;
    }

    public abstract MessageOut<? extends AbstractRangeCommand> createMessage();
    public abstract AbstractRangeCommand forSubRange(AbstractBounds<RowPosition> range);
    public abstract AbstractRangeCommand withUpdatedLimit(int newLimit);

    public abstract int limit();
    public abstract boolean countCQL3Rows();
    public abstract List<Row> executeLocally();

    public long getTimeout()
    {
        return DatabaseDescriptor.getRangeRpcTimeout();
    }
}
