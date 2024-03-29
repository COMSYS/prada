package org.apache.cassandra.service.paxos;
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


import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.net.IAsyncCallback;

public abstract class AbstractPaxosCallback<T> implements IAsyncCallback<T>
{
    protected final CountDownLatch latch;
    protected final int targets;

    public AbstractPaxosCallback(int targets)
    {
        this.targets = targets;
        latch = new CountDownLatch(targets);
    }

    public boolean isLatencyForSnitch()
    {
        return false;
    }

    public int getResponseCount()
    {
        return (int) (targets - latch.getCount());
    }

    public void await() throws WriteTimeoutException
    {
        try
        {
            if (!latch.await(DatabaseDescriptor.getWriteRpcTimeout(), TimeUnit.MILLISECONDS))
                throw new WriteTimeoutException(WriteType.CAS, ConsistencyLevel.SERIAL, getResponseCount(), targets);
        }
        catch (InterruptedException ex)
        {
            throw new AssertionError("This latch shouldn't have been interrupted.");
        }
    }

    @Override
    public boolean done()
    {
        return true;
    }
}
