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
package org.apache.cassandra.service;

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.*;
import org.apache.cassandra.utils.FBUtilities;

import com.google.common.util.concurrent.AtomicDouble;

public class LoadBroadcaster implements IEndpointStateChangeSubscriber
{
    public static final LoadBroadcaster instance = new LoadBroadcaster();

    private static final Logger logger = LoggerFactory.getLogger(LoadBroadcaster.class);

    private ConcurrentMap<InetAddress, Double> loadInfo = new ConcurrentHashMap<InetAddress, java.lang.Double>();

    private ConcurrentMap<InetAddress, AtomicDouble> loadInfoCache = new ConcurrentHashMap<InetAddress, AtomicDouble>();

    private LoadBroadcaster()
    {
        Gossiper.instance.register(this);
    }

    public void updateLoadCache(InetAddress target, double diff)
    {
        if (target == null)
        {
            AtomicDouble c = loadInfoCache.get(FBUtilities.getBroadcastAddress());
            if (c == null)
            {
                c = new AtomicDouble();
                loadInfoCache.put(FBUtilities.getBroadcastAddress(), c);
            }
            c.addAndGet(diff);
        }
        else
        {
            AtomicDouble c = loadInfoCache.get(target);
            if (c != null)
                c.addAndGet(diff * loadInfo.size());
        }
    }

    public void onChange(InetAddress endpoint, ApplicationState state, VersionedValue value)
    {
        if (state != ApplicationState.LOAD)
            return;
        loadInfo.put(endpoint, Double.valueOf(value.value));
        loadInfoCache.put(endpoint, new AtomicDouble());
    }

    public void onJoin(InetAddress endpoint, EndpointState epState)
    {
        VersionedValue localValue = epState.getApplicationState(ApplicationState.LOAD);
        if (localValue != null)
        {
            onChange(endpoint, ApplicationState.LOAD, localValue);
        }
    }
    
    public void beforeChange(InetAddress endpoint, EndpointState currentState, ApplicationState newStateKey, VersionedValue newValue) {}

    public void onAlive(InetAddress endpoint, EndpointState state) {}

    public void onDead(InetAddress endpoint, EndpointState state) {}

    public void onRestart(InetAddress endpoint, EndpointState state) {}

    public void onRemove(InetAddress endpoint)
    {
        loadInfo.remove(endpoint);
        loadInfoCache.remove(endpoint);
    }

    public Map<InetAddress, Double> getLoadInfo()
    {
        return Collections.unmodifiableMap(loadInfo);
    }

    public Map<InetAddress, AtomicDouble> getLoadInfoCache()
    {
        return Collections.unmodifiableMap(loadInfoCache);
    }

    public void startBroadcasting()
    {
        // send the first broadcast "right away" (i.e., in 2 gossip heartbeats, when we should have someone to talk to);
        // after that send every BROADCAST_INTERVAL.
        Runnable runnable = new Runnable()
        {
            public void run()
            {
                if (logger.isDebugEnabled())
                    logger.debug("Disseminating load info ...");
                double load = StorageService.instance.getLoad();
                Gossiper.instance.addLocalApplicationState(ApplicationState.LOAD,
                                                           StorageService.instance.valueFactory.load(load));
            }
        };
        StorageService.scheduledTasks.scheduleWithFixedDelay(runnable, 2 * Gossiper.intervalInMillis, DatabaseDescriptor.getLoadBroadcastInterval(), TimeUnit.MILLISECONDS);
    }
}

