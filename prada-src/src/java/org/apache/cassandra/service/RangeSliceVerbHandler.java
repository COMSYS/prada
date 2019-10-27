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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.annotation.IndirectionInformation;
import org.apache.cassandra.db.AbstractRangeCommand;
import org.apache.cassandra.db.RangeSliceCommand;
import org.apache.cassandra.db.RangeSliceReply;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.filter.TombstoneOverwhelmingException;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteBufferUtil;

public class RangeSliceVerbHandler implements IVerbHandler<AbstractRangeCommand>
{
    public void doVerb(MessageIn<AbstractRangeCommand> message, int id)
    {
        try
        {
            if (StorageService.instance.isBootstrapMode())
            {
                /* Don't service reads! */
                throw new RuntimeException("Cannot service reads while bootstrapping!");
            }
            boolean redirected = message.payload.origin != null;
            RangeSliceReply reply = processRangeSliceCommand(message.payload, message.from, id);
            Tracing.trace("Enqueuing response to {}", message.from);
            if (!redirected)
                MessagingService.instance().sendReply(reply.createMessage(), id, message.from);
            else
                MessagingService.instance().sendReply(reply.createMessage(), message.payload.originId, message.payload.origin);
        }
        catch (TombstoneOverwhelmingException e)
        {
            // error already logged.  Drop the request
        }
        catch (Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }

    public static RangeSliceReply processRangeSliceCommand(AbstractRangeCommand payload, InetAddress from, int id)
    {
        boolean redirected = payload.origin != null;
        List<Row> rows = payload.executeLocally();
        RangeSliceReply reply;
        if (rows.isEmpty() || (!rows.get(0).indirection && !rows.get(rows.size()-1).indirection) || !(payload instanceof RangeSliceCommand))
        {
            // no indirection rows
            reply = new RangeSliceReply(rows);
            reply.count = 1;
            if (redirected)
                reply.origin = from;
        }
        else
        {
            assert(!redirected);
            // at least one indirection row
            List<Row> rrows = new ArrayList<>();
            Set<InetAddress> addresses = new HashSet<InetAddress>();
            for (Row r : rows)
            {
                IndirectionInformation ind = null;
                if (r.indirection && (ind = IndirectionInformation.RowToIndirectionInformation(r)) != null)
                {
                    for (ByteBuffer target : ind.getNodes())
                    {
                        addresses.add(StorageService.instance.getAssociatedEndpoint(ByteBufferUtil.stringWOException(target)));
                    }
                }
                else if (!r.indirection)
                {
                    rrows.add(r);
                }
            }
            reply = new RangeSliceReply(rrows);
            reply.count = addresses.size()+1;
            RangeSliceCommand comm = (RangeSliceCommand)payload;
            comm.origin = from;
            comm.originId = id;
            MessageOut<RangeSliceCommand> msg = comm.createMessage();
            for (InetAddress addr : addresses)
                MessagingService.instance().sendOneWay(msg, addr);
        }
        return reply;
    }
}
