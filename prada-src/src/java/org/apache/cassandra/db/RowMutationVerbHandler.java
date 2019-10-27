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

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.annotation.IndirectionInformation;
import org.apache.cassandra.annotation.IndirectionSupport;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.io.util.FastByteArrayInputStream;
import org.apache.cassandra.net.*;
import org.apache.cassandra.service.LoadBroadcaster;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteBufferUtil;

public class RowMutationVerbHandler implements IVerbHandler<RowMutation>
{
    private static final Logger logger = LoggerFactory.getLogger(RowMutationVerbHandler.class);

    public void doVerb(MessageIn<RowMutation> message, int id)
    {
        try
        {
            RowMutation rm = message.payload;

            // Check if there were any forwarding headers in this message
            byte[] from = message.parameters.get(RowMutation.FORWARD_FROM);
            InetAddress replyTo;
            if (from == null)
            {
                replyTo = message.from;
                byte[] forwardBytes = message.parameters.get(RowMutation.FORWARD_TO);
                if (forwardBytes != null)
                    forwardToLocalNodes(rm, message.verb, forwardBytes, message.from);
            }
            else
            {
                replyTo = InetAddress.getByAddress(from);
            }

            boolean ind = false;
            if (!rm.getColumnFamilies().isEmpty())
            {
                ColumnFamily cf = rm.getColumnFamilies().iterator().next();
                if (!cf.deletionInfo().isLive() && cf.deletionInfo().isDeleted(null, System.currentTimeMillis()) && IndirectionSupport.isRegularKeyspace(rm.getKeyspaceName()))
                {
                    String key = ByteBufferUtil.stringWOException(rm.key());
                    IndirectionInformation exRefInfo = IndirectionInformation.getExistingIndirectionInformation(rm.getKeyspaceName(), cf.metadata().cfName, key);
                    List<String> existingNodes = null;
                    if (exRefInfo != null)
                        existingNodes = exRefInfo.getNodesAsString();
                    if (existingNodes != null && from == null)
                    {
                        ind = true;
                        CFMetaData cfm = Keyspace.open(rm.getKeyspaceName() + IndirectionSupport.DataKeyspacePostfix).getColumnFamilyStore(cf.metadata().cfName).metadata;
                        ColumnFamily dcf = TreeMapBackedSortedColumns.factory.create(cfm);
                        dcf.delete(new DeletionInfo(cf.deletionInfo().getTopLevelDeletion().markedForDeleteAt, cf.deletionInfo().getTopLevelDeletion().localDeletionTime));
                        MessageOut<RowMutation> delMsg = new MessageOut<RowMutation>(message.verb, new RowMutation(rm.key(), dcf), RowMutation.serializer).withParameter(RowMutation.FORWARD_FROM, message.from.getAddress());
                        for (String node : existingNodes)
                            MessagingService.instance().sendOneWay(delMsg, id, StorageService.instance.getAssociatedEndpoint(node));
                    }
                    else if (existingNodes == null)
                    {
                        rm.apply();
                    }
                    if (existingNodes != null)
                    {
                        CFMetaData cfm = Keyspace.open(rm.getKeyspaceName() + IndirectionSupport.ReferenceKeyspacePostfix).getColumnFamilyStore(cf.metadata().cfName).metadata;
                        ColumnFamily dcf = TreeMapBackedSortedColumns.factory.create(cfm);
                        dcf.delete(new DeletionInfo(cf.deletionInfo().getTopLevelDeletion().markedForDeleteAt, cf.deletionInfo().getTopLevelDeletion().localDeletionTime));
                        RowMutation refMut = new RowMutation(rm.key(), dcf);
                        refMut.apply();
                    }
                }
                else
                    rm.apply();
            }
            else
                rm.apply();
            if (!ind)
            {
                WriteResponse response = new WriteResponse();
                Tracing.trace("Enqueuing response to {}", replyTo);
                MessagingService.instance().sendReply(response.createMessage(), id, replyTo);
            }
            for (ColumnFamily c : rm.getColumnFamilies())
                LoadBroadcaster.instance.updateLoadCache(null, c.dataSize());
        }
        catch (IOException e)
        {
            logger.error("Error in row mutation", e);
        }
    }

    /**
     * Older version (< 1.0) will not send this message at all, hence we don't
     * need to check the version of the data.
     */
    private void forwardToLocalNodes(RowMutation rm, MessagingService.Verb verb, byte[] forwardBytes, InetAddress from) throws IOException
    {
        DataInputStream in = new DataInputStream(new FastByteArrayInputStream(forwardBytes));
        int size = in.readInt();

        // tell the recipients who to send their ack to
        MessageOut<RowMutation> message = new MessageOut<RowMutation>(verb, rm, RowMutation.serializer).withParameter(RowMutation.FORWARD_FROM, from.getAddress());
        // Send a message to each of the addresses on our Forward List
        for (int i = 0; i < size; i++)
        {
            InetAddress address = CompactEndpointSerializationHelper.deserialize(in);
            int id = in.readInt();
            Tracing.trace("Enqueuing forwarded write to {}", address);
            MessagingService.instance().sendOneWay(message, id, address);
        }
    }
}
