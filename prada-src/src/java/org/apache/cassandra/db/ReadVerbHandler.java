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

import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.annotation.IndirectionInformation;
import org.apache.cassandra.annotation.IndirectionSupport;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteBufferUtil;

public class ReadVerbHandler implements IVerbHandler<ReadCommand>
{
    private static final Logger logger = LoggerFactory.getLogger( ReadVerbHandler.class );

    public void doVerb(MessageIn<ReadCommand> message, int id)
    {
        if (StorageService.instance.isBootstrapMode())
        {
            throw new RuntimeException("Cannot service reads while bootstrapping!");
        }

        ReadCommand command = message.payload;
        Keyspace keyspace = Keyspace.open(command.ksName);
        Row row;
        row = command.getRow(keyspace);

        IndirectionInformation ind = null;
        if (row.indirection && (ind = IndirectionInformation.RowToIndirectionInformation(row)) != null)
        {
            assert(!command.ksName.contains(IndirectionSupport.ReferenceKeyspacePostfix));
            String newKs = command.ksName + IndirectionSupport.DataKeyspacePostfix;
            ReadCommand newCommand = command.copy(newKs);
            MessageOut<ReadCommand> msgout = new MessageOut<ReadCommand>(MessagingService.Verb.READ, newCommand, ReadCommand.serializer);
            List<InetAddress> addrs = StorageService.instance.getNaturalEndpoints(command.ksName, command.key);
            int i = addrs.indexOf(FBUtilities.getBroadcastAddress());
            assert(i >= 0);
            assert(i < ind.getNodes().size());
            org.apache.cassandra.dht.Token tk = StorageService.getPartitioner().getTokenFactory().fromString(ByteBufferUtil.stringWOException(ind.getNodes().get(i)));
            InetAddress addr = StorageService.instance.getNaturalEndpoints(command.ksName, tk).get(0);
            MessagingService.instance().sendOneWay(msgout, id, addr);
        }
        else
        {
            MessageOut<ReadResponse> reply = new MessageOut<ReadResponse>(MessagingService.Verb.REQUEST_RESPONSE,
                                                                          getResponse(command, row),
                                                                          ReadResponse.serializer);
            Tracing.trace("Enqueuing response to {}", message.from);
            MessagingService.instance().sendReply(reply, id, command.from);
        }
    }

    public static ReadResponse getResponse(ReadCommand command, Row row)
    {
        if (command.isDigestQuery())
        {
            return new ReadResponse(ColumnFamily.digest(row.cf));
        }
        else
        {
            return new ReadResponse(row);
        }
    }
}
