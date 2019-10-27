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

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.FastByteArrayInputStream;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;

public class RangeSliceReply
{
    public static final RangeSliceReplySerializer serializer = new RangeSliceReplySerializer();

    public final List<Row> rows;
    public int count;
    public InetAddress origin;

    public RangeSliceReply(List<Row> rows)
    {
        this.rows = rows;
        this.count = -1;
    }

    public MessageOut<RangeSliceReply> createMessage()
    {
        return new MessageOut<RangeSliceReply>(MessagingService.Verb.REQUEST_RESPONSE, this, serializer);
    }

    @Override
    public String toString()
    {
        return "RangeSliceReply{" +
               "rows=" + StringUtils.join(rows, ",") +
               '}';
    }

    public static RangeSliceReply read(byte[] body, int version) throws IOException
    {
        return serializer.deserialize(new DataInputStream(new FastByteArrayInputStream(body)), version);
    }

    private static class RangeSliceReplySerializer implements IVersionedSerializer<RangeSliceReply>
    {
        public void serialize(RangeSliceReply rsr, DataOutput out, int version) throws IOException
        {
            out.writeInt(rsr.rows.size());
            for (Row row : rsr.rows)
                Row.serializer.serialize(row, out, version);
            out.writeInt(rsr.count);
            if (rsr.origin == null)
            {
                out.writeShort((short)0);
            }
            else
            {
                ByteBufferUtil.writeWithShortLength(ByteBuffer.wrap(rsr.origin.getAddress()), out);
            }
        }

        public RangeSliceReply deserialize(DataInput in, int version) throws IOException
        {
            int rowCount = in.readInt();
            List<Row> rows = new ArrayList<Row>(rowCount);
            for (int i = 0; i < rowCount; i++)
                rows.add(Row.serializer.deserialize(in, version));
            RangeSliceReply rsr = new RangeSliceReply(rows);
            rsr.count = in.readInt();
            int originLen = in.readShort();
            if (originLen > 0)
            {
                rsr.origin = InetAddress.getByAddress(ByteBufferUtil.read(in, originLen).array());
            }
            return rsr;
        }

        public long serializedSize(RangeSliceReply rsr, int version)
        {
            int size = TypeSizes.NATIVE.sizeof(rsr.rows.size());
            for (Row row : rsr.rows)
                size += Row.serializer.serializedSize(row, version);
            size += TypeSizes.NATIVE.sizeof(rsr.count);
            if (rsr.origin == null)
            {
                size += TypeSizes.NATIVE.sizeof((short)0);
            }
            else
            {
                size += TypeSizes.NATIVE.sizeof((short) rsr.origin.getAddress().length) + rsr.origin.getAddress().length;
            }
            return size;
        }
    }
}
