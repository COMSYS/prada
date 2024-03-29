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
package org.apache.cassandra.transport.messages;

import java.nio.ByteBuffer;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolException;
import org.apache.cassandra.transport.ServerError;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.MD5Digest;

/**
 * Message to indicate an error to the client.
 */
public class ErrorMessage extends Message.Response
{
    private static final Logger logger = LoggerFactory.getLogger(ErrorMessage.class);

    public static final Message.Codec<ErrorMessage> codec = new Message.Codec<ErrorMessage>()
    {
        public ErrorMessage decode(ChannelBuffer body, int version)
        {
            ExceptionCode code = ExceptionCode.fromValue(body.readInt());
            String msg = CBUtil.readString(body);

            TransportException te = null;
            switch (code)
            {
                case SERVER_ERROR:
                    te = new ServerError(msg);
                    break;
                case PROTOCOL_ERROR:
                    te = new ProtocolException(msg);
                    break;
                case BAD_CREDENTIALS:
                    te = new AuthenticationException(msg);
                    break;
                case UNAVAILABLE:
                    {
                        ConsistencyLevel cl = CBUtil.readConsistencyLevel(body);
                        int required = body.readInt();
                        int alive = body.readInt();
                        te = new UnavailableException(cl, required, alive);
                    }
                    break;
                case OVERLOADED:
                    te = new OverloadedException(msg);
                    break;
                case IS_BOOTSTRAPPING:
                    te = new IsBootstrappingException();
                    break;
                case TRUNCATE_ERROR:
                    te = new TruncateException(msg);
                    break;
                case WRITE_TIMEOUT:
                case READ_TIMEOUT:
                    ConsistencyLevel cl = CBUtil.readConsistencyLevel(body);
                    int received = body.readInt();
                    int blockFor = body.readInt();
                    if (code == ExceptionCode.WRITE_TIMEOUT)
                    {
                        WriteType writeType = Enum.valueOf(WriteType.class, CBUtil.readString(body));
                        te = new WriteTimeoutException(writeType, cl, received, blockFor);
                    }
                    else
                    {
                        byte dataPresent = body.readByte();
                        te = new ReadTimeoutException(cl, received, blockFor, dataPresent != 0);
                    }
                    break;
                case UNPREPARED:
                    {
                        MD5Digest id = MD5Digest.wrap(CBUtil.readBytes(body));
                        te = new PreparedQueryNotFoundException(id);
                    }
                    break;
                case SYNTAX_ERROR:
                    te = new SyntaxException(msg);
                    break;
                case UNAUTHORIZED:
                    te = new UnauthorizedException(msg);
                    break;
                case INVALID:
                    te = new InvalidRequestException(msg);
                    break;
                case CONFIG_ERROR:
                    te = new ConfigurationException(msg);
                    break;
                case ALREADY_EXISTS:
                    String ksName = CBUtil.readString(body);
                    String cfName = CBUtil.readString(body);
                    if (cfName.isEmpty())
                        te = new AlreadyExistsException(ksName);
                    else
                        te = new AlreadyExistsException(ksName, cfName);
                    break;
                case INDIRECTION_ERROR:
                    te = new IndirectionException(msg);
                    break;
            }
            return new ErrorMessage(te);
        }

        public void encode(ErrorMessage msg, ChannelBuffer dest, int version)
        {
            dest.writeInt(msg.error.code().value);
            CBUtil.writeString(msg.error.getMessage(), dest);

            switch (msg.error.code())
            {
                case UNAVAILABLE:
                    UnavailableException ue = (UnavailableException)msg.error;
                    CBUtil.writeConsistencyLevel(ue.consistency, dest);
                    dest.writeInt(ue.required);
                    dest.writeInt(ue.alive);
                    break;
                case WRITE_TIMEOUT:
                case READ_TIMEOUT:
                    RequestTimeoutException rte = (RequestTimeoutException)msg.error;
                    boolean isWrite = msg.error.code() == ExceptionCode.WRITE_TIMEOUT;

                    CBUtil.writeConsistencyLevel(rte.consistency, dest);
                    dest.writeInt(rte.received);
                    dest.writeInt(rte.blockFor);
                    if (isWrite)
                        CBUtil.writeString(((WriteTimeoutException)rte).writeType.toString(), dest);
                    else
                        dest.writeByte((byte)(((ReadTimeoutException)rte).dataPresent ? 1 : 0));
                    break;
                case UNPREPARED:
                    PreparedQueryNotFoundException pqnfe = (PreparedQueryNotFoundException)msg.error;
                    CBUtil.writeBytes(pqnfe.id.bytes, dest);
                    break;
                case ALREADY_EXISTS:
                    AlreadyExistsException aee = (AlreadyExistsException)msg.error;
                    CBUtil.writeString(aee.ksName, dest);
                    CBUtil.writeString(aee.cfName, dest);
                    break;
            }
        }

        public int encodedSize(ErrorMessage msg, int version)
        {
            int size = 4 + CBUtil.sizeOfString(msg.error.getMessage());
            switch (msg.error.code())
            {
                case UNAVAILABLE:
                    UnavailableException ue = (UnavailableException)msg.error;
                    size += CBUtil.sizeOfConsistencyLevel(ue.consistency) + 8;
                    break;
                case WRITE_TIMEOUT:
                case READ_TIMEOUT:
                    RequestTimeoutException rte = (RequestTimeoutException)msg.error;
                    boolean isWrite = msg.error.code() == ExceptionCode.WRITE_TIMEOUT;
                    size += CBUtil.sizeOfConsistencyLevel(rte.consistency) + 8;
                    size += isWrite ? CBUtil.sizeOfString(((WriteTimeoutException)rte).writeType.toString()) : 1;
                    break;
                case UNPREPARED:
                    PreparedQueryNotFoundException pqnfe = (PreparedQueryNotFoundException)msg.error;
                    size += CBUtil.sizeOfBytes(pqnfe.id.bytes);
                    break;
                case ALREADY_EXISTS:
                    AlreadyExistsException aee = (AlreadyExistsException)msg.error;
                    size += CBUtil.sizeOfString(aee.ksName);
                    size += CBUtil.sizeOfString(aee.cfName);
                    break;
            }
            return size;
        }
    };

    // We need to figure error codes out (#3979)
    public final TransportException error;

    private ErrorMessage(TransportException error)
    {
        super(Message.Type.ERROR);
        this.error = error;
    }

    private ErrorMessage(TransportException error, int streamId)
    {
        this(error);
        setStreamId(streamId);
    }

    public static ErrorMessage fromException(Throwable e)
    {
        int streamId = 0;
        if (e instanceof WrappedException)
        {
            streamId = ((WrappedException)e).streamId;
            e = e.getCause();
        }

        if (e instanceof TransportException)
            return new ErrorMessage((TransportException)e, streamId);

        // Unexpected exception
        logger.error("Unexpected exception during request", e);
        return new ErrorMessage(new ServerError(e), streamId);
    }

    @Override
    public String toString()
    {
        return "ERROR " + error.code() + ": " + error.getMessage();
    }

    public static RuntimeException wrap(Throwable t, int streamId)
    {
        return new WrappedException(t, streamId);
    }

    private static class WrappedException extends RuntimeException
    {
        private final int streamId;

        public WrappedException(Throwable cause, int streamId)
        {
            super(cause);
            this.streamId = streamId;
        }
    }

}
