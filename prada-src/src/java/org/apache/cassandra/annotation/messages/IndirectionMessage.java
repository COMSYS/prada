package org.apache.cassandra.annotation.messages;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.utils.ByteBufferUtil;

public abstract class IndirectionMessage {

    public ByteBuffer content;
    private IndirectionMessageType type;
    public static final IndirectionMessageSerializer serializer = new IndirectionMessageSerializer();

    public IndirectionMessage(ByteBuffer content, IndirectionMessageType type)
    {
        this.content = content;
        this.type = type;
    }

    public abstract MessageOut<IndirectionMessage> createMessage();

    public IndirectionMessageType getType()
    {
        return type;
    }

    public static class IndirectionMessageSerializer implements IVersionedSerializer<IndirectionMessage>
    {
        public void serialize(IndirectionMessage message, DataOutput out, int version) throws IOException
        {
            out.write(message.getType().ordinal());
            ByteBufferUtil.writeWithShortLength(message.content, out);
        }

        public IndirectionMessage deserialize(DataInput in, int version) throws IOException
        {
            int t = in.readByte();
            IndirectionMessage msg = null;
            switch (IndirectionMessageType.values()[t])
            {
            case DATA:
                msg = new IndirectionDataMessage(ByteBufferUtil.readWithShortLength(in));
                break;
            case DELETE:
                msg = new IndirectionDeleteMessage(ByteBufferUtil.readWithShortLength(in));
                break;
            case UPDATE_REFERENCES:
                msg = new IndirectionUpdateReferencesMessage(ByteBufferUtil.readWithShortLength(in));
                break;
            }
            return msg;
        }

        public long serializedSize(IndirectionMessage message, int version)
        {
            int length = message.content.remaining();
            return TypeSizes.NATIVE.sizeof((short) length) + 1 + length;
        }
    }

    public enum IndirectionMessageType
    {
        DATA, DELETE, UPDATE_REFERENCES
    }
}
