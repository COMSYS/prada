package org.apache.cassandra.annotation.messages;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;

public class IndirectionDeleteMessage extends IndirectionMessage {

    public IndirectionDeleteMessage(ByteBuffer content)
    {
        super(content, IndirectionMessageType.DELETE);
    }

    public MessageOut<IndirectionMessage> createMessage()
    {
        return new MessageOut<IndirectionMessage>(MessagingService.Verb.INDIRECTION_DELETE_MESSAGE, this, serializer);
    }

    public static class IndirectionDeleteMessageSerializer implements IVersionedSerializer<IndirectionDeleteMessage>
    {
        public void serialize(IndirectionDeleteMessage message, DataOutput out, int version) throws IOException
        {
            ByteBufferUtil.writeWithShortLength(message.content, out);
        }

        public IndirectionDeleteMessage deserialize(DataInput in, int version) throws IOException
        {
            return new IndirectionDeleteMessage(ByteBufferUtil.readWithShortLength(in));
        }

        public long serializedSize(IndirectionDeleteMessage message, int version)
        {
            int length = message.content.remaining();
            return TypeSizes.NATIVE.sizeof((short) length) + length;
        }
    }

}
