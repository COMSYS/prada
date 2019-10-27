package org.apache.cassandra.annotation.messages;

import java.nio.ByteBuffer;

import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.utils.ByteBufferUtil;

public class IndirectionDataMessage extends IndirectionMessage {

    public IndirectionDataMessage(ByteBuffer content)
    {
        super(content, IndirectionMessageType.DATA);
    }

    public IndirectionDataMessage(ByteBuffer content, ClientState state)
    {
        this(content);
        if (state.getRawKeyspace() != null)
        {
            String contentString = ByteBufferUtil.stringWOException(content);
            if (contentString.substring(0, contentString.indexOf("(")).indexOf(".") == -1)
            {
                contentString = contentString.replaceFirst("(?i)INSERT INTO ", "INSERT INTO " + state.getRawKeyspace() + ".");
            }
            this.content = ByteBufferUtil.bytes(contentString);
        }
    }

    public MessageOut<IndirectionMessage> createMessage()
    {
        return new MessageOut<IndirectionMessage>(MessagingService.Verb.INDIRECTION_DATA_MESSAGE, this, serializer);
    }
}
