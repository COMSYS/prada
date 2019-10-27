package org.apache.cassandra.annotation.messages;

import java.nio.ByteBuffer;

import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;

public class IndirectionUpdateReferencesMessage extends IndirectionMessage
{
    public IndirectionUpdateReferencesMessage(ByteBuffer content)
    {
        super(content, IndirectionMessageType.UPDATE_REFERENCES);
    }

    public MessageOut<IndirectionMessage> createMessage()
    {
        return new MessageOut<IndirectionMessage>(MessagingService.Verb.INDIRECTION_UPDATE_REFERENCES_MESSAGE, this, serializer);
    }
}
