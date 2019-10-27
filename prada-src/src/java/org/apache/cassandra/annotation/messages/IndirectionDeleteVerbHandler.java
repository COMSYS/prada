package org.apache.cassandra.annotation.messages;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.cassandra.annotation.IndirectionSupport;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.TreeMapBackedSortedColumns;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;

public class IndirectionDeleteVerbHandler  implements IVerbHandler<IndirectionMessage>
{

    @Override
    public void doVerb(MessageIn<IndirectionMessage> message, int id)
    {
        String msgStr = ByteBufferUtil.stringWOException(message.payload.content);
        String[] parts = msgStr.split(";");
        assert(parts.length == 5);
        CFMetaData cfm = Keyspace.open(parts[0] + IndirectionSupport.DataKeyspacePostfix).getColumnFamilyStore(parts[1]).metadata;
        QueryProcessor.processInternal("DELETE FROM " + cfm.ksName + "." + cfm.cfName + " WHERE " + ByteBufferUtil.stringWOException(cfm.partitionKeyColumns().get(0).name) + "='" + parts[2] +"';", cfm.ksName);
        IndirectionMessage msg = new IndirectionDeleteMessage(ByteBufferUtil.EMPTY_BYTE_BUFFER);
        MessageOut<IndirectionMessage> msgout = new MessageOut<>(MessagingService.Verb.INTERNAL_RESPONSE, msg, IndirectionMessage.serializer);
        try
        {
            InetAddress addr = InetAddress.getByName(parts[4]);
            MessagingService.instance().sendReply(msgout, Integer.parseInt(parts[3]), addr);
        } catch (UnknownHostException e)
        {
            e.printStackTrace();
        }
    }

}
