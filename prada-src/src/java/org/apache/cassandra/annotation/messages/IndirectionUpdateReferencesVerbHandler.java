package org.apache.cassandra.annotation.messages;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.cassandra.annotation.IndirectionInformation;
import org.apache.cassandra.annotation.IndirectionSupport;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ConsistencyLevel;

import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;

public class IndirectionUpdateReferencesVerbHandler implements IVerbHandler<IndirectionMessage>
{
    @Override
    public void doVerb(MessageIn<IndirectionMessage> message, int id)
    {
        String query = ByteBufferUtil.stringWOException(message.payload.content);
        String[] args = query.split(";");
        assert(args.length > 0);
        int nodesCount = Integer.parseInt(args[0]);
        assert(args.length == nodesCount + 5);
        List<String> chosenNodes = new LinkedList<String>();
        int i;
        for(i = 1; i <= nodesCount; i++)
            chosenNodes.add(args[i]);
        String keyspace = args[i];
        String cf = args[i+1];
        String key = args[i+2];
        String annotations = args[i+3];
        IndirectionInformation exRefInfo = IndirectionInformation.getExistingIndirectionInformation(keyspace, cf, key);

        List<String> existingNodes = null;
        if (exRefInfo != null)
            existingNodes = exRefInfo.getNodesAsString();

        IndirectionMessage msg = null;
        int delCount = 0;
        double size = 0;
        String error = null;

        if (existingNodes == null)
        {
            // we inserted a new key, everything is fine
            IndirectionSupport.sendToLog("Inserted new key with annotations.", false, true, "IndirectionWriteSupport");
        }
        else
        {
            // we inserted an existing key, check whether we have to delete data from nodes
            IndirectionSupport.sendToLog("Inserted an existing key with annotations.", false, true, "IndirectionWriteSupport");
            existingNodes.removeAll(chosenNodes);
            if (existingNodes.isEmpty())
            {
                IndirectionSupport.sendToLog("Set of chosen nodes contains all existing nodes.", false, true, "IndirectionWriteSupport");
            }
            else
            {
                IndirectionSupport.sendToLog("Set of chosen nodes does not contain all existing nodes.", false, true, "IndirectionWriteSupport");
                List<InetAddress> addresses = new LinkedList<InetAddress>();
                for (String node : existingNodes)
                {
                    addresses.add(StorageService.instance.getAssociatedEndpoint(node));
                }
                delCount = addresses.size();

                String delMsgStr = keyspace + ";" + cf + ";" + key + ";" + id + ";" + message.from.getHostAddress();
                MessageOut<IndirectionMessage> delMsg = new IndirectionDeleteMessage(ByteBufferUtil.bytes(delMsgStr)).createMessage();
                for (InetAddress addr : addresses)
                    MessagingService.instance().sendOneWay(delMsg, addr);
            }
        }
        try
        {
            if(msg == null)
            {
                QueryState queryState = QueryState.forInternalCalls();
                QueryProcessor.process(IndirectionInformation.getInsertQuery(chosenNodes, keyspace, cf, key, annotations), queryState, new QueryOptions(ConsistencyLevel.ONE, Collections.<ByteBuffer>emptyList()));
                if (existingNodes == null && queryState.getSizeDiff() > 0)
                    size = queryState.getSizeDiff();
            }
        }
        catch(RequestExecutionException | RequestValidationException e)
        {
            e.printStackTrace();
            size = -1;
            error = "IND_REF_UPDATE_FAILED:INSERT_FAILED";
        }
        StringBuilder sb = new StringBuilder();
        sb.append(size).append(";").append(delCount);
        if (error != null)
            sb.append(";").append(error);
        msg = new IndirectionUpdateReferencesMessage(ByteBufferUtil.bytes(sb.toString()));
        MessageOut<IndirectionMessage> msgout = new MessageOut<>(MessagingService.Verb.INTERNAL_RESPONSE, msg, IndirectionMessage.serializer);
        MessagingService.instance().sendReply(msgout, id, message.from);
    }
}
