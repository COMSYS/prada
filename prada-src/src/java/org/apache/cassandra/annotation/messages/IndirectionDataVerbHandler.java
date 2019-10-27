package org.apache.cassandra.annotation.messages;

import java.nio.ByteBuffer;
import java.util.Collections;

import org.apache.cassandra.annotation.IndirectionSupport;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.cql3.statements.UpdateStatement;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.db.*;

public class IndirectionDataVerbHandler implements IVerbHandler<IndirectionMessage> {

    public void doVerb(MessageIn<IndirectionMessage> message, int id)
    {
        String query = ByteBufferUtil.stringWOException(message.payload.content);

        String newquery = query.substring(0, query.indexOf("."));
        newquery += IndirectionSupport.DataKeyspacePostfix;
        if (query.contains(" WITH ANNOTATIONS "))
        {
            newquery += query.substring(query.indexOf("."), query.lastIndexOf(" WITH ANNOTATIONS "));
            newquery += ";";
        }
        else
            newquery += query.substring(query.indexOf("."), query.lastIndexOf(";"));

        IndirectionDataMessage msg = null;
        try
        {
            QueryState queryState = QueryState.forInternalCalls();
            ResultMessage result = QueryProcessor.process(newquery, queryState, new QueryOptions(ConsistencyLevel.ONE, Collections.<ByteBuffer>emptyList()));
            double diff = queryState.getSizeDiff() > 0 ? queryState.getSizeDiff() : 0;
            msg = new IndirectionDataMessage(ByteBufferUtil.bytes(diff));
        }
        catch(RequestExecutionException|RequestValidationException e)
        {
           msg = new IndirectionDataMessage(ByteBufferUtil.bytes(-1.0));
           e.printStackTrace();
        }
        MessageOut<IndirectionMessage> msgout = new MessageOut<>(MessagingService.Verb.INTERNAL_RESPONSE, msg, IndirectionMessage.serializer);
        MessagingService.instance().sendReply(msgout, id, message.from);
    }

    private boolean nodeIsEligible(String query)
    {
        try
        {
            ParsedStatement stmt = QueryProcessor.parseStatement(query);
            assert(stmt instanceof UpdateStatement.ParsedInsert);
            UpdateStatement.ParsedInsert parsed = ((UpdateStatement.ParsedInsert)stmt);
            if (parsed.annotations.isDirect())
                return true;
            if (!parsed.annotations.hasAnnotations())
                return true;
            assert(parsed.annotations.hasAnnotations() && !parsed.annotations.isDirect());

            for (String token : parsed.annotations.getSatisfyingNodes())
               if (FBUtilities.getBroadcastAddress().equals(StorageService.instance.getAssociatedEndpoint(token)))
                   return true;
        }
        catch (RequestValidationException e)
        {
            e.printStackTrace();
        }
        return false;
    }

}
