package org.apache.cassandra.annotation;

import java.net.InetAddress;
import java.nio.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

import org.apache.cassandra.annotation.messages.*;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.statements.*;
import org.apache.cassandra.net.*;
import org.apache.cassandra.service.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.db.*;
import org.apache.cassandra.exceptions.*;

import com.google.common.base.Predicate;

public class IndirectionWriteSupport {

    public static void initKeyspaces(CreateKeyspaceStatement ksStatement) throws RequestValidationException
    {
        String KeySpaceName = ksStatement.keyspace();
        if (IndirectionSupport.isRegularKeyspace(KeySpaceName))
        {
            try
            {
                QueryProcessor.process("CREATE KEYSPACE " + KeySpaceName + IndirectionSupport.DataKeyspacePostfix + " WITH REPLICATION = { 'class' : 'LocalStrategy' };", ConsistencyLevel.ONE);
            }
            catch(RequestExecutionException e)
            {
                e.printStackTrace();
                throw new InvalidRequestException("Error creating " + IndirectionSupport.DataKeyspacePostfix + " Keyspace");
            }
            CreateKeyspaceStatement stmtRef = new CreateKeyspaceStatement(ksStatement.keyspace() + IndirectionSupport.ReferenceKeyspacePostfix, ksStatement.getAttrs(), false);
            stmtRef.execute(QueryState.forInternalCalls(), QueryOptions.DEFAULT);
        }
    }

    public static void initTables(String queryStr, CreateTableStatement stmt)
    {
        String keyspace = stmt.keyspace();
        if (!IndirectionSupport.isRegularKeyspace(keyspace))
            return;

        String TableName = stmt.columnFamily();
        int replicationFactor = Keyspace.open(keyspace).getReplicationStrategy().getReplicationFactor();
        String colNames = "";
        for (int i=0; i < replicationFactor; i++)
        {
            colNames += ", node" + (i+1) + " varchar";
        }
        colNames += ", " + IndirectionInformation.AnnotationRowName + " varchar";

        IndirectionSupport.queryDB(queryStr.replaceFirst(Pattern.quote(keyspace + "."), keyspace + IndirectionSupport.DataKeyspacePostfix + "."), ConsistencyLevel.ONE);
        IndirectionSupport.queryDB("CREATE TABLE " + keyspace + IndirectionSupport.ReferenceKeyspacePostfix + "." + TableName + " (Key varchar PRIMARY KEY" + colNames + ");", ConsistencyLevel.ONE);
    }

    public static void DropIndirectionKeyspaces(String keyspace)
    {
        if (IndirectionSupport.isRegularKeyspace(keyspace))
        {
            IndirectionSupport.queryDB("DROP KEYSPACE " + keyspace + IndirectionSupport.DataKeyspacePostfix + ";", ConsistencyLevel.ONE);
            IndirectionSupport.queryDB("DROP KEYSPACE " + keyspace + IndirectionSupport.ReferenceKeyspacePostfix + ";", ConsistencyLevel.ONE);
        }
    }

    public static void DropTable(String keyspace, String table)
    {
        if (IndirectionSupport.isRegularKeyspace(keyspace))
        {
            IndirectionSupport.queryDB("DROP TABLE " + keyspace + IndirectionSupport.DataKeyspacePostfix + "." + table + ";", ConsistencyLevel.ONE);
            IndirectionSupport.queryDB("DROP TABLE " + keyspace + IndirectionSupport.ReferenceKeyspacePostfix + "." + table + ";", ConsistencyLevel.ONE);
        }
    }

    public static boolean insertData(UpdateStatement.ParsedInsert statement, String queryStr, ClientState clientState) throws RequestValidationException, IndirectionException, SyntaxException
    {
        if (!IndirectionSupport.isRegularKeyspace(statement.keyspace()))
            return false;

        CQLStatement cqlstatem = statement.prepare().statement;
        assert(cqlstatem instanceof UpdateStatement);

        if (Schema.instance.getKeyspaceInstance(statement.keyspace()) == null)
            throw new InvalidRequestException("Keyspace " + statement.keyspace() + " does not exist");
        Keyspace keyspace = Keyspace.open(statement.keyspace());
        // We look for nodes which fulfill the annotations and let the LoadBalancer choose a subset of these
        // according to the replication factor. Then we specify the chosen nodes directly in the query expression
        //(afterwards the expression is direct)
        statement.annotations.makeDirectWithAnnotations(keyspace.getReplicationStrategy().getReplicationFactor(), keyspace.getName(), ((UpdateStatement) cqlstatem).PrimaryKey); // insert regardless of nodes which may hold existing data

        if (statement.annotations.isDirect())
        {
            HashSet<InetAddress> addresses = new HashSet<>(statement.annotations.getTargets());

            if (addresses.size() != statement.annotations.getTargets().size())
                throw new InvalidRequestException("One or more target tokens belong to the same endpoint!");
            if (statement.annotations.getTargets().size() != keyspace.getReplicationStrategy().getReplicationFactor())
                throw new InvalidRequestException("Exactly " + keyspace.getReplicationStrategy().getReplicationFactor() + " target nodes must be specified");

            // Send data to all target nodes
            IndirectionDataPredicate pred = new IndirectionDataPredicate();
            AsyncMultipleResponses<IndirectionMessage> cb =
                    sendMessageToNodes(addresses, new IndirectionDataMessage(ByteBufferUtil.bytes(queryStr), clientState), pred, ConsistencyLevel.ONE, keyspace);

            assert (cqlstatem instanceof UpdateStatement);
            assert (((UpdateStatement) cqlstatem).PrimaryKey != ByteBufferUtil.EMPTY_BYTE_BUFFER);

            IndirectionInformation refInfo = new IndirectionInformation(ByteBufferUtil.stringWOException(((UpdateStatement) cqlstatem).PrimaryKey), statement.keyspace(), statement.columnFamily(), statement.annotations);

            boolean newData = refInfo.updateReferenceInformation();

            try {
                if(!cb.waitForResponses(DatabaseDescriptor.getTimeout(MessagingService.Verb.INDIRECTION_DATA_MESSAGE), TimeUnit.MILLISECONDS))
                {
                    throw new SyntaxException("WannabeIndirectionException: Error inserting redirected data");
                }
            } catch (TimeoutException e1) {
                throw new SyntaxException("WannabeIndirectionException: Insertion of redirected data timed out");
            }
            if (newData)
            {
                InetAddress ownAddr = FBUtilities.getBroadcastAddress();
                for (InetAddress target : addresses)
                {
                    if (!ownAddr.equals(target))
                        LoadBroadcaster.instance.updateLoadCache(target, pred.getDiff());
                }
            }

            return true;
        }

        return false;
    }

    public static AsyncMultipleResponses<IndirectionMessage> sendMessageToNodes(Collection<InetAddress> addresses, IndirectionMessage msg, Predicate<IndirectionMessage> pred, ConsistencyLevel cl, Keyspace keyspace)
    {
        MessageOut<IndirectionMessage> msgout = (MessageOut<IndirectionMessage>) msg.createMessage();
        AsyncMultipleResponses<IndirectionMessage> callback = new AsyncMultipleResponses<>(pred, cl, keyspace, addresses.size());
        for (InetAddress addr : addresses)
        {
            IndirectionSupport.sendToLog("Send message to Node node with Address " + addr.toString(), false, true, "IndirectionWriteSupport");
            MessagingService.instance().sendRR(msgout, addr, callback);
        }
        return callback;
    }

    public static AsyncMultipleResponses<IndirectionMessage> sendMessageToNodes(Collection<String> Tokens, Keyspace keyspace, IndirectionMessage msg, Predicate<IndirectionMessage> pred, ConsistencyLevel cl)
    {
        List<InetAddress> addresses = new ArrayList<InetAddress>();
        for (String Token : Tokens)
        {
            addresses.add(StorageService.instance.getAssociatedEndpoint(Token));
        }
        return sendMessageToNodes(addresses, msg, pred, cl, keyspace);
    }

    // Returns for a Collection of Keys (e.g. [A,'B',C]) the concatenated form used for CQL queries, i.e. "'A','B','C'"
    public static String concatenateKeysForCQL(Collection<ByteBuffer> keys)
    {
        String result = "";
        for (ByteBuffer key : keys)
        {
            String keyStr = ByteBufferUtil.stringWOException(key);
            if (keyStr.startsWith("'") && keyStr.endsWith("'"))
                    keyStr = keyStr.substring(1, keyStr.length()-1);
            assert !(keyStr.startsWith("'") || keyStr.endsWith("'"));
            result += "'" + keyStr + "',";
        }
        return result.substring(0, result.length()-1);
    }

    public static class IndirectionDataPredicate implements Predicate<IndirectionMessage>
    {
        private double value = -1;

        @Override
        public boolean apply(IndirectionMessage msg)
        {
            double diff = ByteBufferUtil.toDouble(msg.content);
            if (value < 0 && diff >= 0)
                value = diff;
            return diff >= 0;
        }
 
        public double getDiff()
        {
            return value > 0 ? value : 0;
        }
    }
}
