package org.apache.cassandra.annotation;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.cassandra.annotation.messages.AsyncUpdateReferencesCallback;
import org.apache.cassandra.annotation.messages.IndirectionMessage;
import org.apache.cassandra.annotation.messages.IndirectionUpdateReferencesMessage;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.cql3.statements.UpdateStatement;
import org.apache.cassandra.db.*;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.LoadBroadcaster;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;


public class IndirectionInformation {

    public static final String AnnotationRowName = "saved_annotations";

    private final String key;
    private final ArrayList<ByteBuffer> nodes = new ArrayList<>();
    private final String keyspace;
    private final String columnFamily;
    private final String annotationString;

    public IndirectionInformation(UntypedResultSet.Row row)
    {
        this(row, row.getColumns().get(0).ksName, row.getColumns().get(0).cfName);
    }

    public IndirectionInformation(String key, String ks, String cf, Constraints c)
    {
        this.key = key;
        this.keyspace = ks;
        this.columnFamily = cf;
        this.annotationString = c.getAnnotationsAsString();
        for (InetAddress addr : c.getTargets())
            this.nodes.add(c.getTokens().get(addr));
    }

    private IndirectionInformation(UntypedResultSet.Row row, String ks, String cf)
    {
        assert(row.has("key"));
        assert(row.has(IndirectionInformation.AnnotationRowName));

        this.key = row.getString("key");
        this.keyspace = ks;
        this.columnFamily = cf;

        assert(keyspace.endsWith(IndirectionSupport.ReferenceKeyspacePostfix));

        for (int i=1; i<row.hasCount()-1; i++)
        {
            assert(row.has("node" + i));
            this.nodes.add(row.getBytes("node" + i));
        }
        assert(getNodes().size() == row.hasCount() - 2 );
        this.annotationString = row.getString(IndirectionInformation.AnnotationRowName);
    }

    public static IndirectionInformation RowToIndirectionInformation(Row row)
    {
        HashMap<String, ByteBuffer> columns = new HashMap<String, ByteBuffer>();
        Iterator<Column> it = row.cf.iterator();
        while (it.hasNext())
        {
            Column col = it.next();
            String name = ByteBufferUtil.stringWOException(col.name()).replaceAll("[^\\p{Print}]", "");
            if (name.length() > 0)
            {
                assert(name.startsWith("node") || name.equals(IndirectionInformation.AnnotationRowName));
                columns.put(name, col.value());
            }
            else
            {
                columns.put("key", row.key.key);
            }
        }
        return (columns.size() < 2 ? null : new IndirectionInformation(new UntypedResultSet.Row(columns), row.cf.metadata().ksName, row.cf.metadata().cfName));
    }

    public boolean updateReferenceInformation() throws IndirectionException
    {
        CFMetaData cfMetaData = Schema.instance.getKSMetaData(keyspace).cfMetaData().get(columnFamily);
        Token t = StorageService.getPartitioner().getToken(cfMetaData.getKeyValidator().fromString(key));
        List<InetAddress> addresses = StorageService.instance.getLiveNaturalEndpoints(Keyspace.open(keyspace + IndirectionSupport.ReferenceKeyspacePostfix), t);
        if (addresses.isEmpty())
            throw new IndirectionException("No node holding the corresponding indirection information is available.");
        StringBuilder sb = new StringBuilder();
        List<String> targetNodes = getNodesAsString();
        sb.append(targetNodes.size()); sb.append(';');
        for(String str : targetNodes)
        {
            sb.append(str); sb.append(';');
        }
        sb.append(keyspace); sb.append(';');
        sb.append(columnFamily); sb.append(';');
        sb.append(key); sb.append(';');
        sb.append(annotationString);
        MessageOut<IndirectionMessage> msgout = (new IndirectionUpdateReferencesMessage(ByteBufferUtil.bytes(sb.toString()))).createMessage();
        boolean newReferences = true;
        try
        {
            AsyncUpdateReferencesCallback cb = new AsyncUpdateReferencesCallback();
            MessagingService.instance().sendRR(msgout, addresses.get(0), cb);
            cb.waitForResponses(msgout.getTimeout(), TimeUnit.MILLISECONDS);
            double diff = cb.getSize();
            if (diff < 0)
            {
                throw new IndirectionException("Error updating indirection information: " + cb.getError());
            }
            if (diff > 0)
            {
                InetAddress ownAddr = FBUtilities.getBroadcastAddress();
                for (InetAddress target : addresses)
                {
                    if (!ownAddr.equals(target))
                        LoadBroadcaster.instance.updateLoadCache(target, diff);
                }
            }
            else
                newReferences = false;
        } catch (TimeoutException e)
        {
            throw new IndirectionException("No node holding indirection information is avaiable.");
        }
        return newReferences;
    }

    public static IndirectionInformation getExistingIndirectionInformation(String keyspace, String cf, String Key)
    {
        if (keyspace.equals(Keyspace.SYSTEM_KS) || keyspace.equals("system_traces") || keyspace.equals(IndirectionSupport.DataAnnotationAbilities) || keyspace.endsWith(IndirectionSupport.ReferenceKeyspacePostfix) || keyspace.endsWith(IndirectionSupport.DataKeyspacePostfix)) {
            return null;
	}
        String mks = keyspace + IndirectionSupport.ReferenceKeyspacePostfix;
        UntypedResultSet referenceInfo = QueryProcessor.processInternal("SELECT * FROM " + mks + "." + cf + " WHERE key IN ('" + Key + "');", mks);
        assert(referenceInfo.size() == 0 || referenceInfo.size() == 1);
        if (referenceInfo.size() == 1)
        {
            IndirectionInformation refInfo = new IndirectionInformation(referenceInfo.one(), keyspace + IndirectionSupport.ReferenceKeyspacePostfix, cf);
            assert(refInfo.getKey().equals(Key));
            return refInfo;
        }
        return null;
    }

    // Returns, whether requesting node is a replica node
    public boolean selfIsReplica()
    {
        for (String node : getNodesAsString())
        {
            if (DatabaseDescriptor.getInitialTokens().contains(node))
                    return true;
        }
        return false;
    }

    public int getReplicaCount()
    {
        return nodes.size();
    }

    public String getKey()
    {
        return key;
    }

    public ArrayList<ByteBuffer> getNodes()
    {
        return nodes;
    }

    public ArrayList<String> getNodesAsString()
    {
        ArrayList<String> result = new ArrayList<String>();
        for (ByteBuffer entry : nodes)
            result.add(ByteBufferUtil.stringWOException(entry));
        return result;
    }

    public List<InetAddress> getEndpoints()
    {
        List<InetAddress> result = new ArrayList<InetAddress>();
        for (ByteBuffer entry : nodes)
            result.add(StorageService.instance.getAssociatedEndpoint(ByteBufferUtil.stringWOException(entry)));
        return result;
    }

    public String getKeyspace()
    {
        return keyspace;
    }

    public String getCFname()
    {
        return columnFamily;
    }

    public static String getInsertQuery(List<String> nodes, String keyspace, String columnFamily, String key, String annotationString)
    {
        String colNames = "";
        String Tokens = "";
        for (int i=0; i < nodes.size(); i++)
        {
            colNames += ", node" + (i+1);
            Tokens += ", '" + nodes.get(i) + "'";
        }
        colNames += ", " + IndirectionInformation.AnnotationRowName;
        Tokens += ", '" + annotationString.replaceAll("'", "\"") + "'";
        return "INSERT INTO " + keyspace + IndirectionSupport.ReferenceKeyspacePostfix + "." + columnFamily + " (Key" + colNames + ") VALUES ('" + key + "'" + Tokens + ");";
    }

    public String getInsertQuery()
    {
        String colNames = "";
        String Tokens = "";
        for (int i=0; i < nodes.size(); i++)
        {
            colNames += ", node" + (i+1);
            Tokens += ", '" + ByteBufferUtil.stringWOException(nodes.get(i)) + "'";
        }
        colNames += ", " + IndirectionInformation.AnnotationRowName;
        Tokens += ", '" + annotationString.replaceAll("'", "\"") + "'";
        return "INSERT INTO " + keyspace + IndirectionSupport.ReferenceKeyspacePostfix + "." + columnFamily + " (Key" + colNames + ") VALUES ('" + key + "'" + Tokens + ");";
    }

    public Constraints getConstraints()
    {
        // We use dummy values for keyspace, cf and values since we are only interested in the Constraints
        String queryStr = "INSERT INTO dummy.dummy (dummy) VALUES ('dummy') " + annotationString.replaceAll("\"", "'") + ";";
        try
        {
            ParsedStatement stmt = QueryProcessor.parseStatement(queryStr);
            assert(stmt instanceof UpdateStatement.ParsedInsert);
            return ((UpdateStatement.ParsedInsert)stmt).annotations;
        }
        catch (SyntaxException e)
        {
            e.printStackTrace();
            return null;
        }
    }

    public String toString()
    {
        return "Reference Information for " + keyspace + "." + columnFamily + " and key " + key + ": " + nodes.toString() + " [" + annotationString + "]";
    }
}
