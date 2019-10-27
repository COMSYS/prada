package org.apache.cassandra.annotation;

import java.util.*;

import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;

public class ConsistencyCheckStatement extends ParsedStatement implements CQLStatement {

    private final String keyspace;
    private final String cf;
    private final String key;
    private final boolean repair;

    public ConsistencyCheckStatement(String keyspace, String cf, String key, boolean repair)
    {
        this.keyspace = keyspace;
        this.cf = cf;
        this.key = key;
        this.repair = repair;
    }

    @Override
    public int getBoundTerms()
    {
        return 0;
    }

    @Override
    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException {}

    @Override
    public void validate(ClientState state) throws RequestValidationException
    {
        if (Schema.instance.getKeyspaceInstance(keyspace) == null)
            throw new InvalidRequestException("Keyspace " + keyspace + " does not exist!");
        if (Schema.instance.getId(keyspace, cf) == null)
            throw new InvalidRequestException("ColumnFamily " + cf + " does not exist!");
    }

    @Override
    public ResultMessage execute(QueryState state, QueryOptions options) throws RequestValidationException, RequestExecutionException
    {
        int goodCount = 0;
        int badCount = 0;

        System.out.println("------------------------------Consistency Check START------------------------------");
        System.out.println("Start Consistency Check for keyspace: " + keyspace + ", ColumnFamily: " + cf + ", Key: " + key);

        UntypedResultSet referenceTable = QueryProcessor.process("SELECT * FROM " + keyspace + IndirectionSupport.ReferenceKeyspacePostfix + "." + cf + ";", ConsistencyLevel.ONE);
        List<IndirectionInformation> refInfos = getRelevantRows(referenceTable);

        // Perform Forward Check, i.e., check whether we have a data row for each Indirection Row
        for (IndirectionInformation refInfo: refInfos)
        {
            String query = "SELECT * FROM " + keyspace + IndirectionSupport.DataKeyspacePostfix + "." + cf + " WHERE " + key + " IN ('" + refInfo.getKey() + "');";
            UntypedResultSet chk = QueryProcessor.process(query, ConsistencyLevel.ONE);
            if (chk.size() == 1)
                goodCount++;
            else
                badCount++;
        }
        System.out.println("Finished Forward Consistency Check for " + refInfos.size() + " relevant rows! Summary: " + goodCount + " rows OK, " + badCount + " rows inconsistent");

        // Perform Reverse Check, i.e., check whether we have an indirection row for each Data Row
        String query = "SELECT * FROM " + keyspace + IndirectionSupport.DataKeyspacePostfix + "." + cf + ";";
        UntypedResultSet chk = QueryProcessor.process(query, ConsistencyLevel.ONE);
        int orphanedCount = chk.size() - refInfos.size();

        System.out.println("Finished Reverse Consistency Check for " + chk.size() + " rows! Summary: " + orphanedCount + " orphaned rows found!");

        if (badCount == 0 && orphanedCount == 0)
        {
            System.out.println("Finished Consistency Check without errors!");
        }
        else
            System.out.println("Finished Consistency Check with errors, check log!");

        if (orphanedCount > 0)
        {
            if (repair)
            {
                System.out.println("Start repair on orphaned rows");
                List<UntypedResultSet.Row> orphanedRows = getOrphanedRows(chk, refInfos);
                assert(orphanedCount == orphanedRows.size());
                for (UntypedResultSet.Row row : orphanedRows)
                {
                    assert(row.has(key));
                    String primaryKey = row.getString(key);
                    QueryProcessor.process("DELETE FROM " + keyspace + IndirectionSupport.DataKeyspacePostfix + "." + cf + " WHERE " + key + " IN ('" + primaryKey + "');", ConsistencyLevel.ONE);
                }
                System.out.println("Successfully repaired " + orphanedRows.size() + " orphaned rows!");
            }
            else
                System.out.println("Run REPAIR CONSISTENCY to repair orphaned rows");
        }
        else
            System.out.println("No orphaned rows found, skipping repair!");

        System.out.println("-------------------------------Consistency Check END-------------------------------");
        return new ResultMessage.Void();
    }

    @Override
    public ResultMessage executeInternal(QueryState state) throws RequestValidationException, RequestExecutionException
    {
        return null;
    }

    @Override
    public Prepared prepare() throws RequestValidationException
    {
        return new Prepared(this);
    }

    private List<IndirectionInformation> getRelevantRows(UntypedResultSet set)
    {
        List<IndirectionInformation> result = new ArrayList<IndirectionInformation>();
        for (UntypedResultSet.Row row : set)
        {
            IndirectionInformation refInfo = new IndirectionInformation(row);
            if (refInfo.selfIsReplica())
                result.add(refInfo);
        }
        return result;
    }

    private List<UntypedResultSet.Row> getOrphanedRows(UntypedResultSet dataRows, List<IndirectionInformation> refInfos)
    {
        List<UntypedResultSet.Row> result = new ArrayList<UntypedResultSet.Row>();
        for (UntypedResultSet.Row row : dataRows)
        {
            boolean ok = false;
            for (IndirectionInformation refInfo : refInfos)
            {
                if (row.getString(key).equals(refInfo.getKey()))
                {
                    ok = true;
                    break;
                }
            }
            if (!ok)
                result.add(row);
        }
        return result;
    }

}
