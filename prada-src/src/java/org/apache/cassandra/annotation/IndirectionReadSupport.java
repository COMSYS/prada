package org.apache.cassandra.annotation;

import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.service.*;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.db.*;


public class IndirectionReadSupport {

    private static AbstractReadExecutor startReadExecutor(Row IndRow) throws UnavailableException
    {
        DecoratedKey DataKey = IndRow.key;
        String cfName = IndRow.cf.metadata().cfName;
        String KeySpaceName = IndRow.cf.metadata().ksName;
        ConsistencyLevel cl = ConsistencyLevel.ONE;

        assert (KeySpaceName.endsWith(IndirectionSupport.ReferenceKeyspacePostfix));

        IndirectionInformation refInfo = IndirectionInformation.RowToIndirectionInformation(IndRow);
        if (refInfo == null)
            return null;

        String KeyspaceNameIndirectionData = KeySpaceName.replace(IndirectionSupport.ReferenceKeyspacePostfix, IndirectionSupport.DataKeyspacePostfix);

        String targetString = "";
        for (ByteBuffer i : refInfo.getNodes())
            targetString += ByteBufferUtil.stringWOException(i) + ", ";
        IndirectionSupport.sendToLog("fetchRowForIndirectionRow: DataKey=" + DataKey + " cfName=" + cfName + " KeySpaceName=" + KeyspaceNameIndirectionData + " TargetNodes=" + targetString, false, true, "IndirectionReadSupport");

        SliceQueryFilter filter = new SliceQueryFilter(ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER, false, Integer.MAX_VALUE);
        SliceFromReadCommand cmd = new SliceFromReadCommand(KeyspaceNameIndirectionData, DataKey.key, cfName, System.currentTimeMillis(), filter);

        AbstractReadExecutor exec = AbstractReadExecutor.getReadExecutorForIndirectionRequest(cmd, cl, refInfo.getNodes());
        exec.executeAsync();
        return exec;
    }

    public static List<Row> fetchRowsForIndirectionRows(List<Row> IndRows) throws UnavailableException
    {
        List<AbstractReadExecutor> executors = new ArrayList<AbstractReadExecutor>();
        List<Row> Result = new ArrayList<Row>();
        for (Row row : IndRows)
        {
            if (row.cf.metadata().ksName.endsWith(IndirectionSupport.ReferenceKeyspacePostfix))
                assert(row.indirection);
            if (row.indirection)
            {
                assert(row.cf.metadata().ksName.endsWith(IndirectionSupport.ReferenceKeyspacePostfix));
                // This condition allows us distinguish between rows which are really deleted and rows whose key was reused and the tombstone is still set
                if (!(row.cf.asMap().size() == 0))
                {
                    AbstractReadExecutor ex = startReadExecutor(row);
                    if ( ex != null)
                        executors.add(ex);
                }
            }
            else
                Result.add(row);
        }
        for (AbstractReadExecutor exec : executors)
        {
            try
            {
                Row row = exec.get();
                if (row != null)
                    Result.add(row);
            }
            catch (DigestMismatchException e)
            {
                e.printStackTrace();
                throw new AssertionError(e); // full data requested from each node here, no digests should be sent
            }
            catch (ReadTimeoutException e)
            {
                throw new UnavailableException(ConsistencyLevel.ONE, 0, 0);
            }
        }
        return Result;
    }

    public static Row getIndirectionRow(Keyspace keyspace, QueryFilter filter)
    {
        if (!keyspace.getName().equals(Keyspace.SYSTEM_KS) && !keyspace.getName().equals("system_traces") && !keyspace.getName().equals(IndirectionSupport.DataAnnotationAbilities))
        {
            if (keyspace.getName().endsWith(IndirectionSupport.DataKeyspacePostfix))
            {
                return getIndirectionDataRow(keyspace, filter);
            }
            else
            {
                return getIndirectionReferenceRow(keyspace, filter);
            }
        }
        return null;
    }

    private static Row getIndirectionReferenceRow(Keyspace keyspace, QueryFilter filter)
    {
        // Only check for user keyspaces
        if (IndirectionSupport.isRegularKeyspace(keyspace.getName()))
        {
            Collection<ColumnFamilyStore> CFs = (Keyspace.open(keyspace.getName() + IndirectionSupport.ReferenceKeyspacePostfix)).getColumnFamilyStores();
            for (ColumnFamilyStore CFS : CFs)
            {
                if (CFS.name.equals(filter.cfName))
                {
                    Row row = (Keyspace.open(keyspace.getName() + IndirectionSupport.ReferenceKeyspacePostfix)).getRow(filter);
                    if (row.cf != null)
                    {
                        IndirectionSupport.sendToLog("Found Indirection Information for " + keyspace.getName() + "." + filter.cfName + " with key " + filter.key.toString(), false, true, "IndirectionReadSupport");
                        Row IndRow = new Row(filter.key, row.cf, true);
                        return IndRow;
                    }
                }
            }
        }
        // Return null if no Indirection Information exists!
        IndirectionSupport.sendToLog("No Indirection Information found for " + keyspace.getName() + "." + filter.cfName + " with key " + filter.key.toString(), false, true, "IndirectionReadSupport");
        return null;
    }

    private static Row getIndirectionDataRow(Keyspace keyspace, QueryFilter filter)
    {
        Collection<ColumnFamilyStore> CFs = Keyspace.open(keyspace.getName()).getColumnFamilyStores();
        for (ColumnFamilyStore CFS : CFs)
        {
            if (CFS.name.equals(filter.cfName))
            {
                Row row = (Keyspace.open(keyspace.getName())).getRow(filter);
                return row;
            }
        }
        return null;
    }

    public static List<Row> getIndirectionReferenceRows(String keyspace, String columnFamily, ExtendedFilter exFilter, List<IndexExpression> rowFilter)
    {
        List<Row> Result = new ArrayList<Row>();
        if (IndirectionSupport.isRegularKeyspace(keyspace))
        {
            ColumnFamilyStore cfs = Keyspace.open(keyspace + IndirectionSupport.ReferenceKeyspacePostfix).getColumnFamilyStore(columnFamily);

            List<Row> result = new ArrayList<Row>();
            if (cfs.indexManager.hasIndexFor(rowFilter))
                result = cfs.search(exFilter);
            else
                result = cfs.getRangeSlice(exFilter);

            for (Row row : result)
            {
                IndirectionSupport.sendToLog("Found Indirection Information for " + keyspace + "." + columnFamily + " with key " + row.key.toString(), false, true, "IndirectionReadSupport");
                Result.add(new Row(row.key, row.cf, true));
            }
            return Result;
        }
        else
        {
            if (keyspace != Keyspace.SYSTEM_KS && keyspace != "system_traces") {
                IndirectionSupport.sendToLog("No Indirection Information found for " + keyspace + "." + columnFamily, false, true, "IndirectionReadSupport");
            }
            return Result;
        }
    }

    public static List<String> getIndirectionReplicas(String keyspace, String table, ByteBuffer key)
    {
        IndirectionInformation refInfo = IndirectionInformation.getExistingIndirectionInformation(keyspace, table, ByteBufferUtil.stringWOException(key));

        if (refInfo != null)
        {
            assert(refInfo.getReplicaCount() == Keyspace.open(keyspace).getReplicationStrategy().getReplicationFactor());
            IndirectionSupport.sendToLog("Queried replicas for key '" + ByteBufferUtil.stringWOException(key) + "' in " + "table '" + keyspace + "." + table, false, true, "IndirectionReadSupport");
            return refInfo.getNodesAsString();
        }
        else
        {
            IndirectionSupport.sendToLog("No replicas found for key '" + ByteBufferUtil.stringWOException(key) + "' in " + "table '" + keyspace + "." + table, false, true, "IndirectionReadSupport");
            return new ArrayList<String>();
        }
    }

}
