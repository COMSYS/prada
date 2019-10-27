package org.apache.cassandra.annotation.loadbalancing;

import java.util.*;

import org.apache.cassandra.cql3.UntypedResultSet;

public class SimpleScoringLoadBalancer extends AbstractScoringLoadBalancer {

    protected Map<String, Double> calculateScorings(HashMap<String, UntypedResultSet> abilities)
    {
        HashMap<String, Double> values = new HashMap<String, Double>();

        for (String name : abilities.keySet())
        {
            for (Iterator<UntypedResultSet.Row> RowIt = abilities.get(name).iterator(); RowIt.hasNext();)
            {
                UntypedResultSet.Row row = RowIt.next();
                double prev = 0;
                if (values.get(row.getString("id")) != null)
                    prev = values.get(row.getString("id"));
                values.put(row.getString("id"), prev + (row.hasCount()-1));
            }
        }
        return values;
    }

}
