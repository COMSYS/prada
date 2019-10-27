package org.apache.cassandra.annotation.loadbalancing;

import java.net.InetAddress;
import java.util.*;

import org.apache.cassandra.annotation.*;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.service.StorageService;

public abstract class AbstractScoringLoadBalancer extends LoadBalancer {

    public Date lastUpdate;
    public TreeMap<String, Double> Scorings;
    // Defines the time (in ms) after which a scoring update is enforced
    public static final int updateTolerance = 3600000;


    public AbstractScoringLoadBalancer()
    {
        this.lastUpdate = new Date(0);
        this.Scorings = new TreeMap<String, Double>();
    }

    public List<InetAddress> getNodes(Constraints c, int replicationFactor, String keyspace)
    {
        assert (c.getAllCandidateEndpoints().size() >= replicationFactor);

        HashSet<String> Tokens = c.getAllCandidates();
        Date currentTime = new Date();
        if (lastUpdate.getTime() == 0 || ((currentTime.getTime() - lastUpdate.getTime()) > updateTolerance ) || !Scorings.keySet().containsAll(Tokens))
        {
            try
            {
                Set<String> TokensToUpdate = new HashSet<String>(Tokens);
                // If we do not update due to too big time difference to last update, we can only update Tokens for which we haven't any Scoring yet
                if (!((currentTime.getTime() - lastUpdate.getTime()) > updateTolerance ))
                {
                    Set<String> intersection = new HashSet<String>(Scorings.keySet());
                    intersection.retainAll(Tokens);
                    TokensToUpdate.removeAll(intersection);
                }
                updateScorings(TokensToUpdate);
                lastUpdate = currentTime;
            }
            catch (RequestExecutionException e)
            {
                IndirectionSupport.sendToLog("Error updating Scorings! Fall back to simple mode...", true, false, "AbstractScoringLoadBalancer");
                e.printStackTrace();
                ArrayList<String> result = new ArrayList<String>(Tokens);
                return new ArrayList<InetAddress>(c.getAllCandidateEndpoints()).subList(0, replicationFactor);
            }
        }

        ArrayList<InetAddress> result = new ArrayList<>();
        TreeMap<String, Double> tmp = new TreeMap<String, Double>();
        for (String token : Tokens)
            tmp.put(token, Scorings.get(token));

        assert(Tokens.size() == tmp.size() && Scorings.keySet().containsAll(Tokens));

        // Select the elements with smallest Scoring, if we want the elements with highest Scoring, calculateScorings is responsible to calculate reciprocal values
        Map.Entry<String, Double> smallest = null;
        while(result.size() < replicationFactor)
        {
            for (Map.Entry<String, Double> entry : tmp.entrySet())
            {
                if (smallest == null || smallest.getValue() > entry.getValue())
                    smallest = entry;
            }
            result.add(StorageService.instance.getAssociatedEndpoint(smallest.getKey()));
            tmp.remove(smallest.getKey());
        }

        assert(result.size() == replicationFactor);
        assert(Tokens.containsAll(result));
        assert(tmp.size() + result.size() == Tokens.size());

        return result;
    }

    private void updateScorings(Set<String> Tokens) throws RequestExecutionException
    {
        IndirectionSupport.sendToLog("Updating Scorings for Tokens " + Tokens.toString(), false, true, "AbstractScoringLoadBalancer");

        HashMap<String, UntypedResultSet> abilities = new HashMap<String, UntypedResultSet>();
        String tokenstring = "";
        for (String s : Tokens)
        {
            if (tokenstring != "")
                tokenstring += ", ";
            tokenstring += "'" + s + "'";
        }

        for (String name : IndirectionSupport.DataAnnotationConfiguration.list.keySet())
        {
            String queryStr = "SELECT * FROM " + IndirectionSupport.DataAnnotationAbilities + "." + name + " WHERE id IN ( " + tokenstring + " );";
            abilities.put(name, QueryProcessor.process(queryStr, ConsistencyLevel.ONE));
        }

        assert(abilities.size() == IndirectionSupport.DataAnnotationConfiguration.list.size());

        Scorings.putAll(calculateScorings(abilities));
    }

    protected Map<String, Double> getReciprocalMap(Map<String, Double> values)
    {
        for (String s : values.keySet())
            values.put(s, 1/values.get(s));
        return values;
    }

    protected abstract Map<String, Double> calculateScorings(HashMap<String, UntypedResultSet> abilities);

}
