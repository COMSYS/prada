package org.apache.cassandra.annotation.loadbalancing;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.annotation.Constraints;

public class LRULoadBalancer extends LoadBalancer {

    private ArrayList<InetAddress> history = new ArrayList<>();


    public List<InetAddress> getNodes(Constraints c, int replicationFactor, String keyspace)
    {
        ArrayList<InetAddress> result = new ArrayList<>();

        // Put tokens never used at the top of the list
        for (InetAddress addr : c.getAllCandidateEndpoints())
        {
            if (!history.contains(addr))
                history.add(0, addr);
        }

        // Use the topmost tokens which satisfy the annotations as result
        for (InetAddress addr : history)
        {
            if (c.getAllCandidateEndpoints().contains(addr))
                result.add(addr);
            if (result.size() == replicationFactor)
                break;
        }

        assert(result.size() == replicationFactor);

        // Remove chosen tokens and append them at the end of the history
        for (InetAddress addr : result)
        {
            history.remove(addr);
            history.add(addr);
        }

        return result;
    }

}
