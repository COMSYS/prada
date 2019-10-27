package org.apache.cassandra.annotation.loadbalancing;

import java.net.InetAddress;
import java.util.*;

import org.apache.cassandra.annotation.Constraints;

public class RandomLoadBalancer extends LoadBalancer {

    public List<InetAddress> getNodes(Constraints c, int replicationFactor, String keyspace)
    {
        ArrayList<InetAddress> list = new ArrayList<>(c.getAllCandidateEndpoints());
        ArrayList<InetAddress> result = new ArrayList<>();
        for (int i=0; i < replicationFactor; i++)
        {
            int rand = (int) (Math.random()*(list.size()));
            result.add(list.get(rand));
            list.remove(rand);
        }
        return result;
    }

}
