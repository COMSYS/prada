package org.apache.cassandra.annotation.loadbalancing;

import java.net.InetAddress;
import java.util.*;

import org.apache.cassandra.annotation.Constraints;

public class SimpleLoadBalancer extends LoadBalancer {

    public List<InetAddress> getNodes(Constraints c, int replicationFactor, String keyspace)
    {
        return new ArrayList<InetAddress>(c.getAllCandidateEndpoints()).subList(0, replicationFactor);
    }

}
