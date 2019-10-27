package org.apache.cassandra.annotation.loadbalancing;

import java.net.InetAddress;
import java.util.*;

import org.apache.cassandra.annotation.Constraints;
import org.apache.cassandra.annotation.IndirectionSupport;

public abstract class LoadBalancer {

    private static LoadBalancer instance = null;

    public abstract List<InetAddress> getNodes(Constraints c, int replicationFactor, String keyspace);


    public static LoadBalancer getInstance()
    {
        if (instance == null)
        {
            switch (IndirectionSupport.DataAnnotationConfiguration.loadBalancer)
            {
            case SimpleLoadBalancer:
                instance = new SimpleLoadBalancer();
                break;
            case RandomLoadBalancer:
                instance = new RandomLoadBalancer();
                break;
            case LRULoadBalancer:
                instance = new LRULoadBalancer();
                break;
            case SimpleScoringLoadBalancer:
                instance = new SimpleScoringLoadBalancer();
                break;
            case CounterMinLoadBalancer:
                instance = new CounterMinLoadBalancer();
            default:
                break;
            }
        }
        return instance;
    }

    public enum LoadBalancers
    {
        SimpleLoadBalancer, RandomLoadBalancer, SimpleScoringLoadBalancer, LRULoadBalancer, CounterMinLoadBalancer
    }

}
