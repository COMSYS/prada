package org.apache.cassandra.annotation.loadbalancing;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.TreeSet;

import org.apache.cassandra.annotation.Constraints;
import org.apache.cassandra.service.LoadBroadcaster;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CounterMinLoadBalancer extends LoadBalancer
{
    private static final Logger logger = LoggerFactory.getLogger(CounterMinLoadBalancer.class);

    @Override
    public ArrayList<InetAddress> getNodes(Constraints c, int replicationFactor, String keyspace)
    {
        TreeSet<Pair<Double, InetAddress>> set = new TreeSet<>(new Comp());
        for (InetAddress addr : c.getAllCandidateEndpoints())
        {
            Double load = LoadBroadcaster.instance.getLoadInfo().get(addr);
            if (load != null)
            {
                Double cache = LoadBroadcaster.instance.getLoadInfoCache().get(addr).get();
                assert(cache != null);
                set.add(Pair.of(load + cache, addr));
            }
            else
                logger.warn("getLoadInfo() failed, endpoint: " + addr.getHostAddress());
        }
        ArrayList<InetAddress> nodes = new ArrayList<>();
        Iterator<Pair<Double, InetAddress>> itr = set.iterator();
        for (int i = 0; (i < replicationFactor && itr.hasNext()); ++i)
            nodes.add(itr.next().getRight());
        return nodes;
    }

    public static class Comp implements Comparator<Pair<Double, InetAddress>>
    {

        @Override
        public int compare(Pair<Double, InetAddress> o1, Pair<Double, InetAddress> o2)
        {
            if (o1.getLeft() < o2.getLeft())
                return -1;
            else if (o1.getLeft() > o2.getLeft())
                return 1;
            else
            {
                return compare(o1.getRight(), o2.getRight());
            }
        }

        public int compare(InetAddress o1, InetAddress o2)
        {
            byte[] a1 = o1.getAddress();
            byte[] a2 = o2.getAddress();
            int l = a1.length < a2.length ? a1.length : a2.length;
            for (int i = 0; i < l; ++i)
            {
                if (a1[i] < a2[i])
                    return -1;
                else if (a2[i] < a1[i])
                    return 1;
            }
            if (a1.length < a2.length)
                return -1;
            else if (a2.length < a1.length)
                return 1;
            return 0;
        }
    }
}
