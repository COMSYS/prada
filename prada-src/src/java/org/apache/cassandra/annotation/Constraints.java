package org.apache.cassandra.annotation;

import java.net.InetAddress;
import java.nio.*;
import java.util.*;

import org.apache.cassandra.annotation.loadbalancing.LoadBalancer;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public class Constraints {

    private List<InetAddress> targets;
    private HashSet<String> allCandidates;
    private HashSet<InetAddress> allCandidateEndpoints;
    private HashMap<String, HashSet<String>> parameters;
    private HashMap<InetAddress, ByteBuffer> tokens;
    private String annotationString;


    public Constraints()
    {
        this(new ArrayList<InetAddress>());
    }

    public Constraints(ArrayList<InetAddress> target)
    {
        this.targets = target;
        this.parameters = new HashMap<>();
        this.allCandidates = new HashSet<>();
        this.allCandidateEndpoints = new HashSet<>();
        this.tokens = new HashMap<>();
    }

    private void useDirectEndpoint(InetAddress a)
    {
        this.targets.add(a);
    }

    public void useDirect(String t)
    {
        InetAddress addr = StorageService.instance.getAssociatedEndpoint(t);
        tokens.put(addr, ByteBufferUtil.bytes(t));
        useDirectEndpoint(addr);
    }

    private void useDirectEndpoints(Collection<InetAddress> list)
    {
        assert (list.size() > 0);
        targets.addAll(list);
    }

    public void useDirect(Collection<String> list)
    {
        for (String s : list)
            useDirect(s);
    }

    public void useRegular()
    {
        this.targets = new ArrayList<>();
    }

    public void addAnnotationParameters(String key, ArrayList<String> params)
    {
        useRegular();
        parameters.put(key, new HashSet<String>(params));
        generateAnnotationString();
    }

    public boolean isDirect()
    {
        return (this.targets.size() > 0);
    }

    public boolean hasAnnotations()
    {
        return (parameters.size() > 0);
    }

    public List<InetAddress> getTargets()
    {
        assert (this.isDirect());
        return targets;
    }

    public List<String> getTargetsAsString()
    {
        ArrayList<String> result = new ArrayList<String>();
        for (InetAddress entry : targets)
            result.add(entry.getHostAddress());
        return result;
    }

    public HashSet<String> getAllCandidates()
    {
        return allCandidates;
    }

    public HashSet<InetAddress> getAllCandidateEndpoints()
    {
        return allCandidateEndpoints;
    }

    public Map<InetAddress, ByteBuffer> getTokens()
    {
        return tokens;
    }

    public Map<String, HashSet<String>> getParameters()
    {
        return parameters;
    }

    public void makeDirectWithAnnotations(int replicationFactor, String keyspace, ByteBuffer key) throws RequestValidationException
    {
        if (!isDirect() && hasAnnotations())
        {
            allCandidates = getSatisfyingNodes();
            allCandidateEndpoints = new HashSet<>();
            for (String target : allCandidates)
            {
                InetAddress addr = StorageService.instance.getAssociatedEndpoint(target);
                assert(addr!=null);
                allCandidateEndpoints.add(addr);
                tokens.put(addr, ByteBufferUtil.bytes(target));
            }

            if (allCandidateEndpoints.size() < replicationFactor)
                throw new InvalidRequestException("Not enough nodes present satisfiying the Data Annotiations to achieve replication factor " + replicationFactor + ". Annotations: " + annotationString + ". Endpoint: " + FBUtilities.getBroadcastAddress().getHostAddress() + ". allCandidates: " + allCandidateEndpoints.toString());

            if (!allCandidateEndpoints.isEmpty())
            {
                List<InetAddress> TokensAfterLoadBalancing;
                if (allCandidateEndpoints.size() == replicationFactor)
                    TokensAfterLoadBalancing = new ArrayList<>(allCandidateEndpoints);
                else
                {
                    List<InetAddress> cAddresses = StorageService.instance.getNaturalEndpoints(keyspace, key);
                    if (allCandidateEndpoints.contains(cAddresses))
                        TokensAfterLoadBalancing = cAddresses;
                    else
                        TokensAfterLoadBalancing = LoadBalancer.getInstance().getNodes(this, replicationFactor, keyspace);
                }
                useDirectEndpoints(TokensAfterLoadBalancing);
            }
        }

        IndirectionSupport.sendToLog("Set of nodes satisfying the Data Annotations: " + allCandidates.toString(), false, true, "DataAnnotationType");
        IndirectionSupport.sendToLog("Constraints are now: " + this.toString(), false, true, "DataAnnotationType");
    }

    public HashSet<String> getSatisfyingNodes() throws RequestValidationException
    {
      // Check for unsupported Data Annotations and throw exception if parameters are undefined
        for (String name : parameters.keySet())
        {
            if (IndirectionSupport.DataAnnotationConfiguration.list.get(name) == null)
                throw new InvalidRequestException("This node does not support the Data Annotation '" + name + "'!");

            IndirectionSupport.DataAnnotationConfiguration.list.get(name).validateAnnotation(parameters.get(name));
        }

        try
        {
            String firstname = parameters.keySet().iterator().next();
            HashSet<String> InitialSet = IndirectionSupport.DataAnnotationConfiguration.list.get(firstname).getSatisfyingNodes(parameters.get(firstname));

            for (String name : parameters.keySet())
            {
                if (name.equals(firstname))
                    continue;
                InitialSet.retainAll(IndirectionSupport.DataAnnotationConfiguration.list.get(name).getSatisfyingNodes(parameters.get(name)));
            }
            return InitialSet;

        }
        catch (RequestExecutionException e)
        {
            e.printStackTrace();
            throw new InvalidRequestException("An error occured during determining of nodes satisfying the data annotation. Check log!");
        }
    }

    public void mergeAnnotations(Constraints merge)
    {
        if (merge.hasAnnotations())
        {
            for (String annotation : merge.getParameters().keySet())
            {
                if (parameters.containsKey(annotation))
                    parameters.get(annotation).addAll(merge.getParameters().get(annotation));
                else
                    parameters.put(annotation, merge.getParameters().get(annotation));
            }
            minimizeAnnotationConstraints();
        }
    }

    private void minimizeAnnotationConstraints()
    {
        for (String annotation : parameters.keySet())
        {
            IndirectionSupport.DataAnnotationConfiguration.list.get(annotation).minimize(parameters.get(annotation));
        }
        generateAnnotationString();
    }

    private void generateAnnotationString()
    {
        if (parameters.size() > 0)
        {
            String result = "WITH ANNOTATIONS ";
            for (String annotation : parameters.keySet())
            {
                if (!result.equals("WITH ANNOTATIONS "))
                    result += " AND ";
                result += "\"" + annotation + "\"" + " = {";
                for (String value : parameters.get(annotation))
                {
                    result += " \"" + value + "\",";
                }
                // Delete last ","
                result = result.substring(0, result.length()-1);
                result += " }";
            }
            annotationString = result;
        }
        else
        {
            annotationString = "";
        }
    }

    public String getAnnotationsAsString()
    {
        return annotationString;
    }

    public String toString()
    {
        String str = "DataAnnotationType: ";
        if (isDirect())
        {
            str += "direct, Redirection to nodes ";
            for (InetAddress node : this.targets)
                str += node.getHostAddress() + ",";
            str = str.substring(0, str.length()-1);
        }
        else
        {
            str += "regular";
            for (String key : parameters.keySet())
            {
                str += ", Data Annotation " + key + ":";
                for (String p : parameters.get(key))
                    str += " " + p;
            }
        }
        return str;
    }

}
