package org.apache.cassandra.annotation;

import java.util.*;

import org.apache.cassandra.annotation.dataannotations.*;
import org.apache.cassandra.annotation.dataannotations.IntegerDataAnnotation.IntegerComparators;
import org.apache.cassandra.annotation.loadbalancing.*;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.yaml.snakeyaml.*;

public class DataAnnotationConfig {

    public HashMap<String, AbstractDataAnnotation<?>> list;
    public LoadBalancer.LoadBalancers loadBalancer;
    public int repFactor;
    public String repStrategy;


    public DataAnnotationConfig(int repfactor, String repstrategy, String loadbalancer, HashMap<String, AbstractDataAnnotation<?>> annotations)
    {
        this.repFactor = repfactor;
        this.repStrategy = repstrategy;
        this.loadBalancer = LoadBalancer.LoadBalancers.valueOf(loadbalancer);
        this.list = annotations;
    }

    public DataAnnotationConfig()
    {
        this.list = new HashMap<String, AbstractDataAnnotation<?>>();
        this.loadBalancer = LoadBalancer.LoadBalancers.SimpleLoadBalancer;
        this.repFactor = 1;
        this.repStrategy = "SimpleStrategy";
    }

    public String toString()
    {
        DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        Yaml yaml = new Yaml(options);
        return yaml.dump(this);
    }

    public void announceCapabilities()
    {
        try
        {
            for (String name : list.keySet())
            {
                AbstractDataAnnotation<?> annotation = list.get(name);
                if (Schema.instance.getId(IndirectionSupport.DataAnnotationAbilities, name) == null)
                {
                    IndirectionSupport.sendToLog("Table for Data Annotation '" + name + "' does not exist! Create it...", false, true, "DataAnnotationConfig");
                    QueryProcessor.process(annotation.createCreateTablestatement(), ConsistencyLevel.ONE);
                }
                if (annotation.hasAbilities())
                {
                    IndirectionSupport.sendToLog("Announce Abilities for Data Annotation '" + name + "'", false, true, "DataAnnotationConfig");
                    QueryProcessor.process(annotation.createINSERTstatement(), ConsistencyLevel.ANY);
                }
            }
        }
        catch(RequestExecutionException e)
        {
            IndirectionSupport.sendToLog("An error occured during announcing Data Annotation Abilities: " + e.getMessage(), true, false, "DataAnnotationConfig");
            e.printStackTrace();
            System.exit(1);
        }
    }

    public void dropAbility(String annotation)
    {
        assert(list.containsKey(annotation));
        IndirectionSupport.sendToLog("Drop Ability for Data Annotation '" + annotation + "' from Table", false, true, "DataAnnotationConfig");
        try
        {
            QueryProcessor.process("DELETE from " + IndirectionSupport.DataAnnotationAbilities + "." + annotation + " WHERE id IN ( '" + list.get(annotation).getOwnToken() + "' );", ConsistencyLevel.ANY);
        }
        catch(RequestExecutionException e)
        {
            IndirectionSupport.sendToLog("An error occured during dropping the Data Annotation Abilities for '" + annotation + "': " + e.getMessage(), true, false, "DataAnnotationConfig");
            e.printStackTrace();
        }

    }

    public void dropAbilities()
    {
        for (String name : list.keySet())
            dropAbility(name);
    }

}
