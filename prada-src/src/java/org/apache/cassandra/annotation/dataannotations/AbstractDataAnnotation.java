package org.apache.cassandra.annotation.dataannotations;

import java.util.*;

import org.apache.cassandra.annotation.IndirectionSupport;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;

public abstract class AbstractDataAnnotation<T>
{
    public String name;
    public Collection<T> supportedValues;
    public final int max;

    public AbstractDataAnnotation(String name, Collection<T> supportedValues, int max)
    {
        if (supportedValues.size() > max)
        {
            supportedValues = new HashSet<T>();
            IndirectionSupport.sendToLog("Number of supported values exceeds maximum! Proceed without support for " + name + " Data Annotation!", true, false, "AbstractDataAnnotation");
        }
        this.name = name;
        this.supportedValues = supportedValues;
        this.max = max;
    }

    public String getOwnToken()
    {
        assert (DatabaseDescriptor.getInitialTokens().iterator().hasNext());
        // Returns with first Token
        return DatabaseDescriptor.getInitialTokens().iterator().next();
    }


    public abstract String createINSERTstatement();
    public abstract String createCreateTablestatement();
    public abstract boolean hasAbilities();
    public abstract HashSet<String> getSatisfyingNodes(Set<String> params) throws RequestExecutionException;
    public abstract void validateAnnotation(Set<String> params) throws RequestValidationException;
    public abstract void minimize(Set<String> set);


}
