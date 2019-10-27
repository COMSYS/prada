package org.apache.cassandra.annotation.dataannotations;

import java.util.*;

import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.annotation.*;

public class BoolDataAnnotation extends AbstractDataAnnotation<Boolean>
{

    public BoolDataAnnotation(String name, boolean value)
    {
        super(name, new HashSet<Boolean>(), 1);
        this.supportedValues.add(value);
    }

    public String createINSERTstatement()
    {
        return "INSERT INTO " + IndirectionSupport.DataAnnotationAbilities + "." + this.name + " ( id, value ) VALUES ( '" + getOwnToken() + "', " + (supportedValues.contains(true)) + " );";
    }

    public String createCreateTablestatement()
    {
        return "CREATE TABLE " + IndirectionSupport.DataAnnotationAbilities + "." + this.name + " ( id varchar PRIMARY KEY, value boolean );";
    }

    public boolean hasAbilities()
    {
        return (supportedValues.contains(true));
    }

    public HashSet<String> getSatisfyingNodes(Set<String> params) throws RequestExecutionException
    {
        String query = "SELECT * FROM " + IndirectionSupport.DataAnnotationAbilities + "." + this.name + ";";
        HashSet<String> set = new HashSet<String>();

        UntypedResultSet result = QueryProcessor.processInternal(query, IndirectionSupport.DataAnnotationAbilities);

        for (Iterator<UntypedResultSet.Row> it = result.iterator(); it.hasNext();)
        {
            UntypedResultSet.Row row = it.next();
            if (row.getBoolean("value") == true)
                set.add(row.getString("id"));
        }
        return set;
    }

    public void validateAnnotation(Set<String> params) throws RequestValidationException
    {
        for (String param : params)
        {
            if (!(param.equals("true") || param.equals("false")))
                throw new InvalidRequestException("Boolean Data Annotation '" + this.name + "' contains undefined values!");
            if (param.equals("false"))
                throw new InvalidRequestException("Boolean Data Annotation '" + this.name + "' must be omitted or parameter must be 'true'!");
        }
    }

    @Override
    public void minimize(Set<String> set)
    {
        // noop
    }

}
