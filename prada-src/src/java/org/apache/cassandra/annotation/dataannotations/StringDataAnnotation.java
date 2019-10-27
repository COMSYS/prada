package org.apache.cassandra.annotation.dataannotations;

import java.util.*;

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.annotation.*;

public class StringDataAnnotation extends AbstractDataAnnotation<String> {

    public final HashMap<String, HashSet<String>> collectionValues;


    public StringDataAnnotation(String name, int max, HashSet<String> supported, HashMap<String, HashSet<String>> collectionValues)
    {
        super(name, supported, max);
        if (collectionValues == null)
            collectionValues = new HashMap<String, HashSet<String>>();
        this.collectionValues = collectionValues;
    }

    public String createINSERTstatement()
    {
        assert (max > 0 && supportedValues.size() <= max);
        String str = "INSERT INTO " + IndirectionSupport.DataAnnotationAbilities + "." + this.name + " ( id";
        String str2 = "";
        Iterator<String> it = supportedValues.iterator();
        for (int i=0; i < supportedValues.size(); i++)
        {
            str += ", " + "value" + i;
            str2 += ", '" + it.next() + "'";
        }
        str += " ) VALUES ( '" + getOwnToken() + "' " + str2;
        return str + " );";
    }

    public String createCreateTablestatement()
    {
        assert (max > 0 && supportedValues.size() <= max);
        String str = "CREATE TABLE " + IndirectionSupport.DataAnnotationAbilities + "." + this.name + " ( id varchar PRIMARY KEY";
        for (int i = 0; i < max; i++)
        {
            str += ", value" + i + " varchar";
        }
        return  str + " );";
    }

    public boolean hasAbilities()
    {
        return (this.supportedValues.size() > 0);
    }

    public HashSet<String> getSatisfyingNodes(Set<String> params) throws RequestExecutionException
    {
        String query = "SELECT * FROM " + IndirectionSupport.DataAnnotationAbilities + "." + this.name + ";";
        HashSet<String> set = new HashSet<String>();

        UntypedResultSet result = QueryProcessor.processInternal(query, IndirectionSupport.DataAnnotationAbilities);

        for (Iterator<UntypedResultSet.Row> it = result.iterator(); it.hasNext();)
        {
            UntypedResultSet.Row row = it.next();

loop:       for (String param : params)
            {
                if (containsParameter(row, param))
                {
                    set.add(row.getString("id"));
                    break loop;
                }
            }
        }
        return set;
    }

    private boolean containsParameter(UntypedResultSet.Row row, String param)
    {
        assert (row.getColumns().size() <= max+1);
        for (int i = 0; i < max; i++)
        {
            if (row.has("value" + i))
            {
                if (collectionValues.keySet().contains(param))
                {
                    if (collectionValues.get(param).contains(row.getString("value" + i)))
                        return true;
                }
                else
                {
                    if (row.getString("value" + i).equals(param))
                        return true;
                }
            }
            else
            {
                // The first non-existing column indicates that no more columns with data exists, since we insert all data-containing columns with ascending indices
                break;
            }
        }
        return false;
    }

    public void validateAnnotation(Set<String> params) throws RequestValidationException
    {
        for (String param : params)
            if (param.trim().length() == 0)
              throw new InvalidRequestException("String Data Annotation '" + this.name + "' contains undefined values!");
    }

    @Override
    public void minimize(Set<String> set)
    {
        for (String name : collectionValues.keySet())
        {
            if (set.contains(name))
            {
                for (String val : collectionValues.get(name))
                {
                    if (set.contains(val))
                    {
                        set.remove(name);
                        break;
                    }
                }
            }
        }
    }

}
