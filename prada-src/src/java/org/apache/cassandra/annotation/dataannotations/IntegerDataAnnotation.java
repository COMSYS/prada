package org.apache.cassandra.annotation.dataannotations;

import java.util.*;

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.annotation.*;

public class IntegerDataAnnotation extends AbstractDataAnnotation<Integer> {

    public enum IntegerComparators {LESS, EQUAL, GREATER, LESSEQUAL, GREATEREQUAL}

    public final IntegerComparators Comparator;


    public IntegerDataAnnotation(String name, Collection<Integer> supportedValues, IntegerComparators comp, int max)
    {
        super(name, supportedValues, max);
        this.Comparator = comp;
    }

    public String createINSERTstatement()
    {
        assert (supportedValues.size() > 0 && supportedValues.size() <= max);
        String str = "INSERT INTO " + IndirectionSupport.DataAnnotationAbilities + "." + this.name + " ( id";
        String str2 = "";
        Iterator<Integer> it = supportedValues.iterator();
        for (int i=0; i < supportedValues.size(); i++)
        {
            str += ", " + "value" + i;
            str2 += ", " + it.next();
        }
        str += " ) VALUES ( '" + getOwnToken() + "' " + str2;
        return str + " );";
    }

    public String createCreateTablestatement()
    {
        assert (max != -1 && max > 0);
        String str = "CREATE TABLE " + IndirectionSupport.DataAnnotationAbilities + "." + this.name + " ( id varchar PRIMARY KEY";
        for (int i = 0; i < max; i++)
        {
            str += ", value" + i + " int";
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
            int count = 0;

            for (String param : params)
                if (containsParameter(row, param))
                    count++;

            if (params.size() == count)
                set.add(row.getString("id"));
        }
        return set;
    }

    public void validateAnnotation(Set<String> params) throws RequestValidationException
    {
        for (String param : params)
        {
            try
            {
                Integer.parseInt(param);
            }
            catch(NumberFormatException e)
            {
                throw new InvalidRequestException("Integer Data Annotation '" + this.name + "' contains nonnumerical values!");
            }
        }
    }

    private boolean containsParameter(UntypedResultSet.Row row, String param)
    {
        for (int i = 0; i < max; i++)
        {
            if (row.has("value" + i))
            {
                if (compare(Integer.parseInt(param), row.getInt("value" + i)))
                    return true;
            }
            else
            {
                // The first non-existing column indicates that no more columns with data exists, since we insert all data-containing columns with ascending indices
                break;
            }
        }
        return false;
    }

    private boolean compare (int a, int b)
    {
        switch(Comparator)
        {
            case LESS: return (a < b);
            case EQUAL: return (a == b);
            case GREATER: return (a > b);
            case LESSEQUAL: return (a <= b);
            case GREATEREQUAL: return (a >= b);
        }
        return false;
    }

    @Override
    public void minimize(Set<String> set)
    {
        // noop
    }

}
