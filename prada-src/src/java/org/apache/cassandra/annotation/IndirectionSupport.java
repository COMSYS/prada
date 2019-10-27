package org.apache.cassandra.annotation;

import java.io.*;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.*;

import org.antlr.runtime.*;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.db.*;
import org.apache.log4j.helpers.Loader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;


public class IndirectionSupport {

    private static final boolean debugMode = false;

    // Keyspace Postfix for Keyspace holding the actual redirected data items
    public static final String DataKeyspacePostfix = "_redirecteddata";
    // Keyspace Postfix for Keyspace holding the reference information
    public static final String ReferenceKeyspacePostfix = "_references";
    // Keyspace name for Keyspace holding the Data Annotation abilities of the nodes
    public static final String DataAnnotationAbilities = "dataannotation";

    public static DataAnnotationConfig DataAnnotationConfiguration;


    public static void Bootstrap()
    {
        DataAnnotationConfiguration = loadDataAnnotationConfig();
        IndirectionSupport.sendToLog("Data Annotation Settings loaded: " + DataAnnotationConfiguration.toString(), false, true, "IndirectionSupport");

        if (Schema.instance.getKeyspaceInstance(DataAnnotationAbilities) == null)
        {
            IndirectionSupport.sendToLog("Keyspace for Data Annotations does not exist, create it!", false, true, "IndirectionSupport");
            try
            {
                QueryProcessor.process("CREATE KEYSPACE " + DataAnnotationAbilities + " WITH REPLICATION = { 'class' : '" + DataAnnotationConfiguration.repStrategy + "', 'replication_factor' : '" + DataAnnotationConfiguration.repFactor + "' };", ConsistencyLevel.ONE);
            }
            catch (RequestExecutionException e)
            {
                IndirectionSupport.sendToLog("Error creating '" + IndirectionSupport.DataAnnotationAbilities + "' keyspace!", true, false, "IndirectionSupport");
                e.printStackTrace();
                System.exit(1);
            }
        }

        DataAnnotationConfiguration.announceCapabilities();
    }

    private static DataAnnotationConfig loadDataAnnotationConfig()
    {
        URL configUrl = Loader.getResource("annotations.conf");
        if (configUrl != null)
        {
            try
            {
                CharStream stream = new ANTLRFileStream(configUrl.getFile().toString());
                configLexer lexer = new configLexer(stream);
                TokenStream tokenStream = new CommonTokenStream(lexer);
                configParser parser = new configParser(tokenStream);

                DataAnnotationConfig config = parser.result();

                lexer.throwLastRecognitionError();
                parser.throwLastRecognitionError();

                return config;
            }
            catch (IOException e)
            {
                IndirectionSupport.sendToLog("Error reading Data Annotation config!", true, false, "IndirectionSupport");
                e.printStackTrace();
                System.exit(1);
            }
            catch (SyntaxException|RecognitionException e)
            {
                IndirectionSupport.sendToLog("Configuration file annotation.conf malformed!", true, false, "IndirectionSupport");
                e.printStackTrace();
                System.exit(1);
            }
        }
        else
        {
            IndirectionSupport.sendToLog("Data Annotation Configuration file not found!", true, false, "IndirectionSupport");
            System.exit(1);
        }
        return new DataAnnotationConfig();
    }

    public static boolean isRegularKeyspace(String KeySpaceName)
    {
        return ( !(KeySpaceName.endsWith(IndirectionSupport.ReferenceKeyspacePostfix) || KeySpaceName.endsWith(IndirectionSupport.DataKeyspacePostfix) || KeySpaceName.equals(Keyspace.SYSTEM_KS) || KeySpaceName.equals("system_traces") || KeySpaceName.equals(IndirectionSupport.DataAnnotationAbilities)));
    }

    public static void queryDB(String queryStr, ConsistencyLevel cl)
    {
        try
        {
            QueryProcessor.process(queryStr, cl);
        }
        catch (RequestExecutionException e)
        {
            IndirectionSupport.sendToLog("RequestExcecutionException occured! Error message: " + e.getMessage(), false, true, "IndirectionSupport");
            e.printStackTrace();
        }
    }

    public static void Teardown()
    {
        // Do not drop abilities if object is null, can appear if no data annotation configuration file exists
        if (IndirectionSupport.DataAnnotationConfiguration != null)
            IndirectionSupport.DataAnnotationConfiguration.dropAbilities();

        Evaluation.printTimestamps();
    }

    public static void sendToLog(String msg, boolean error, boolean onlyOnDebugMode, String context)
    {
        if (Evaluation.evalMode && !error)
            return;
        if (onlyOnDebugMode && debugMode == false)
            return;
        Logger loggerInstance = LoggerFactory.getLogger(context);
        if (error)
            loggerInstance.error("AnnotationLog (" + context + "): " + msg);
        else
            loggerInstance.info("AnnotationLog (" + context + "): " + msg);
    }

}
