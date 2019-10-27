package org.apache.cassandra.annotation;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;

public class Evaluation {

    private String outputPath;
    public static final boolean evalMode = true;
    public static final boolean details = false;
    private Map<Long, LinkedList<Timestamp>> timestamps;
    private static Evaluation instance = null;


    private Evaluation()
    {
        timestamps = Collections.synchronizedMap(new HashMap<Long, LinkedList<Timestamp>>());
        Map<String, String> envs = System.getenv();
        String outputPrefix;
        outputPrefix = ".";
        outputPath = outputPrefix + "/eval/eval-measurements-";
    }

    private static Evaluation getInstance()
    {
        if(instance == null)
        {
            instance = new Evaluation();
        }
        return instance;
    }

    public static void addSimpleTimestamp(String method, String subject, long time)
    {
        getInstance()._addSimpleTimestamp(method, subject, time);
    }

    public static void addStartTimestamp(String method, String subject)
    {
        getInstance()._addStartTimestamp(method, subject);
    }

    private void _addStartTimestamp(String method, String subject)
    {
        if (!evalMode || !details)
            return;

        Timestamp t = new Timestamp();

        LinkedList<Timestamp> list = timestamps.get(Thread.currentThread().getId());
        if (list == null)
        {
            timestamps.put(Thread.currentThread().getId(), list = new LinkedList<>());
        }

        list.add(t);
        t.start = true;
        t.method = method;
        t.subject = subject;
        t.time = System.nanoTime();
    }

    private void _addSimpleTimestamp(String method, String subject, long time)
    {
        if (!evalMode || details)
            return;

        Timestamp t = new Timestamp();

        LinkedList<Timestamp> list = timestamps.get(Thread.currentThread().getId());
        if (list == null)
        {
            timestamps.put(Thread.currentThread().getId(), list = new LinkedList<>());
        }

        list.add(t);
        t.start = false;
        t.method = method;
        t.subject = subject;
        t.time = time;
    }

    public static void addEndTimestamp()
    {
        getInstance()._addEndTimestamp();
    }

    private void _addEndTimestamp()
    {
        if (!evalMode || !details)
            return;

        long time = System.nanoTime();

        LinkedList<Timestamp> list = timestamps.get(Thread.currentThread().getId());
        assert(list != null);

        Timestamp t = new Timestamp();

        list.add(t);
        t.start = false;
        t.time = time;
    }

    public static void printTimestamps()
    {
        getInstance()._printTimestamps();
    }

    private void _printTimestamps()
    {
        if (!evalMode)
            return;

        int i = 1;
        String path = outputPath + i + ".txt";
        File f = new File(path);

        while (f.exists())
        {
            i++;
            path = outputPath + i + ".txt";
            f = new File(path);
        }

        Writer writer = null;
        try
        {
            writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(path), "utf-8"));

            for(Entry<Long, LinkedList<Timestamp>> e : timestamps.entrySet())
            {
                writer.write("----------------------------------\nTHREAD ID: " + e.getKey()
                        + "\n----------------------------------\n");
                Iterator<Timestamp> it = e.getValue().iterator();
                while(it.hasNext())
                    if(details)
                        printTimes(writer, it.next(), it, 0);
                    else
                        printTimesSimple(writer, it);
                writer.write("\n\n");
            }
        }
        catch (IOException ex)
        {
          ex.printStackTrace();
        }
        finally
        {
           try
           {
               writer.close();
           }
           catch
           (Exception e)
           {
               e.printStackTrace();
           }
        }
    }

    private void indent(StringBuilder sb, int c)
    {
        while((c--) > 0) sb.append(' ');
    }

    private void printTimesSimple(Writer writer, Iterator<Timestamp> it) throws IOException
    {
        while(it.hasNext())
        {
            Timestamp st = it.next();
            StringBuilder sb = new StringBuilder();
            sb = new StringBuilder();
            sb.append("END\n");
            sb.append(st.method); sb.append("\n");
            sb.append(st.subject); sb.append("\n");
            sb.append("TIME: "); sb.append(st.time/1000);
            sb.append("us\n\n");
            writer.write(sb.toString());
        }
    }

    private void printTimes(Writer writer, Timestamp st, Iterator<Timestamp> it, int d) throws IOException
    {
        StringBuilder sb = new StringBuilder();
        indent(sb, d*2); sb.append("START\n");
        indent(sb, d*2); sb.append(st.method); sb.append("\n");
        indent(sb, d*2); sb.append(st.subject); sb.append("\n\n");
        writer.write(sb.toString());

        assert(it.hasNext());
        Timestamp t = it.next();
        while (t.start)
        {
            printTimes(writer, t, it, d+1);
            t = it.next();
        }

        sb = new StringBuilder();
        indent(sb, d*2); sb.append("END\n");
        indent(sb, d*2); sb.append(st.method); sb.append("\n");
        indent(sb, d*2); sb.append(st.subject); sb.append("\n");
        indent(sb, d*2); sb.append("TIME: "); sb.append((t.time - st.time)/1000);
        sb.append("us\n\n");
        writer.write(sb.toString());
    }

    private static class Timestamp
    {
        public long time;
        public boolean start;
        public String method;
        public String subject;

        public String toString()
        {
            return (start ? "START " : "END ") + method + ": " + subject;
        }
    }
}
