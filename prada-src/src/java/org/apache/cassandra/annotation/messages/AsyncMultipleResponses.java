package org.apache.cassandra.annotation.messages;

import java.net.InetAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.MessageIn;

import com.google.common.base.Predicate;

public class AsyncMultipleResponses<T> implements IAsyncCallback<T>
{
    private List<ResultEntry<T>> results;
    private Predicate<T> pred;
    private boolean done;
    private final long start = System.nanoTime();
    private Keyspace keyspace;
    private ConsistencyLevel cl;
    private int consistency;
    private int nodes;
    private int responses;

    public AsyncMultipleResponses(Predicate<T> pred, ConsistencyLevel cl, Keyspace keyspace, int nodes)
    {
        this.results = new LinkedList<>();
        this.pred = pred;
        this.keyspace = keyspace;
        this.cl = cl;
        this.nodes = nodes;
    }

    public boolean waitForResponses(long timeout, TimeUnit tu) throws TimeoutException
    {
        timeout = tu.toNanos(timeout);
        boolean interrupted = false;
        try
        {
            synchronized (this)
            {
                while (!done)
                {
                    try
                    {
                        long overallTimeout = timeout - (System.nanoTime() - start);
                        if (overallTimeout <= 0)
                        {
                            throw new TimeoutException("Operation timed out.");
                        }
                        TimeUnit.NANOSECONDS.timedWait(this, overallTimeout);
                    }
                    catch (InterruptedException e)
                    {
                        interrupted = true;
                    }
                }
            }
        }
        finally
        {
            if (interrupted)
            {
                Thread.currentThread().interrupt();
            }
        }
        return consistency >= cl.blockFor(keyspace);
    }

    public List<ResultEntry<T>> get(long timeout, TimeUnit tu) throws TimeoutException
    {
        waitForResponses(timeout, tu);
        return results;
    }

    public synchronized void response(MessageIn<T> response)
    {
        if (!done)
        {
            results.add(new ResultEntry<T>(response.from, response.payload));
            responses++;
            if (pred == null || pred.apply(response.payload))
            {
                consistency++;
                if (consistency >= cl.blockFor(keyspace))
                {
                    done = true;
                    this.notifyAll();
                }
            }
            if(!done && responses >= nodes)
            {
                done = true;
                this.notifyAll();
            }
        }
    }

    public boolean isLatencyForSnitch()
    {
        return false;
    }

    public static class ResultEntry<T>
    {
        private InetAddress addr;
        private T result;

        public ResultEntry(InetAddress addr, T result)
        {
            this.addr = addr;
            this.result = result;
        }

        public InetAddress getNodeAddress()
        {
            return addr;
        }

        public T getResult()
        {
            return result;
        }
    }

    @Override
    public boolean done()
    {
        return true;
    }
}
