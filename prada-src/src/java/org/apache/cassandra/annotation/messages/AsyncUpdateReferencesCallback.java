package org.apache.cassandra.annotation.messages;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.utils.ByteBufferUtil;

public class AsyncUpdateReferencesCallback implements IAsyncCallback<IndirectionMessage>
{
    private boolean doneReferences;
    private boolean doneDelete;
    private boolean done;
    private double size;
    private final long start = System.nanoTime();
    private String error = null;

    public double getSize()
    {
        return size;
    }

    public String getError()
    {
        return error;
    }

    @Override
    public synchronized void response(MessageIn<IndirectionMessage> response)
    {
        if (!done)
        {
            switch (response.payload.getType())
            {
            case UPDATE_REFERENCES:
                String msg = ByteBufferUtil.stringWOException(response.payload.content);
                String[] parts = msg.split(";");
                assert(parts.length >= 2);
                size = Double.parseDouble(parts[0]);
                if (!doneDelete && Integer.parseInt(parts[1]) < 1)
                    doneDelete = true;
                if (parts.length > 2)
                    error = parts[2];
                doneReferences = true;
                break;
            case DELETE:
                doneDelete = true;
                break;
            default:
                break;
            }
            if(!done && doneReferences && doneDelete)
            {
                done = true;
                this.notifyAll();
            }
        }
    }

    public void waitForResponses(long timeout, TimeUnit tu) throws TimeoutException
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
    }

    @Override
    public boolean isLatencyForSnitch()
    {
        return false;
    }

    @Override
    public synchronized boolean done()
    {
        return done;
    }
}
