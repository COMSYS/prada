package org.apache.cassandra.exceptions;

public class IndirectionException extends RequestExecutionException
{
    public IndirectionException(String msg)
    {
        super(ExceptionCode.INDIRECTION_ERROR, msg);
    }
}
