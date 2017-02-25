package com.tk.thrift.client.failover;

import org.apache.thrift.transport.TTransport;

/**
 * @author Tingkun Zhang
 */
public interface ConnectionValidator {

	boolean isValid(TTransport object);

}
