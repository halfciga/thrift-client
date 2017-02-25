package com.tk.thrift.client.pool;

import org.apache.thrift.transport.TTransport;

/**
 * @author Tingkun Zhang
 */
public interface ThriftConnectionPool {

	TTransport getConnection(ThriftServerInfo thriftServerInfo);

	void returnConnection(ThriftServerInfo thriftServerInfo, TTransport transport);

	void returnBrokenConnection(ThriftServerInfo thriftServerInfo, TTransport transport);

	void close();

}
