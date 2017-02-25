package com.tk.thrift.client;

import java.util.List;

import org.apache.thrift.TServiceClient;

import com.tk.thrift.client.pool.ThriftServerInfo;


/**
 * @author Tingkun Zhang
 */

public interface ThriftClient {

	public <X extends TServiceClient> X iface(Class<X> ifaceClass);

	public List<ThriftServerInfo> getAvaliableServers();

	public void close();
}
