package com.tk.thrift.client;

import com.google.common.base.Charsets;
import com.tk.thrift.client.failover.ConnectionValidator;
import com.tk.thrift.client.failover.FailoverCheckingStrategy;
import com.tk.thrift.client.pool.ThriftServerInfo;
import com.tk.thrift.client.provider.Provider;
import com.tk.thrift.client.utils.MurmurHash3;

import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.apache.thrift.TServiceClient;

/**
 * @author Tingkun Zhang
 */
public class HashThriftClient extends DefaultThriftClient {

	public HashThriftClient(Provider<ThriftServerInfo> registry) {
		this(registry, null, new GenericKeyedObjectPoolConfig(), null);
	}

	public HashThriftClient(Provider<ThriftServerInfo> provider, ConnectionValidator validator) {
		this(provider, validator,new GenericKeyedObjectPoolConfig(),null);
	}

	public HashThriftClient(Provider<ThriftServerInfo> provider, ConnectionValidator validator, GenericKeyedObjectPoolConfig poolConfig){
		this(provider, validator,poolConfig,null);
	}
	
	public HashThriftClient(Provider<ThriftServerInfo> provider, ConnectionValidator validator, GenericKeyedObjectPoolConfig poolConfig, String backupServers){
		super(provider, validator,poolConfig,backupServers);
	}

	public HashThriftClient(Provider<ThriftServerInfo> provider, ConnectionValidator validator, FailoverCheckingStrategy<ThriftServerInfo> strategy,
							GenericKeyedObjectPoolConfig poolConfig, int connTimeout, String backupServers) {
		super(provider, validator, strategy, poolConfig, connTimeout, backupServers);
	}

	public <X extends TServiceClient> X iface(Class<X> ifaceClass, String key) {
		byte[] b = key.getBytes(Charsets.UTF_8);
		return iface(ifaceClass, MurmurHash3.murmurhash3_x86_32(b, 0, b.length, 0x1234ABCD));
	}

}
