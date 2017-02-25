package com.tk.thrift.client;

import java.util.List;

import com.tk.thrift.client.failover.ConnectionValidator;
import com.tk.thrift.client.failover.FailoverCheckingStrategy;
import com.tk.thrift.client.pool.ThriftServerInfo;
import com.tk.thrift.client.provider.Provider;
import com.tk.thrift.client.utils.ThriftClientUtils;

import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.apache.thrift.TServiceClient;

/**
 * @author Tingkun Zhang
 */
public class WeightThriftClient extends DefaultThriftClient {

	public WeightThriftClient(Provider<ThriftServerInfo> provider) {
		this(provider, null, new GenericKeyedObjectPoolConfig(), null);
	}

	public WeightThriftClient(Provider<ThriftServerInfo> provider, ConnectionValidator validator) {
		this(provider, validator,new GenericKeyedObjectPoolConfig(),null);
	}

	public WeightThriftClient(Provider<ThriftServerInfo> provider, ConnectionValidator validator, GenericKeyedObjectPoolConfig poolConfig){
		this(provider, validator,poolConfig,null);
	}

	public WeightThriftClient(Provider<ThriftServerInfo> provider, ConnectionValidator validator, GenericKeyedObjectPoolConfig poolConfig, String backupServers){
		super(provider, validator,poolConfig,backupServers);
	}

	public WeightThriftClient(Provider<ThriftServerInfo> provider, ConnectionValidator validator, FailoverCheckingStrategy<ThriftServerInfo> strategy,
							GenericKeyedObjectPoolConfig poolConfig, int connTimeout, String backupServers) {
		super(provider, validator, strategy, poolConfig, connTimeout, backupServers);
	}


	@Override
	public <X extends TServiceClient> X iface(Class<X> ifaceClass) {
		List<ThriftServerInfo> servers = getAvaliableServers();
		if (servers == null || servers.isEmpty()) {
			throw new NullPointerException("servers could not be null");
		}
		int[] chances = new int[servers.size()];
		for (int i = 0; i < servers.size(); i++) {
			chances[i] = servers.get(i).getChance();
		}
		return iface(ifaceClass, servers.get(ThriftClientUtils.chooseWithChance(chances)));
	}

}
