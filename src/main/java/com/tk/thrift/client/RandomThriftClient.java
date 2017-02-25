package com.tk.thrift.client;

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
public class RandomThriftClient extends DefaultThriftClient {

    public RandomThriftClient(Provider<ThriftServerInfo> provider) {
        this(provider, null, new GenericKeyedObjectPoolConfig(), null);
    }

    public RandomThriftClient(Provider<ThriftServerInfo> provider, ConnectionValidator validator) {
        this(provider, validator, new GenericKeyedObjectPoolConfig(), null);
    }

    public RandomThriftClient(Provider<ThriftServerInfo> provider, ConnectionValidator validator, GenericKeyedObjectPoolConfig poolConfig) {
        this(provider, validator, poolConfig, null);
    }

    public RandomThriftClient(Provider<ThriftServerInfo> provider, ConnectionValidator validator, GenericKeyedObjectPoolConfig poolConfig, String backupServers) {
        super(provider, validator, poolConfig, backupServers);
    }

    public RandomThriftClient(Provider<ThriftServerInfo> provider, ConnectionValidator validator, FailoverCheckingStrategy<ThriftServerInfo> strategy,
                            GenericKeyedObjectPoolConfig poolConfig, int connTimeout, String backupServers) {
        super(provider, validator, strategy, poolConfig, connTimeout, backupServers);
    }


    @Override
    public <X extends TServiceClient> X iface(Class<X> ifaceClass) {
        return iface(ifaceClass, ThriftClientUtils.randomNextInt());
    }

}
