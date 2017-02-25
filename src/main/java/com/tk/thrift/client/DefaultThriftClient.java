package com.tk.thrift.client;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javassist.util.proxy.MethodFilter;
import javassist.util.proxy.MethodHandler;
import javassist.util.proxy.Proxy;
import javassist.util.proxy.ProxyFactory;

import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.tk.thrift.client.failover.ConnectionValidator;
import com.tk.thrift.client.failover.Failover;
import com.tk.thrift.client.failover.FailoverCheckingStrategy;
import com.tk.thrift.client.pool.DefaultThriftConnectionPool;
import com.tk.thrift.client.pool.ThriftConnectionFactory;
import com.tk.thrift.client.pool.ThriftServerInfo;
import com.tk.thrift.client.provider.Provider;
import com.tk.thrift.client.provider.ProviderListener;
import com.tk.thrift.client.utils.ThriftClientUtils;

/**
 * @author Tingkun Zhang
 */
public class DefaultThriftClient implements ThriftClient, ProviderListener<ThriftServerInfo> {

	private final static Logger logger = LoggerFactory.getLogger(DefaultThriftClient.class);

	private AtomicInteger i = new AtomicInteger(0);

	private Failover failover;

	private DefaultThriftConnectionPool pool;

	private Provider<ThriftServerInfo> provider;

	private final static int DEFAULT_CONN_TIMEOUT = (int) TimeUnit.SECONDS.toMillis(5);

	public DefaultThriftClient(Provider<ThriftServerInfo> provider) {
		this(provider, null, new GenericKeyedObjectPoolConfig(), null);
	}

	public DefaultThriftClient(Provider<ThriftServerInfo> provider, ConnectionValidator validator) {
		this(provider, validator, new GenericKeyedObjectPoolConfig(), null);
	}

	public DefaultThriftClient(Provider<ThriftServerInfo> provider, ConnectionValidator validator, GenericKeyedObjectPoolConfig poolConfig) {
		this(provider, validator, poolConfig, null);
	}

	public DefaultThriftClient(Provider<ThriftServerInfo> provider, ConnectionValidator validator, GenericKeyedObjectPoolConfig poolConfig,
			String backupServers) {
		this(provider, validator, new FailoverCheckingStrategy<ThriftServerInfo>(), poolConfig, DEFAULT_CONN_TIMEOUT, backupServers);
	}

	public DefaultThriftClient(Provider<ThriftServerInfo> provider, ConnectionValidator validator,
			FailoverCheckingStrategy<ThriftServerInfo> strategy, GenericKeyedObjectPoolConfig poolConfig, int connTimeout,
			String backupServers) {
		this.provider = provider;
		provider.addListener(this);
		this.failover = new Failover(validator, strategy);
		this.pool = new DefaultThriftConnectionPool(new ThriftConnectionFactory(failover, connTimeout), poolConfig);
		Collection<ThriftServerInfo> serverInfos = provider.list();
		failover.setServerInfoList(serverInfos.isEmpty() ? new ArrayList<ThriftServerInfo>() : Lists.newArrayList(serverInfos));
		List<ThriftServerInfo> serverInfoList = Strings.isNullOrEmpty(backupServers) ? new ArrayList<ThriftServerInfo>() : ThriftServerInfo
				.ofs(backupServers);
		failover.setBackupServerInfoList(serverInfoList);
	}

	@Override
	public <X extends TServiceClient> X iface(Class<X> ifaceClass) {
		return iface(ifaceClass, i.getAndDecrement());
	}

	protected <X extends TServiceClient> X iface(Class<X> ifaceClass, int hash) {
		List<ThriftServerInfo> servers = getAvaliableServers();
		if (servers == null || servers.isEmpty()) {
			throw new NullPointerException("servers could not be null");
		}
		hash = Math.abs(hash);
		final ThriftServerInfo selected = servers.get(hash % servers.size());
		return iface(ifaceClass, selected);
	}

	@SuppressWarnings("unchecked")
	protected <X extends TServiceClient> X iface(final Class<X> ifaceClass, final ThriftServerInfo selected) {
		final TTransport transport;
		try {
			transport = pool.getConnection(selected);
		} catch (RuntimeException e) {
			if (e.getCause() != null && e.getCause() instanceof TTransportException)
				failover.getFailoverCheckingStrategy().fail(selected);
			throw e;
		}
		TProtocol protocol = new TBinaryProtocol(transport);

		ProxyFactory factory = new ProxyFactory();
		factory.setSuperclass(ifaceClass);
		factory.setFilter(new MethodFilter() {
			@Override
			public boolean isHandled(Method m) {
				return ThriftClientUtils.getInterfaceMethodNames(ifaceClass).contains(m.getName());
			}
		});
		try {
			X x = (X) factory.create(new Class[] { TProtocol.class }, new Object[] { protocol });
			((Proxy) x).setHandler(new MethodHandler() {
				@Override
				public Object invoke(Object self, Method thisMethod, Method proceed, Object[] args) throws Throwable {

					boolean success = false;
					try {
						Object result = proceed.invoke(self, args);
						success = true;
						return result;
					} finally {
						if (success) {
							pool.returnConnection(selected, transport);
						} else {
							failover.getFailoverCheckingStrategy().fail(selected);
							pool.returnBrokenConnection(selected, transport);
						}
					}
				}
			});
			return x;
		} catch (NoSuchMethodException | IllegalArgumentException | InstantiationException | IllegalAccessException
				| InvocationTargetException e) {
			throw new RuntimeException("fail to create proxy.", e);
		}
	}

	@Override
	public void close() {
		pool.close();
	}

	@Override
	public List<ThriftServerInfo> getAvaliableServers() {
		return failover.getAvailableServers();
	}

	@Override
	public void onFresh() {
		Collection<ThriftServerInfo> serverInfoList = provider.list();
		logger.info("ON_FRESH SERVER_INFO_LIST:{}", serverInfoList);
		if (serverInfoList.isEmpty()) {
			logger.warn("onFresh event: serverInfo is empty.");
		} else {
			failover.setServerInfoList(Lists.newArrayList(serverInfoList));
		}
	}

}
