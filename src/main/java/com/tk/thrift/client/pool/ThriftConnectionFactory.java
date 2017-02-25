package com.tk.thrift.client.pool;

import com.tk.thrift.client.failover.ConnectionValidator;
import com.tk.thrift.client.failover.Failover;

import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * @author Tingkun Zhang
 */
public class ThriftConnectionFactory implements KeyedPooledObjectFactory<ThriftServerInfo, TTransport> {

	private static final Logger logger = LoggerFactory.getLogger(DefaultThriftConnectionPool.class);

	private int timeout;//in millis

	private Failover failover;

	public ThriftConnectionFactory(Failover failover, int timeout) {
		this.failover = failover;
		this.timeout = timeout;
	}

	public ThriftConnectionFactory(Failover failover) {
		this(failover, (int) TimeUnit.SECONDS.toMillis(5));
	}

	public ThriftConnectionFactory(int timeout) {
		this(null, timeout);
	}

	public ThriftConnectionFactory() {
		this((int) TimeUnit.SECONDS.toMillis(5));
	}

	@Override
	public PooledObject<TTransport> makeObject(ThriftServerInfo info) throws Exception {
		TSocket tsocket = new TSocket(info.getHost(), info.getPort());
		tsocket.setTimeout(timeout);
		TFramedTransport transport = new TFramedTransport(tsocket);

		transport.open();
		DefaultPooledObject<TTransport> result = new DefaultPooledObject<TTransport>(transport);
		logger.debug("make new thrift connection:{}", info);
		return result;
	}

	@Override
	public void destroyObject(ThriftServerInfo info, PooledObject<TTransport> p) throws Exception {
		TTransport transport = p.getObject();
		if (transport != null) {
			transport.close();
			logger.debug("close thrift connection:{}", info);
		}
	}

	@Override
	public boolean validateObject(ThriftServerInfo info, PooledObject<TTransport> p) {
		boolean isValidate;
		try {
			if (failover == null) {
				isValidate = p.getObject().isOpen();
			} else {
				ConnectionValidator validator = failover.getConnectionValidator();
				isValidate = p.getObject().isOpen() && (validator == null || validator.isValid(p.getObject()));
			}
		} catch (Throwable e) {
			logger.warn("fail to validate tsocket:{}", info, e);
			isValidate = false;
		}
		//not sure whether it should fail
		if (failover != null && !isValidate) {
			failover.getFailoverCheckingStrategy().fail(info);
		}
		logger.info("validateObject isValidate:{}", isValidate);
		return isValidate;
	}

	@Override
	public void activateObject(ThriftServerInfo info, PooledObject<TTransport> p) throws Exception {
		// do nothing
	}

	@Override
	public void passivateObject(ThriftServerInfo info, PooledObject<TTransport> p) throws Exception {
		// do nothing
	}

}
