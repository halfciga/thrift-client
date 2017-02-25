package com.tk.thrift.client.failover;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.tk.thrift.client.pool.ThriftServerInfo;

/**
 * @author Tingkun Zhang
 */
public class Failover {

	private volatile List<ThriftServerInfo> serverInfoList;

	private List<ThriftServerInfo> backupServerInfoList;

	private FailoverCheckingStrategy<ThriftServerInfo> failoverCheckingStrategy;

	private ConnectionValidator connectionValidator;

	public Failover(ConnectionValidator connectionValidator) {
		this(connectionValidator, new FailoverCheckingStrategy<ThriftServerInfo>());
	}

	public Failover(ConnectionValidator connectionValidator, FailoverCheckingStrategy<ThriftServerInfo> failoverCheckingStrategy) {
		this.connectionValidator = connectionValidator;
		this.failoverCheckingStrategy = failoverCheckingStrategy;
	}

	public List<ThriftServerInfo> getAvailableServers() {
		List<ThriftServerInfo> returnList = new ArrayList<>();
		Set<ThriftServerInfo> failedServers = failoverCheckingStrategy.getFailed();
		for (ThriftServerInfo thriftServerInfo : serverInfoList) {
			if (!failedServers.contains(thriftServerInfo))
				returnList.add(thriftServerInfo);
		}
		if (returnList.isEmpty() && !backupServerInfoList.isEmpty()) {
			for (ThriftServerInfo thriftServerInfo : backupServerInfoList) {
				if (!failedServers.contains(thriftServerInfo))
					returnList.add(thriftServerInfo);
			}
		}
		// 如果所有的服务都命中隔离策略，那就降级使用serverInfoList,防止所有的服务都不稳定的情况，无服务可用
		if (returnList.isEmpty()) {
			returnList.addAll(serverInfoList);
		}
		return returnList;
	}

	public void setServerInfoList(List<ThriftServerInfo> serverInfoList) {
		this.serverInfoList = serverInfoList;
	}

	public void setBackupServerInfoList(List<ThriftServerInfo> backupServerInfoList) {
		this.backupServerInfoList = backupServerInfoList;
	}

	public FailoverCheckingStrategy<ThriftServerInfo> getFailoverCheckingStrategy() {
		return failoverCheckingStrategy;
	}

	public ConnectionValidator getConnectionValidator() {
		return connectionValidator;
	}
}
