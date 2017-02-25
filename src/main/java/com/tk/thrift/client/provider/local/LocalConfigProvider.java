package com.tk.thrift.client.provider.local;

import com.tk.thrift.client.pool.ThriftServerInfo;
import com.tk.thrift.client.provider.Provider;
import com.tk.thrift.client.provider.ProviderListener;

import java.util.Collection;
import java.util.List;

/**
 * @author Tingkun Zhang
 */
public class LocalConfigProvider implements Provider<ThriftServerInfo> {

	private List<ThriftServerInfo> serverInfoList;

	public LocalConfigProvider(String connStr) {
		serverInfoList = ThriftServerInfo.ofs(connStr);
	}

	@Override
	public Collection<ThriftServerInfo> list() {
		return serverInfoList;
	}

	@Override
	public void addListener(ProviderListener<ThriftServerInfo> listener) {

	}
}
