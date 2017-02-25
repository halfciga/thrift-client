package com.tk.thrift.client.pool;

import java.util.List;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;

/**
 * @author Tingkun Zhang
 */
public final class ThriftServerInfo {

	private static ConcurrentMap<String, ThriftServerInfo> allInfos = new MapMaker().weakValues().makeMap();

	private static Splitter splitter = Splitter.on(':');

	private final String host;

	private final int port;

	private final int chance;

	private ThriftServerInfo(String serverconfig) {
		List<String> split = splitter.splitToList(serverconfig);
		this.host = split.get(0);
		this.port = Integer.parseInt(split.get(1));
		if (split.size() > 2)
			this.chance = Integer.parseInt(split.get(2));
		else
			this.chance = 1;
	}

	public static final List<ThriftServerInfo> ofs(String[] serverconfigs) {
		List<ThriftServerInfo> l = Lists.newArrayListWithCapacity(serverconfigs.length);
		for (String string : serverconfigs) {
			l.add(of(string));
		}
		return l;
	}

	public static final List<ThriftServerInfo> ofs(String serverconfigs) {
		return ofs(StringUtils.split(serverconfigs, ','));
	}

	public static final ThriftServerInfo of(String serverconfig) {
		if (allInfos.containsKey(serverconfig))
			return allInfos.get(serverconfig);
		ThriftServerInfo tsi = new ThriftServerInfo(serverconfig);
		ThriftServerInfo rt = allInfos.putIfAbsent(serverconfig, tsi);
		return rt == null ? tsi : rt;
	}

	public String getHost() {
		return host;
	}

	public int getPort() {
		return port;
	}

	public int getChance() {
		return chance;
	}

	@Override
	public String toString() {
		return "ThriftServerInfo [host=" + host + ", port=" + port + ",chance=" + chance + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((host == null) ? 0 : host.hashCode());
		result = prime * result + port;
		result = prime * result + chance;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ThriftServerInfo other = (ThriftServerInfo) obj;
		if (chance != other.chance)
			return false;
		if (host == null) {
			if (other.host != null)
				return false;
		} else if (!host.equals(other.host))
			return false;
		if (port != other.port)
			return false;
		return true;
	}

}
