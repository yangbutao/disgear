package com.newcosoft.zookeeper;

public class ZkCoreNodeProps {

	private ZkNodeProps nodeProps;

	public ZkCoreNodeProps(ZkNodeProps nodeProps) {
		this.nodeProps = nodeProps;
	}

	public String getCoreUrl() {
		return getCoreUrl(nodeProps.getStr(ZkStateReader.BASE_URL_PROP),
				nodeProps.getStr(ZkStateReader.CORE_NAME_PROP));
	}

	public String getNodeName() {
		return nodeProps.getStr(ZkStateReader.NODE_NAME_PROP);
	}

	public String getState() {
		return nodeProps.getStr(ZkStateReader.STATE_PROP);
	}

	public String getBaseUrl() {
		return nodeProps.getStr(ZkStateReader.BASE_URL_PROP);
	}

	public String getCoreName() {
		return nodeProps.getStr(ZkStateReader.CORE_NAME_PROP);
	}

	public static String getCoreUrl(ZkNodeProps nodeProps) {
		return getCoreUrl(nodeProps.getStr(ZkStateReader.BASE_URL_PROP),
				nodeProps.getStr(ZkStateReader.CORE_NAME_PROP));
	}

	public static String getCoreUrl(String baseUrl, String coreName) {
		StringBuilder sb = new StringBuilder();
		sb.append(baseUrl);
		if (!baseUrl.endsWith("/"))
			sb.append("/");
		sb.append(coreName);
		if (!(sb.substring(sb.length() - 1).equals("/")))
			sb.append("/");
		return sb.toString();
	}

	@Override
	public String toString() {
		return nodeProps.toString();
	}

	public ZkNodeProps getNodeProps() {
		return nodeProps;
	}

	public boolean isLeader() {
		return nodeProps.containsKey(ZkStateReader.LEADER_PROP);
	}

}
