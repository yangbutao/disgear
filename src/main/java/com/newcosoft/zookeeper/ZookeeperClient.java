package com.newcosoft.zookeeper;

import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

public class ZookeeperClient {
	private ZooKeeper zkClient;

	public ZookeeperClient(ZooKeeper keeper) {
		this.zkClient = keeper;
	}

	public void ensureExists(final String path, final byte[] data,
			CreateMode createMode) throws KeeperException, InterruptedException {
		if (zkClient.exists(path, null) != null) {
			return;
		}
		try {
			makePath(path, data, CreateMode.PERSISTENT);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public void makePath(String path, byte[] data, CreateMode createMode)
			throws Exception {
		boolean failOnExists = true;
		boolean retry = true;

		if (path.startsWith("/")) {
			path = path.substring(1, path.length());
		}
		String[] paths = path.split("/");
		StringBuilder sbPath = new StringBuilder();
		for (int i = 0; i < paths.length; i++) {
			byte[] bytes = null;
			String pathPiece = paths[i];
			sbPath.append("/" + pathPiece);
			final String currentPath = sbPath.toString();
			Object exists = zkClient.exists(currentPath, null);
			if (exists == null || ((i == paths.length - 1) && failOnExists)) {
				CreateMode mode = CreateMode.PERSISTENT;
				if (i == paths.length - 1) {
					mode = createMode;
					bytes = data;
				}
				try {
					zkClient.create(currentPath, bytes,
							ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
				} catch (NodeExistsException e) {

					if (!failOnExists) {
						// TODO:
						zkClient.setData(currentPath, data, -1);

						zkClient.exists(currentPath, null);
						return;
					}

					if (i == paths.length - 1) {
						throw e;
					}
				}
				if (i == paths.length - 1) {

					zkClient.exists(currentPath, null);
				}
			} else if (i == paths.length - 1) {

				zkClient.setData(currentPath, data, -1);

				zkClient.exists(currentPath, null);
			}
		}

	}

	public void clean(String path) throws InterruptedException, KeeperException {
		List<String> children;
		try {
			
			children = zkClient.getChildren(path, null);
		} catch (NoNodeException r) {
			return;
		}
		for (String string : children) {
			
			if (path.equals("/") && string.equals("zookeeper"))
				continue;
			if (path.equals("/")) {
				clean(path + string);
			} else {
				clean(path + "/" + string);
			}
		}
		try {
			if (!path.equals("/")) {
				zkClient.delete(path, -1);
			}
		} catch (NoNodeException r) {
			return;
		}
	}
}
