package com.newcosoft.zookeeper;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.TreeMap;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributedQueue {
	private static final Logger LOG = LoggerFactory
			.getLogger(DistributedQueue.class);

	private static long DEFAULT_TIMEOUT = 5 * 60 * 1000;

	private final String dir;

	private ZooKeeper zookeeper;
	private List<ACL> acl = ZooDefs.Ids.OPEN_ACL_UNSAFE;

	private final String prefix = "qn-";

	private final String response_prefix = "qnr-";

	public DistributedQueue(ZooKeeper zookeeper, String dir, List<ACL> acl) {
		this.dir = dir;
		try {
			new ZookeeperClient(zookeeper).ensureExists(dir, null,
					CreateMode.PERSISTENT);
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.zookeeper = zookeeper;

	}

	
	private TreeMap<Long, String> orderedChildren(Watcher watcher)
			throws KeeperException, InterruptedException {
		TreeMap<Long, String> orderedChildren = new TreeMap<Long, String>();

		List<String> childNames = null;
		try {
			childNames = zookeeper.getChildren(dir, watcher);
		} catch (KeeperException.NoNodeException e) {
			throw e;
		}

		for (String childName : childNames) {
			try {
				// Check format
				if (!childName.regionMatches(0, prefix, 0, prefix.length())) {
					LOG.debug("Found child node with improper name: "
							+ childName);
					continue;
				}
				String suffix = childName.substring(prefix.length());
				Long childId = new Long(suffix);
				orderedChildren.put(childId, childName);
			} catch (NumberFormatException e) {
				LOG.warn("Found child node with improper format : " + childName
						+ " " + e, e);
			}
		}

		return orderedChildren;
	}

	
	private QueueEvent element() throws NoSuchElementException,
			KeeperException, InterruptedException {
		TreeMap<Long, String> orderedChildren;

		while (true) {
			try {
				orderedChildren = orderedChildren(null);
			} catch (KeeperException.NoNodeException e) {
				throw new NoSuchElementException();
			}
			if (orderedChildren.size() == 0)
				throw new NoSuchElementException();

			for (String headNode : orderedChildren.values()) {
				if (headNode != null) {
					try {
						return new QueueEvent(dir + "/" + headNode,
								zookeeper.getData(dir + "/" + headNode, null,
										null), null);
					} catch (KeeperException.NoNodeException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}

	
	public byte[] remove() throws NoSuchElementException, KeeperException,
			InterruptedException {
		TreeMap<Long, String> orderedChildren;
		
		while (true) {
			try {
				orderedChildren = orderedChildren(null);
			} catch (KeeperException.NoNodeException e) {
				throw new NoSuchElementException();
			}
			if (orderedChildren.size() == 0)
				throw new NoSuchElementException();

			for (String headNode : orderedChildren.values()) {
				String path = dir + "/" + headNode;
				try {
					byte[] data = zookeeper.getData(path, null, null);
					zookeeper.delete(path, -1);
					return data;
				} catch (KeeperException.NoNodeException e) {
					e.printStackTrace();
				}
			}

		}
	}

	
	public byte[] remove(QueueEvent event) throws KeeperException,
			InterruptedException {
		String path = event.getId();
		String responsePath = dir + "/" + response_prefix
				+ path.substring(path.lastIndexOf("-") + 1);
		if (zookeeper.exists(responsePath, false) != null) {
			zookeeper.setData(responsePath, event.getBytes(), -1);
		}
		byte[] data = zookeeper.getData(path, null, null);
		zookeeper.delete(path, -1);
		return data;
	}

	private class LatchChildWatcher implements Watcher {

		Object lock = new Object();
		private WatchedEvent event = null;

		public LatchChildWatcher() {
		}

		public LatchChildWatcher(Object lock) {
			this.lock = lock;
		}

		
		public void process(WatchedEvent event) {
			LOG.info("Watcher fired on path: " + event.getPath() + " state: "
					+ event.getState() + " type " + event.getType());
			synchronized (lock) {
				this.event = event;
				lock.notifyAll();
			}
		}

		public void await(long timeout) throws InterruptedException {
			synchronized (lock) {
				lock.wait(timeout);
			}
		}

		public WatchedEvent getWatchedEvent() {
			return event;
		}
	}

	
	public byte[] take() throws KeeperException, InterruptedException {
		TreeMap<Long, String> orderedChildren;
		
		while (true) {
			LatchChildWatcher childWatcher = new LatchChildWatcher();
			try {
				orderedChildren = orderedChildren(childWatcher);
			} catch (KeeperException.NoNodeException e) {
				zookeeper.create(dir, new byte[0], acl, CreateMode.PERSISTENT);
				continue;
			}
			if (orderedChildren.size() == 0) {
				childWatcher.await(DEFAULT_TIMEOUT);
				continue;
			}

			for (String headNode : orderedChildren.values()) {
				String path = dir + "/" + headNode;
				try {
					byte[] data = zookeeper.getData(path, null, null);
					zookeeper.delete(path, -1);
					return data;
				} catch (KeeperException.NoNodeException e) {
					e.printStackTrace();
				}
			}
		}
	}

	
	public boolean offer(byte[] data) throws KeeperException,
			InterruptedException {
		return createData(dir + "/" + prefix, data,
				CreateMode.PERSISTENT_SEQUENTIAL) != null;
	}

	
	private String createData(String path, byte[] data, CreateMode mode)
			throws KeeperException, InterruptedException {
		for (;;) {
			try {
				return zookeeper.create(path, data, acl, mode);
			} catch (KeeperException.NoNodeException e) {
				try {
					zookeeper.create(dir, new byte[0], acl,
							CreateMode.PERSISTENT);
				} catch (KeeperException.NodeExistsException ne) {
					
				}
			}
		}
	}

	
	public QueueEvent offer(byte[] data, long timeout) throws KeeperException,
			InterruptedException {
		String path = createData(dir + "/" + prefix, data,
				CreateMode.PERSISTENT_SEQUENTIAL);
		String watchID = createData(
				dir + "/" + response_prefix
						+ path.substring(path.lastIndexOf("-") + 1), null,
				CreateMode.EPHEMERAL);
		Object lock = new Object();
		LatchChildWatcher watcher = new LatchChildWatcher(lock);
		synchronized (lock) {
			if (zookeeper.exists(watchID, watcher) != null) {
				watcher.await(timeout);
			}
		}
		byte[] bytes = zookeeper.getData(watchID, null, null);
		zookeeper.delete(watchID, -1);
		return new QueueEvent(watchID, bytes, watcher.getWatchedEvent());
	}

	
	public byte[] peek() throws KeeperException, InterruptedException {
		try {
			return element().getBytes();
		} catch (NoSuchElementException e) {
			return null;
		}
	}

	public static class QueueEvent {
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((id == null) ? 0 : id.hashCode());
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
			QueueEvent other = (QueueEvent) obj;
			if (id == null) {
				if (other.id != null)
					return false;
			} else if (!id.equals(other.id))
				return false;
			return true;
		}

		private WatchedEvent event = null;
		private String id;
		private byte[] bytes;

		QueueEvent(String id, byte[] bytes, WatchedEvent event) {
			this.id = id;
			this.bytes = bytes;
			this.event = event;
		}

		public void setId(String id) {
			this.id = id;
		}

		public String getId() {
			return id;
		}

		public void setBytes(byte[] bytes) {
			this.bytes = bytes;
		}

		public byte[] getBytes() {
			return bytes;
		}

		public WatchedEvent getWatchedEvent() {
			return event;
		}

	}

	
	public QueueEvent peek(boolean block) throws KeeperException,
			InterruptedException {
		if (!block) {
			return element();
		}

		TreeMap<Long, String> orderedChildren;
		while (true) {
			LatchChildWatcher childWatcher = new LatchChildWatcher();
			try {
				orderedChildren = orderedChildren(childWatcher);
			} catch (KeeperException.NoNodeException e) {
				zookeeper.create(dir, new byte[0], acl, CreateMode.PERSISTENT);
				continue;
			}
			if (orderedChildren.size() == 0) {
				childWatcher.await(DEFAULT_TIMEOUT);
				continue;
			}

			for (String headNode : orderedChildren.values()) {
				String path = dir + "/" + headNode;
				try {
					byte[] data = zookeeper.getData(path, null, null);
					return new QueueEvent(path, data,
							childWatcher.getWatchedEvent());
				} catch (KeeperException.NoNodeException e) {
					// Another client deleted the node first.
				}
			}
		}
	}

	
	public byte[] poll() throws KeeperException, InterruptedException {
		try {
			return remove();
		} catch (NoSuchElementException e) {
			return null;
		}
	}

}
