package com.newcosoft.zookeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeaderElector {

	private static Logger log = LoggerFactory.getLogger(LeaderElector.class);

	static final String ELECTION_NODE = "/election";

	private final static Pattern LEADER_SEQ = Pattern
			.compile(".*?/?.*?-n_(\\d+)");
	private final static Pattern SESSION_ID = Pattern
			.compile(".*?/?(.*?-.*?)-n_\\d+");

	protected ZooKeeper zkClient;

	public LeaderElector(ZooKeeper zkClient) {
		this.zkClient = zkClient;
	}

	
	private void checkIfIamLeader(final int seq, final BaseElectionContext context,
			boolean replacement) throws KeeperException, InterruptedException,
			IOException {
		
		final String holdElectionPath = context.electionPath + ELECTION_NODE;
		List<String> seqs = zkClient.getChildren(holdElectionPath, null);

		sortSeqs(seqs);
		List<Integer> intSeqs = getSeqs(seqs);
		if (intSeqs.size() == 0) {
			log.warn("Our node is no longer to be leader");
			return;
		}
		if (seq <= intSeqs.get(0)) {
			
			try {
				zkClient.delete(context.leaderPath, -1);
			} catch (Exception e) {
				
			}

			runIamLeaderProcess(context, replacement);
		} else {
			
			int i = 1;
			for (; i < intSeqs.size(); i++) {
				int s = intSeqs.get(i);
				if (seq < s) {
					
					break;
				}
			}
			int index = i - 2;
			if (index < 0) {
				log.warn("Our node is no longer to be leader");
				return;
			}

			zkClient.getData(holdElectionPath + "/" + seqs.get(index),
					new Watcher() {

					
						public void process(WatchedEvent event) {

							if (EventType.None.equals(event.getType())) {
								return;
							}
							// am I the next leader?
							try {
								checkIfIamLeader(seq, context, true);
							} catch (InterruptedException e) {

								Thread.currentThread().interrupt();
								log.warn("", e);
							} catch (IOException e) {
								log.warn("", e);
							} catch (Exception e) {
								log.warn("", e);
							}
						}

					}, null, true);

		}
	}

	protected void runIamLeaderProcess(final BaseElectionContext context,
			boolean weAreReplacement) throws KeeperException,
			InterruptedException, IOException {
		context.runLeaderProcess(weAreReplacement);
	}


	private int getSeq(String nStringSequence) {
		int seq = 0;
		Matcher m = LEADER_SEQ.matcher(nStringSequence);
		if (m.matches()) {
			seq = Integer.parseInt(m.group(1));
		} else {
			throw new IllegalStateException("Could not find regex match in:"
					+ nStringSequence);
		}
		return seq;
	}

	private String getNodeId(String nStringSequence) {
		String id;
		Matcher m = SESSION_ID.matcher(nStringSequence);
		if (m.matches()) {
			id = m.group(1);
		} else {
			throw new IllegalStateException("Could not find regex match in:"
					+ nStringSequence);
		}
		return id;
	}

	
	private List<Integer> getSeqs(List<String> seqs) {
		List<Integer> intSeqs = new ArrayList<Integer>(seqs.size());
		for (String seq : seqs) {
			intSeqs.add(getSeq(seq));
		}
		return intSeqs;
	}

	
	public String joinElection(BaseElectionContext context, boolean replacement)
			throws KeeperException, InterruptedException, IOException {
		final String shardsElectZkPath = context.electionPath
				+ LeaderElector.ELECTION_NODE;

		long sessionId = zkClient.getSessionId();
		String id = sessionId + "-" + context.id;
		String leaderSeqPath = null;
		boolean cont = true;
		int tries = 0;
		while (cont) {
			try {
				leaderSeqPath = zkClient.create(shardsElectZkPath + "/" + id
						+ "-n_", null, ZooDefs.Ids.OPEN_ACL_UNSAFE,
						CreateMode.EPHEMERAL_SEQUENTIAL);
				context.leaderSeqPath = leaderSeqPath;
				cont = false;
			} catch (ConnectionLossException e) {
				
				List<String> entries = zkClient.getChildren(shardsElectZkPath,
						null);

				boolean foundId = false;
				for (String entry : entries) {
					String nodeId = getNodeId(entry);
					if (id.equals(nodeId)) {
						
						foundId = true;
						break;
					}
				}
				if (!foundId) {
					cont = true;
					if (tries++ > 20) {
						throw new RuntimeException(e);
					}
					try {
						Thread.sleep(50);
					} catch (InterruptedException e2) {
						Thread.currentThread().interrupt();
					}
				}

			} catch (KeeperException.NoNodeException e) {
				
				if (tries++ > 20) {
					throw new RuntimeException(e);
				}
				cont = true;
				try {
					Thread.sleep(50);
				} catch (InterruptedException e2) {
					Thread.currentThread().interrupt();
				}
			}
		}
		int seq = getSeq(leaderSeqPath);
		checkIfIamLeader(seq, context, replacement);

		return leaderSeqPath;
	}

	
	public void setup(final BaseElectionContext context)
			throws InterruptedException, KeeperException {
		String electZKPath = context.electionPath + LeaderElector.ELECTION_NODE;

		new ZookeeperClient(zkClient).ensureExists(electZKPath, null,
				CreateMode.PERSISTENT);
	}

	
	private void sortSeqs(List<String> seqs) {
		Collections.sort(seqs, new Comparator<String>() {

			
			public int compare(String o1, String o2) {
				return Integer.valueOf(getSeq(o1)).compareTo(
						Integer.valueOf(getSeq(o2)));
			}
		});
	}
}
