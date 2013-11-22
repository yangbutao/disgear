package com.newcosoft.cache;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.noggit.JSONUtil;
import org.noggit.JSONWriter;

import com.newcosoft.zookeeper.ZkNodeProps;

public class Shard extends ZkNodeProps {
	public static String REPLICAS = "replicas";
	public static String RANGE = "range";
	public static String STATE = "state";
	public static String LEADER = "leader"; // FUTURE: do we want to record the
											// leader as a slice property in the
											// JSON (as opposed to isLeader as a
											// replica property?)
	public static String ACTIVE = "active";
	public static String INACTIVE = "inactive";
	public static String CONSTRUCTION = "construction";

	private final String name;
	private final Range range;
	private final Map<String, Replica> replicas;
	private final Replica leader;
	private final String state;

	/**
	 * @param name
	 *            The name of the slice
	 * @param replicas
	 *            The replicas of the slice. This is used directly and a copy is
	 *            not made. If null, replicas will be constructed from props.
	 * @param props
	 *            The properties of the slice - a shallow copy will always be
	 *            made.
	 */
	public Shard(String name, Map<String, Replica> replicas,
			Map<String, Object> props) {
		super(props == null ? new LinkedHashMap<String, Object>(2)
				: new LinkedHashMap<String, Object>(props));
		this.name = name;
		/*
		 * propMap = new HashMap<String, Object>(); propMap.put(RANGE,
		 * props.get(RANGE));
		 */
		Object rangeObj = propMap.get(RANGE);
		if (propMap.containsKey(STATE) && propMap.get(STATE) != null)
			this.state = (String) propMap.get(STATE);
		else {
			this.state = ACTIVE; // Default to ACTIVE
			propMap.put(STATE, this.state);
		}
		Range tmpRange = null;
		if (rangeObj instanceof Range) {
			tmpRange = (Range) rangeObj;
		} else if (rangeObj != null) {
			// Doesn't support custom implementations of Range, but currently
			// not needed.
			tmpRange = Router.DEFAULT.fromString(rangeObj.toString());
		}
		range = tmpRange;

		// add the replicas *after* the other properties (for aesthetics, so
		// it's easy to find slice properties in the JSON output)
		this.replicas = replicas != null ? replicas
				: makeReplicas((Map<String, Object>) propMap.get(REPLICAS));
		propMap.put(REPLICAS, this.replicas);

		leader = findLeader();
	}

	private Map<String, Replica> makeReplicas(
			Map<String, Object> genericReplicas) {
		if (genericReplicas == null)
			return new HashMap<String, Replica>(1);
		Map<String, Replica> result = new LinkedHashMap<String, Replica>(
				genericReplicas.size());
		for (Map.Entry<String, Object> entry : genericReplicas.entrySet()) {
			String name = entry.getKey();
			Object val = entry.getValue();
			Replica r;
			if (val instanceof Replica) {
				r = (Replica) val;
			} else {
				r = new Replica(name, (Map<String, Object>) val);
			}
			result.put(name, r);
		}
		return result;
	}

	private Replica findLeader() {
		for (Replica replica : replicas.values()) {
			if (replica.getStr(LEADER) != null)
				return replica;
		}
		return null;
	}

	/**
	 * Return slice name (shard id).
	 */
	public String getName() {
		return name;
	}

	/**
	 * Gets the list of replicas for this slice.
	 */
	public Collection<Replica> getReplicas() {
		return replicas.values();
	}

	/**
	 * Get the map of coreNodeName to replicas for this slice.
	 */
	public Map<String, Replica> getReplicasMap() {
		return replicas;
	}

	public Map<String, Replica> getReplicasCopy() {
		return new LinkedHashMap<String, Replica>(replicas);
	}

	public Replica getLeader() {
		return leader;
	}

	public Replica getReplica(String replicaName) {
		return replicas.get(replicaName);
	}

	public Range getRange() {
		return range;
	}

	public String getState() {
		return state;
	}

	@Override
	public String toString() {
		return name + ':' + JSONUtil.toJSON(propMap);
	}

	@Override
	public void write(JSONWriter jsonWriter) {
		jsonWriter.write(propMap);
	}

}
