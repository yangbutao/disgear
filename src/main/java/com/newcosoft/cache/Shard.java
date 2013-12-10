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
	public static String LEADER = "leader"; 
	public static String ACTIVE = "active";
	public static String INACTIVE = "inactive";
	public static String CONSTRUCTION = "construction";

	private final String name;
	private final Range range;
	private final Map<String, Replica> replicas;
	private final Replica leader;
	private final String state;

	
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
			this.state = ACTIVE; 
			propMap.put(STATE, this.state);
		}
		Range tmpRange = null;
		if (rangeObj instanceof Range) {
			tmpRange = (Range) rangeObj;
		} else if (rangeObj != null) {

			tmpRange = Router.DEFAULT.fromString(rangeObj.toString());
		}
		range = tmpRange;


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

	
	public String getName() {
		return name;
	}

	
	public Collection<Replica> getReplicas() {
		return replicas.values();
	}

	
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
