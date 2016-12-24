package org.spiget.logparser;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;

@Data
public class Stats {

	public int total;
	public int unique;
	public Map<String, Integer> userAgents = new HashMap<>();
	public Map<String, Integer> paths      = new HashMap<>();
	public Map<String, Integer> methods    = new HashMap<>();
	public Map<String, Integer> servers    = new HashMap<>();
	public long timestamp;

	public void increaseUserAgent(String userAgent) {
		increaseMap(userAgents, userAgent);
	}

	public void increasePath(String path) {
		increaseMap(paths, path);
	}

	public void increaseMethod(String method) {
		increaseMap(methods, method);
	}

	public void increaseServer(String server) {
		increaseMap(servers, server);
	}

	void increaseMap(Map<String, Integer> map, String key) {
		if (map.containsKey(key)) {
			map.put(key, map.get(key) + 1);
		} else {
			map.put(key, 1);
		}
	}

}
