package stormlite.distributed;

import java.util.Map;

public class WorkerHelper {
	
	/**
	 * Get the list of workers
	 * 
	 * @param config
	 * @return
	 */
	public static String[] getWorkers(Map<String, String> config) {
		String list = config.get("workerList");
		if (list.startsWith("["))
			list = list.substring(1);
		if (list.endsWith("]"))
			list = list.substring(0, list.length() - 1);
		
		String[] servers = list.split(",");
		
		String[] ret = new String[servers.length];
		int i = 0;
		for (String itemDirty: servers){
			String item = itemDirty.trim();
			if (!item.startsWith("http"))
				ret[i++] = "http://" + item;
			else
				ret[i++] = item;
		}
			
		return ret;
	}
}
