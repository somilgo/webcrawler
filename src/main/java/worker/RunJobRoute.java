package worker;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import spark.Request;
import spark.Response;
import spark.Route;
import stormlite.LocalCluster;

public class RunJobRoute implements Route {
	static Logger log = LogManager.getLogger(RunJobRoute.class);
	LocalCluster cluster;
	
	public RunJobRoute(LocalCluster cluster) {
		this.cluster = cluster;
	}

	@Override
	public Object handle(Request request, Response response) throws Exception {
		log.info("Starting job!");
		cluster.startTopology();

		return "Started";
	}

}
