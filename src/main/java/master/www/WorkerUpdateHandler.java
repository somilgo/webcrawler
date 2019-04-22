package master.www;

import master.CrawlMaster;
import spark.Request;
import spark.Response;
import spark.Route;

public class WorkerUpdateHandler implements Route {

	@Override
	public Object handle(Request request, Response response) throws Exception {
		String status = request.queryParams("status");
		if (status == null) {
			return "Need to send workerstatus with a valid status in queryParams!";
		}
		String port = request.queryParams("port");
		if (port == null) {
			return "Need to send workstatus with a valid port!";
		}

		String ip = request.ip();
		ip = "http://" + ip + ":" + port;

		switch(status) {
			case "idle":
				if (!CrawlMaster.workers.contains(ip)) CrawlMaster.workers.add(ip);
				CrawlMaster.workerStatuses.put(ip, "idle");
				System.out.println("Available workers: " + CrawlMaster.workers);
				break;
			case "waiting":
				CrawlMaster.workerStatuses.put(ip, "waiting");
				break;
			case "mapping":
				CrawlMaster.workerStatuses.put(ip, "mapping");
				break;
			case "reducing":
				CrawlMaster.workerStatuses.put(ip, "reducing");
				break;
			default:
				System.out.println("Incorrect status sent with worker status");
		}

		return "Recorded worker status successfully";
	}
}