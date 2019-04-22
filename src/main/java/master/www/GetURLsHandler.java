package master.www;

import master.CrawlMaster;
import master.RobotsStorage;
import spark.Request;
import spark.Response;
import spark.Route;

public class GetURLsHandler implements Route {
	@Override
	public Object handle(Request request, Response response) throws Exception {
		String urlBody = request.body();
		String crawledUrl = request.queryParams("crawled");
		CrawlMaster.SEND_COUNT.getAndDecrement();
		String port = request.queryParams("port");

		String ip = request.ip();
		ip = "http://" + ip + ":" + port;

		CrawlMaster.workerLastCrawled.put(ip, crawledUrl);
		if (urlBody == null) return "";
		String[] urls = urlBody.split(",");
		if (urls == null) return "";
		for (String u : urls) {
			if (u.length() > 0) {
				CrawlMaster.urls.push(u);
			}

		}

		return "ok";
	}
}
