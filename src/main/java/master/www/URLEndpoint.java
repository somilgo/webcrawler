package master.www;

import master.CrawlMaster;
import spark.Request;
import spark.Response;
import spark.Route;

public class URLEndpoint implements Route {
	@Override
	public Object handle(Request request, Response response) throws Exception {
		String url =  CrawlMaster.urlCache.poll();
		if (url == null) return "";
		CrawlMaster.log.info("Outputting URL! : " + url);
		return url;
	}
}
