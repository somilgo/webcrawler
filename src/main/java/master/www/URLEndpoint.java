package master.www;

import spark.Request;
import spark.Response;
import spark.Route;

public class URLEndpoint implements Route {
	@Override
	public Object handle(Request request, Response response) throws Exception {
		return "ok";
	}
}
