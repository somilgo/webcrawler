package master.www;

import master.CrawlMaster;
import spark.Request;
import spark.Response;
import spark.Route;
import worker.CrawlWorker;

import java.nio.file.Files;
import java.nio.file.Paths;

import static spark.Spark.get;

public class StatusPageHandler implements Route {

	@Override
	public Object handle(Request request, Response response) throws Exception {
			response.type("text/html");

			String out = "<html><body>";

			out += "<style>\n" +
					"table, th, td {\n" +
					"  border: 1px solid black;\n" +
					"}\n" +
					"</style>";

			out+= "Somil Govani (somilgo) <br/><table style=\"width:100%\"><tr><th>Index</th><th>IP</th><th>Status</th><th>Job</th>" +
					"</tr>";
			int index = 0;
			for (String worker : CrawlMaster.workers) {
				out+= "<tr>";

				String ip = CrawlMaster.workers.get(index);
				out += "<td>" + index + "</td>";
				out += "<td>" + ip + "</td>";
				out += "<td>" + CrawlMaster.workerStatuses.get(ip) + "</td>";

				String job = CrawlMaster.workerJobs.get(ip);
				if (job != null) {
					out += "<td>" + job + "</td>";
				} else {
					out += "<td>" + "None" + "</td>";
				}

				out+= "</tr>";
				index++;
			}

			out += "</table>";

			out += "</body></html>";
			return out;
	}
}
