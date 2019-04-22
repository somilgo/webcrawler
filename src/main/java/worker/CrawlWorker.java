package worker;

import static spark.Spark.*;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import db.ContentHashDB;
import db.DocumentDB;
import db.RedirectDB;
import stormlite.Config;
import master.CrawlMaster;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;

import stormlite.LocalCluster;
import stormlite.TopologyContext;
import stormlite.distributed.WorkerHelper;
import stormlite.distributed.WorkerJob;
import stormlite.routers.IStreamRouter;
import stormlite.tuple.Tuple;
import spark.Spark;

/**
 * Simple listener for worker creation
 *
 * @author zives
 *
 */
public class CrawlWorker {
	public static final double MAX_DOC_SIZE = 2 * 1e6;
	static Logger log = LogManager.getLogger(CrawlWorker.class);
	public static final String MASTER_URL = "http://ec2-18-205-190-36.compute-1.amazonaws.com";
	public static final String MASTER_IP =  MASTER_URL + ":" + CrawlMaster.myPort;
	public static final String USER_AGENT = "cis455crawler";
	public static String port;

	static LocalCluster cluster = new LocalCluster();

	static List<TopologyContext> contexts = new ArrayList<>();

	int myPort;
	public static final LinkedList<String> results = new LinkedList<String>();
	public static String workerStatus = "idle";
	public static boolean running = true;
	public static String currJob;
	public static Config currConf;

	static List<String> topologies = new ArrayList<>();

	public CrawlWorker(int myPort) throws MalformedURLException {

		log.info("Creating server listener at socket " + myPort);

		port(myPort);
		final ObjectMapper om = new ObjectMapper();
		om.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
		Spark.post("/definejob", (req, res) -> {
			if (currJob != null) {
				return "Already has job";
			}
			WorkerJob workerJob;
			try {
				workerJob = om.readValue(req.body(), WorkerJob.class);
				currConf = workerJob.getConfig();
				log.info("Processing job definition request" + workerJob.getConfig().get("job") +
						" on machine " + workerJob.getConfig().get("workerIndex"));
				contexts.add(cluster.submitTopology(workerJob.getConfig().get("job"), workerJob.getConfig(),
						workerJob.getTopology()));
				currJob = workerJob.getConfig().get("job");

				// Add a new topology
				synchronized (topologies) {
					topologies.add(workerJob.getConfig().get("job"));
				}
				return "Job launched";
			} catch (IOException e) {
				e.printStackTrace();

				// Internal server error
				res.status(500);
				return e.getMessage();
			}

		});

		Spark.post("/runjob", new RunJobRoute(cluster));

	}

	public static void shutdown() {
		running = false;
		synchronized(topologies) {
			for (String topo: topologies)
				cluster.killTopology(topo);
		}

		cluster.shutdown();
		System.exit(0);
	}

	public static String statusParams(String port, String status,LinkedList<String> results) {
		String out = "";
		out += "?port=" + port;
		out += "&status=" + status;
		return out;
	}



	/**
	 * Simple launch for worker server.  Note that you may want to change / replace
	 * most of this.
	 *
	 * @param args
	 * @throws MalformedURLException
	 */
	public static void main(String args[]) throws IOException {
		if (args.length < 1) {
			System.out.println("Usage: WorkerServer [port number]");
			System.exit(1);
		}

		int myPort = Integer.valueOf(args[0]);
		port = args[0];

		System.out.println("Worker node startup, on port " + myPort);

		CrawlWorker worker = new CrawlWorker(myPort);
		get("/shutdown", (req, resp) ->{
			CrawlWorker.shutdown();
			return "shutdown worker";
		});
		get("/reset", (req, resp) ->{
			CrawlWorker.reset(false);
			return "reset worker";
		});

		try {
			DocumentDB.init();
			ContentHashDB.init();
			RedirectDB.init();
		} catch (Exception e) {
			e.printStackTrace();
		}

		sendWorkerStatus("idle");
		new Thread(){
			public void run(){
				while(running){
					sendStatusToMaster();
					try {
						Thread.sleep(10 * 1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}

			}
		}.start();

		System.out.println("Press [Enter] to shut down this worker...");

		(new BufferedReader(new InputStreamReader(System.in))).readLine();

		CrawlWorker.shutdown();
	}

	public static void sendWorkerStatus(String status) {
		workerStatus = status;
	}

	public static void addResult(String result) {
		results.add(result);
		while (results.size() > 100) {
			results.removeFirst();
		}
	}

	private static void sendStatusToMaster() {
		System.out.println("Sending status: " + workerStatus);
		String statusParams = statusParams(port, workerStatus, results);
		sendStatus(MASTER_IP, "POST", "update", statusParams);
	}

	public static HttpURLConnection sendStatus(String dest, String reqType, String path, String parameters) {
		HttpURLConnection conn = null;

		try {
			URL url = new URL(dest + "/" + path + parameters);

			log.info("Sending request to " + url.toString());

			conn = (HttpURLConnection)url.openConnection();
			conn.setDoOutput(true);
			conn.setRequestMethod(reqType);
			conn.getResponseCode();

		} catch (Exception e) {
			e.printStackTrace();
		}


		return conn;
	}

	private static String urlList(LinkedList<String> urls) {
		StringBuilder s = new StringBuilder();
		for (String u : urls) {
			s.append(u + ",");
		}
		return s.toString();
	}

	public static HttpURLConnection sendURLs(LinkedList<String> urls, String curr) {
		HttpURLConnection conn = null;

		try {
			URL url = new URL(MASTER_IP + "/geturls?crawled=" + curr + "&port=" + port);

			log.info("Sending request to " + url.toString());

			conn = (HttpURLConnection)url.openConnection();
			conn.setDoOutput(true);
			conn.setRequestMethod("POST");

			OutputStream os = conn.getOutputStream();
			byte[] toSend = urlList(urls).getBytes();
			os.write(toSend);
			os.flush();
			os.close();
			conn.getResponseCode();
		} catch (Exception e) {
			e.printStackTrace();
		}


		return conn;
	}

	public static void reset(boolean prop) {
		sendWorkerStatus("idle");
		cluster.killTopology(currJob);
		cluster.shutdown();
		currJob = null;
		String[] workers = WorkerHelper.getWorkers(currConf);
		if (prop)
			for (String w : workers) {
				sendStatus(w, "GET", "reset", "");
			}
		contexts.clear();
	}

	//Unused depite TestMapReduce
	public static void createWorker(Map<String, String> config) {
		if (!config.containsKey("workerList"))
			throw new RuntimeException("Worker spout doesn't have list of worker IP addresses/ports");

		if (!config.containsKey("workerIndex"))
			throw new RuntimeException("Worker spout doesn't know its worker ID");
		else {
			String[] addresses = WorkerHelper.getWorkers(config);
			String myAddress = addresses[Integer.valueOf(config.get("workerIndex"))];

			log.debug("Initializing worker " + myAddress);

			URL url;
			try {
				url = new URL(myAddress);

				new CrawlWorker(url.getPort());
			} catch (MalformedURLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
