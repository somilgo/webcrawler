package master;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import db.ContentHashDB;
import db.DocumentDB;
import db.RedirectDB;
import master.www.GetURLsHandler;
import master.www.URLEndpoint;
import master.www.WorkerUpdateHandler;
import stormlite.Config;
import stormlite.Topology;
import stormlite.TopologyBuilder;
import stormlite.bolt.DocumentFetcherBolt;
import stormlite.bolt.DocumentProcessorBolt;
import stormlite.distributed.WorkerHelper;
import stormlite.distributed.WorkerJob;
import stormlite.spout.URLSpout;
import stormlite.tuple.Fields;
import worker.CrawlWorker;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import spark.Request;
import spark.Response;
import spark.Route;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static spark.Spark.*;
import master.www.StatusPageHandler;

public class CrawlMaster {

	public static int mqport = 7999;
	public static Logger log = LogManager.getLogger(CrawlMaster.class);
	static final long serialVersionUID = 455555001;
	public static final int myPort = 8000;
	private static final String URL_SPOUT = "URL_SPOUT";
	private static final String DOC_FETCH_BOLT = "DOC_FETCH_BOLT";
	private static final String DOC_PROC_BOLT = "DOC_PROC_BOLT";
	public static final String URL_Q = "somilsurls";
	private static final String URL_STORE_ENV = "urls";
	private static final String ROBOT_STORE_ENV = "test_robots";
	public static RobotsStorage ROBOTS;
	public static Channel channel;
	public static Connection connection;
	public static final List<String> workers = new LinkedList<String>();
	public static final HashMap<String, String> workerStatuses = new HashMap<String, String>();
	public static final HashMap<String, String> workerJobs = new HashMap<String, String>();
	public static final HashMap<String, String> workerLastCrawled = new HashMap<String, String>();
	public static URLStore urls;
	public static LinkedBlockingDeque<String> urlCache = new LinkedBlockingDeque<>();
	public static AtomicInteger SEND_COUNT = new AtomicInteger(0);
	private static Random rand = new Random();
	private static AtomicInteger urlThreadCount = new AtomicInteger(0);

	private static final HashSet<Thread> urlThreads = new HashSet<Thread>();
	private static int MAX_THREADS = 49;

	private static void registerStatusPage() { get("/status", new StatusPageHandler()); }
	private static void registerWorkerStatusHandler() {
		post("/update", new WorkerUpdateHandler());
	}
	private static void registerGetURLs() { post("/geturls", new GetURLsHandler()); }
	private static void registerURLEndpoint() {
		System.out.println("HERE"); get("/urlendpoint", new URLEndpoint());
	}
	private static void registerShutdown() {
		get("/shutdown", (req, resp) -> {
			shutdown();
			return "Shutting down master and known workers...";
		});
	}

	private static String startJob() {
		Config config = new Config();

		URLSpout spout = new URLSpout();
		DocumentFetcherBolt fetcher = new DocumentFetcherBolt();
		DocumentProcessorBolt processor = new DocumentProcessorBolt();

		TopologyBuilder builder = new TopologyBuilder();

			// Only one source ("spout") for the words
			builder.setSpout(URL_SPOUT, spout, 20);

		// Parallel mappers, each of which gets specific words
		builder.setBolt(DOC_FETCH_BOLT, fetcher, 1).shuffleGrouping(URL_SPOUT);

		Topology topo = builder.createTopology();

		WorkerJob job = new WorkerJob(topo, config);

		ObjectMapper mapper = new ObjectMapper();
		mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
		try {
			int i = 0;
			for (String dest: workers) {
				if (workerStatuses.get(dest) != null && !workerStatuses.get(dest).equals("idle")) {
					continue;
				}
				config.put("workerIndex", String.valueOf(i++));
				if (sendJob(dest, "POST", "definejob",
						mapper.writerWithDefaultPrettyPrinter().writeValueAsString(job)).getResponseCode() !=
						HttpURLConnection.HTTP_OK) {
					throw new RuntimeException("Job definition request failed");
				}
			}
			for (String dest : workers) {
				if (sendJob(dest, "POST", "runjob", "").getResponseCode() !=
						HttpURLConnection.HTTP_OK) {
					throw new RuntimeException("Job execution request failed");
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return "There was an error creating the job...\n" + e.toString();
		}
		return "Job was created successfully!";
	}

	private static void shutdown() {
		for (String w : workers) {
			try {
				sendJob(w, "GET", "shutdown", "");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		System.exit(0);
	}

	static HttpURLConnection sendJob(String dest, String reqType, String job, String parameters) throws Exception {
		URL url = new URL(dest + "/" + job);

		log.info("Sending request to " + url.toString());

		HttpURLConnection conn = (HttpURLConnection)url.openConnection();
		conn.setDoOutput(true);
		conn.setRequestMethod(reqType);

		if (reqType.equals("POST")) {
			conn.setRequestProperty("Content-Type", "application/json");

			OutputStream os = conn.getOutputStream();
			byte[] toSend = parameters.getBytes();
			os.write(toSend);
			os.flush();
			System.out.println(conn.getResponseCode());
		} else
			conn.getResponseCode();

		return conn;
	}

	public static void sendURL(String url) {
		int workerIndex = rand.nextInt(workers.size());
		String dest = workers.get(workerIndex);
		String path = "newurl?url=" + url;
		String params = "";
		try {
			sendJob(dest, "POST", path, params);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void outputURL(String url) {
		try {
			channel.basicPublish("", URL_Q, null, url.getBytes("UTF-8"));
			System.out.println(" [x] Sent '" + url + "'");

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void initMQ() {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("0.0.0.0");
		try {
			connection = factory.newConnection();
			channel = connection.createChannel();
			channel.queueDeclare(URL_Q, false, false, false, null);
		} catch (TimeoutException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}



	private static void outputURLs() {
		while (true) {
			int cacheSize;
			synchronized (urlCache) {
				cacheSize = urlCache.size();
			}
			int urlThreadCountCurr = urlThreadCount.get();
			int repeat_count = 0;
			while (cacheSize > 500 ||  urlThreadCountCurr > MAX_THREADS) {
				try {
					Thread.sleep(100);
					synchronized (urlCache) {
						log.info("Slept because urlCache size=" + cacheSize
								+ " and thread count = " + urlThreadCountCurr);
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				synchronized (urlCache) {
					cacheSize = urlCache.size();
				}
				urlThreadCountCurr = urlThreadCount.get();
				if (urlThreadCountCurr > MAX_THREADS) {
					repeat_count += 1;
				}
				if (repeat_count > 3) {
					MAX_THREADS += 30;
				}
			}
			String url = null;
			try {
				url = urls.poll();
			} catch (IOException e) {
				e.printStackTrace();
			}
			if (url != null) {
				final String threadurl = url;
				Thread t = new Thread(){
					public void run(){
						urlThreadCount.getAndIncrement();
						try{
							boolean robotsBool = ROBOTS.isOKtoCrawl(threadurl);
							if (robotsBool) {
								urlCache.add(threadurl);
							}
						} catch (Exception e) {
							e.printStackTrace();
						}
						urlThreadCount.getAndDecrement();
					}
				};

				t.start();
			}


			try {
//				while (SEND_COUNT.get() > 500) {
//					log.info("Pausing to let workers catch up");
//					Thread.sleep(1000);
//				}
				if (url == null) {
					System.out.println("null url sleeping...");
					Thread.sleep(1000);
				} else {
					//Thread.sleep(100);
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * The mainline for launching a MapReduce Master.  This should
	 * handle at least the status and workerstatus routes, and optionally
	 * initialize a worker as well.
	 *
	 * @param args
	 */
	public static void main(String[] args) throws IOException {
		port(myPort);

		System.out.println("Master node startup");

		registerWorkerStatusHandler();
		registerStatusPage();
		registerShutdown();
		registerGetURLs();
		registerURLEndpoint();
		//threadPool(20, 2, 2500);
		//initMQ();
//		System.out.println("Press [Enter] to initialize workers...");
//		(new BufferedReader(new InputStreamReader(System.in))).readLine();
//

		System.out.println("Press [Enter] to start crawl...");
		(new BufferedReader(new InputStreamReader(System.in))).readLine();

		Path p = Paths.get(ROBOT_STORE_ENV);
		if (Files.exists(p)) {
			//deleteFolder(new File(ROBOT_STORE_ENV));
		} else {
			new File(ROBOT_STORE_ENV).mkdir();
		}
		ROBOTS = new RobotsStorage();
		ROBOTS.init(ROBOT_STORE_ENV);

		p = Paths.get(URL_STORE_ENV);
		if (Files.exists(p)) {
//			deleteFolder(new File(URL_STORE_ENV));
//			new File(URL_STORE_ENV).mkdir();
		} else {
			new File(URL_STORE_ENV).mkdir();
		}
		urls = new URLStore(URL_STORE_ENV, "URLSTORE", 1000);
//		urls.push("https://dbappserv.cis.upenn.edu/crawltest/nytimes/");
//		urls.push("https://dbappserv.cis.upenn.edu/crawltest/bbc/");
//		urls.push("http://niharpatil.me");
//		urls.push("https://www.techmeme.com/");
//		urls.push("http://dmoz-odp.org/");
		urls.push("https://en.wikipedia.org/wiki/Agriculture");
		urls.push("https://en.wikipedia.org/wiki/Culture");
		//urls.push("https://www.reddit.com/r/popular");
//		urls.push("http://www.ebizmba.com/articles/news-websites");
//		urls.push("http://digg.com/");
//		urls.push("https://www.dtelepathy.com/blog/inspiration/14-beautiful-content-heavy-websites-for-inspiration");
//		urls.push("https://techcrunch.com/");
//		urls.push("https://www.reddit.com");
//		urls.push("https://www.google.com");
//		urls.push("https://www.reddit.com/r/news");
//		urls.push("https://www.medium.com");


////		urls.push("https://www.imdb.com/");
//		urls.push("https://moz.com/top500");

//		urls.push("http://digg.com");
//		//urls.push("http://stackoverflow.com");
//		urls.push("https://en.wikipedia.org/wiki/Adolf_Hitler");
		startJob();
		outputURLs();



		System.out.println("Press [Enter] to shut down the master...");
		(new BufferedReader(new InputStreamReader(System.in))).readLine();

		CrawlMaster.shutdown();
	}



	public static void deleteFolder(File folder) {
		File[] files = folder.listFiles();
		if(files!=null) { //some JVMs return null for empty dirs
			for(File f: files) {
				if(f.isDirectory()) {
					deleteFolder(f);
				} else {
					f.delete();
				}
			}
		}
		folder.delete();
	}
}

