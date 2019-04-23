package master;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.persist.*;
import worker.URLInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.xml.bind.DatatypeConverter;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class RobotsStorage {

    public static Logger logger = LogManager.getLogger(RobotsStorage.class);
    public static final String USER_AGENT = "cis455crawler";
    private Storage robotsStore;
    private HashSet<String> currFetching = new HashSet<String>();

    public void init(String roboEnv) {
        robotsStore = new Storage(roboEnv);
    }

    private RobotsTxtInfo getRobots(URLInfo u) {
        while (currFetching.contains(u.getHostName())) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        currFetching.add(u.getHostName());
        if (robotsStore.get(u.getHostName()) != null) {
            currFetching.remove(u.getHostName());
            return robotsStore.get(u.getHostName());
        }

        String baseURI = u.getHostName();
        String url = null;

        if (u.isSecure()) url = "https://" + baseURI;
        else url = "http://" + baseURI;
        url += "/robots.txt";
        RobotsTxtInfo robotsInfo = new RobotsTxtInfo();
        robotsInfo.setURL(u.getHostName());
        try{
            logger.info(url + ": attempting to fetch robots.txt");
            URL robotUrl = new URL(url);
            HttpURLConnection conn = (HttpURLConnection) robotUrl.openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(5000);

            InputStreamReader isr = new InputStreamReader(conn.getInputStream());
            logger.info("fetched robots.txt success");
            BufferedReader in = new BufferedReader(isr);
            String line = null;
            String currentAgent = null;

            while((line = in.readLine()) != null) {
                if (line.equals("") || line.equals("\n")) continue;
                String[] lineParse = line.split(":");
                if (lineParse.length != 2) {
                    continue;
                }
                lineParse[0] = lineParse[0].trim().toLowerCase();
                lineParse[1] = lineParse[1].trim();
                if (lineParse[0].equals("user-agent")) {
                    robotsInfo.addUserAgent(lineParse[1]);
                    currentAgent = lineParse[1];
                } else if (lineParse[0].equals("disallow")) {
                    robotsInfo.addDisallowedLink(currentAgent, lineParse[1]);
                } else if (lineParse[0].equals("allow")) {
                    robotsInfo.addAllowedLink(currentAgent, lineParse[1]);
                } else if (lineParse[0].equals("sitemap")) {
                    robotsInfo.addSitemapLink(lineParse[1]);
                } else if (lineParse[0].equals("crawl-delay")) {
                    robotsInfo.addCrawlDelay(currentAgent, Integer.parseInt(lineParse[1]));
                }
            }
        } catch (Exception e) {
            logger.debug(u.getHostName() + ": unable to parse robots.txt");
            robotsInfo = new RobotsTxtInfo(); //Putting RTI placeholder so we don't try this again
            robotsInfo.setURL(u.getHostName());
        }

        robotsStore.put(robotsInfo);
        currFetching.remove(u.getHostName());
        return robotsInfo;
    }

    public void receiveCrawl(String url) {
        URLInfo u = new URLInfo(url);
        RobotsTxtInfo robots = getRobots(u);
        String urlToCrawl = null;
        synchronized (robots) {
            robots.crawl();
            urlToCrawl = robots.popToCrawl();
            robots.currCrawling = urlToCrawl != null;
            robotsStore.put(robots);
        }
        if (urlToCrawl != null) {
            final String threadurl = urlToCrawl;
            new Thread(){
                public void run(){
                    String agent = "*";
                    if (robots.containsUserAgent(USER_AGENT)) agent = USER_AGENT;
                    try {
                        Thread.sleep(robots.getCrawlDelay(agent) * 1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    CrawlMaster.urlCache.addFirst(threadurl);
                }
            }.start();
        }
    }

    /**
     * Returns true if the crawl delay says we should wait
     */
    public boolean deferCrawl(String site) {
        URLInfo u = new URLInfo(site);
        RobotsTxtInfo robots = getRobots(u);
        synchronized (robots) {
            long lastCrawled = robots.getLastCrawled();
            long currentTime = new Date().getTime();
            long delay = (currentTime - lastCrawled) / 1000;
            String agent = "*";
            if (robots.containsUserAgent(USER_AGENT)) agent = USER_AGENT;
            if (robots.crawlContainAgent(agent)) {
                if (delay < robots.getCrawlDelay(agent)) {
                    return true;
                }
            }
            robots.crawl();
            return false;
        }
    }

    public boolean nonZeroCrawlDelay(RobotsTxtInfo robots) {
        String agent = "*";
        if (robots.containsUserAgent(USER_AGENT)) agent = USER_AGENT;
        if (robots.crawlContainAgent(agent)) {
            if (robots.getCrawlDelay(agent) > 0) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns true if it's permissible to fetch the content,
     * eg that it satisfies the path restrictions from robots.txt
     */
    public boolean isOKtoCrawl(String site) {
        URLInfo url = new URLInfo(site);
        if (url.getHostName() == null) return false;
        RobotsTxtInfo robot = getRobots(url);
        String agent = "*";
        if (robot.containsUserAgent(USER_AGENT)) agent = USER_AGENT;

        String allowRule = "";
        //Check allowed links - we give allowed links precedence
        List<String> allowedLinks = robot.getAllowedLinks(agent);
        if (allowedLinks != null) {
            for (String link : allowedLinks) {
                if (url.getFilePath().startsWith(link)) {
                    if (link.length() > allowRule.length()) allowRule = link;
                }
            }
        }

        String disallowRule = "";
        //Check disallowed links
        List<String> disallowedLinks = robot.getDisallowedLinks(agent);
        if (disallowedLinks != null) {
            for (String link : disallowedLinks) {
                if (url.getFilePath().startsWith(link)) {
                    if (link.length() > disallowRule.length()) disallowRule = link;
                }
            }
        }
        if (disallowRule.length() != 0 || allowRule.length() < disallowRule.length()) {
            return false;
        } else {
            synchronized (robot) {
                if (robot.currCrawling && nonZeroCrawlDelay(robot)) {
                    robot.addToCrawl(site);
                    robotsStore.put(robot);
                    return false;
                }
                robot.currCrawling = true;
                robotsStore.put(robot);
                return true;
            }
        }
    }

}

class Storage {
    Logger logger = LogManager.getLogger(Storage.class);
    private String envHomeString;
    private StorageLayer sl;

    public Storage (String envHomeString) {
        this.envHomeString = envHomeString;
        sl = setup();
        close();
        sl = setup();
    }

    private StorageLayer setup() {
        sl = new StorageLayer();
        sl.setup(envHomeString);
        return sl;
    }

    public synchronized RobotsTxtInfo get(String url) {
        RobotsTxtInfo c = sl.robotByUrl.get(url);
        return c;
    }

    public synchronized void put(RobotsTxtInfo rti) {
        sl.robotByUrl.put(rti);
    }

    private void shutdown(StorageLayer sl) {
        sl.shutdown();
    }
    public void close() {
        shutdown(sl);
    }
}

class StorageLayer {
    Environment storageEnv;

    EntityStore robotStore;
    PrimaryIndex<String, RobotsTxtInfo> robotByUrl;

    public StorageLayer() {
    }

    public void setup(String envHomeString) {
        try {
            EnvironmentConfig storageEnvConfig = new EnvironmentConfig();
            storageEnvConfig.setAllowCreate(true);
            storageEnvConfig.setTransactional(true);

            StoreConfig robotStoreConfig = new StoreConfig();
            robotStoreConfig.setAllowCreate(true);
            robotStoreConfig.setTransactional(true);

            // Open the environment and entity store
            File envHome = new File(envHomeString);
            storageEnv = new Environment(envHome, storageEnvConfig);

            robotStore = new EntityStore(storageEnv, "ChannelStory", robotStoreConfig);
            robotByUrl = robotStore.getPrimaryIndex(String.class, RobotsTxtInfo.class);
        } catch (DatabaseException dbe) {
            System.err.println("Error opening environment and store: " +
                    dbe.toString());
            System.exit(-1);
        }
    }

    public void shutdown() {
        robotStore.close();
        storageEnv.close();
    }
}
