package stormlite.bolt;

import db.ContentHashDB;
import db.DocumentDB;
import db.RedirectDB;
import org.apache.logging.log4j.LogManager;
import stormlite.OutputFieldsDeclarer;
import stormlite.TopologyContext;
import stormlite.routers.IStreamRouter;
import stormlite.tuple.Fields;
import stormlite.tuple.Tuple;
import stormlite.tuple.Values;
import org.apache.logging.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import worker.CrawlWorker;
import worker.URLInfo;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.BlockingQueue;

public class DocumentFetcherBolt implements IRichBolt {
    static Logger logger = LogManager.getLogger(DocumentFetcherBolt.class);
    String executorId = UUID.randomUUID().toString();
    private OutputCollector collector;
    private static final DocumentProcessorBolt dpb = new DocumentProcessorBolt();
    HashSet<String> redirects;
    Date crawlStart = new Date();

    @Override
    public void cleanup() {}

    @Override
    public void execute(Tuple input) {
        String url = (String) input.getValues().get(0);
        logger.info("Received url: " + url);
        this.redirects = new HashSet<String>();
        crawlUrl(url);
        logger.info("Crawled url: " + url);
    }

    @Override
    public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void setRouter(IStreamRouter router) {
        this.collector.setRouter(router);
    }

    @Override
    public Fields getSchema() {
        return null;
    }

    @Override
    public String getExecutorId() {
        return this.executorId;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("url", "rc", "ct", "doc"));
    }

    private static Map<String, List<String>> getResponseHeaders(String url, String accessTime) throws Exception {
        URL obj = new URL(url);
        URLInfo u = new URLInfo(url);
        HttpURLConnection con = null;

        con = (HttpURLConnection) obj.openConnection();

//        boolean firstSleep = true;
//        while (master.getRobotsStorage().deferCrawl(url)){
//            Thread.sleep(1000);
//            if (firstSleep) logger.info(url + ": deferring crawl");
//            firstSleep = false;
//        }

        con.setRequestMethod("HEAD");
        con.setReadTimeout(3000);
        //add request header
        con.setRequestProperty("User-Agent", CrawlWorker.USER_AGENT);
        if (accessTime != null) {
            con.setRequestProperty("If-Modified-Since", accessTime);
            logger.info(url + ": already in the DB");
        }
        Map<String, List<String>> respMap = con.getHeaderFields();
        Map<String, List<String>> outMap = new HashMap<String, List<String>>();
        outMap.putAll(respMap);

        List<String> respCode = new LinkedList<>();
        respCode.add("" + con.getResponseCode());
        List<String> newUrl = new LinkedList<>();
        newUrl.add(con.getURL().toString());

        outMap.put("response-code", respCode);
        outMap.put("new-urls", newUrl);

        con.disconnect();
        return outMap;
    }

    private static String getResponseContent(String url) throws Exception {
        URL obj = new URL(url);
        URLInfo u = new URLInfo(url);
        HttpURLConnection con = null;

        con = (HttpURLConnection) obj.openConnection();

//        boolean firstSleep = true;
//        while (master.getRobotsStorage().deferCrawl(url)) {
//            Thread.sleep(1000);
//            if (firstSleep) logger.info(url + ": deferring crawl");
//            firstSleep = false;
//        }

        // optional default is GET
        con.setRequestMethod("GET");
        con.setReadTimeout(5000);
        //add request header
        con.setRequestProperty("User-Agent", CrawlWorker.USER_AGENT);

        BufferedReader in = new BufferedReader(
                new InputStreamReader(con.getInputStream()));
        String inputLine;
        StringBuffer content = new StringBuffer();
        while (content.length() < CrawlWorker.MAX_DOC_SIZE &&
                ((inputLine = in.readLine()) != null)) {
            content.append(inputLine + '\n');
        }
        in.close();
        String out = content.toString();

        con.disconnect();
        return out;
    }

    private static boolean checkValidType(String type) {
        if (type == null) return false;
        if (type.contains("text/html") ||
                type.contains("text/xml") ||
                type.contains("application/xml") ||
                type.contains("+xml")) {
            return true;
        }
        return false;
    }

    private static boolean checkValidLength(String length) {
        if (length == null) return false;
        int lengthInt = Integer.parseInt(length);
        if (CrawlWorker.MAX_DOC_SIZE < lengthInt) {
            return false;
        }
        return true;
    }

    private static String createBaseURI(String url) {
        URLInfo u = new URLInfo(url);
        String baseURI = u.getHostName();
        if (u.isSecure()) baseURI = "https://" + baseURI;
        else baseURI = "http://" + baseURI;
        return baseURI;
    }

    private void crawlUrl(String url) {

//        if (ContentHashDB.contains(url) || ContentHashDB.contains(new URLInfo(url).getHostName())) {
//            CrawlWorker.sendURLs(new LinkedList<>(), url);
//            return;
//        }

        DateFormat dateFormat = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz");
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        Date accessDate = DocumentDB.getDocumentTime(url);
        String accessTime = null;
        if (accessDate != null) {
            if (crawlStart.compareTo(accessDate) < 0) {
                CrawlWorker.sendURLs(new LinkedList<>(), url);
                return;
            }
            accessTime = dateFormat.format(accessDate);
        }
        Map<String, List<String>> respHeaders = null;

        URLInfo u = new URLInfo(url);

        while (respHeaders == null)
            try {
                respHeaders = getResponseHeaders(url, accessTime);
            } catch (SocketTimeoutException e) {
                logger.info(url + ": response timed out, ditching url.");
                CrawlWorker.sendURLs(new LinkedList<>(), url);
                return;
            } catch (Exception e) {
                logger.info(url + ": error trying to connect to url - discarding");
                CrawlWorker.sendURLs(new LinkedList<>(), url);
                return;
            }
        String responseCode = respHeaders.get("response-code").get(0);
        String newUrl = respHeaders.get("new-urls").get(0);

        if (!newUrl.equals(url)) {
			//RedirectDB.addRedirectURL(url, newUrl);
            url = newUrl;
        }

        String content = null;
        if (responseCode.equals("304")) {
            content = DocumentDB.getDocumentContent(url);
            logger.info(url + ": Not Modified");
        } else if (responseCode.startsWith("3")) {
            if (respHeaders.get("Location") != null) {
                String redirectLink = respHeaders.get("Location").get(0);
                if (redirectLink.startsWith("http://") ||
                        redirectLink.startsWith("https://")) {
                    LinkedList<String> redirectUrlSend = new LinkedList<String>();
                    redirectUrlSend.add(redirectLink);
                    //CrawlWorker.sendURLs(redirectUrlSend, url);
                    //RedirectDB.addRedirectURL(url, redirectLink);
                    logger.info(url + ": redirected - will crawl redirection link: " + redirectLink);
                    if (!url.equals(redirectLink) && !redirects.contains(url)) {
                        redirects.add(url);
                        crawlUrl(redirectLink);
                    }
                    return;
                }
                else {
                    String baseURI = createBaseURI(url);
                    LinkedList<String> redirectUrlSend = new LinkedList<String>();
                    redirectUrlSend.add(baseURI + redirectLink);
                    //CrawlWorker.sendURLs(redirectUrlSend, url);
                    //RedirectDB.addRedirectURL(url, baseURI+redirectLink);
                    logger.info(url + ": redirected - will crawl redirection link");
                    if (!url.equals(redirectLink) && !redirects.contains(url)) {
                        redirects.add(url);
                        crawlUrl(baseURI + redirectLink);
                    }
                    return;
                }

            }
            CrawlWorker.sendURLs(new LinkedList<>(), url);
            return;
        } else if (responseCode.startsWith("2")) {
            boolean validType = true;
            if (respHeaders.get("Content-Type") != null) {
                String mimeType = respHeaders.get("Content-Type").get(0);
                validType = checkValidType(mimeType);
            }

            boolean validLength = true;
            if (respHeaders.get("Content-Length") != null) {
                String contentLength = respHeaders.get("Content-Length").get(0);
                validLength = checkValidLength(contentLength);
            }

            if (!validType || !validLength) {
                logger.info(url + ": invalid url type or length - skipping");
                CrawlWorker.sendURLs(new LinkedList<>(), url);
                return;
            }
            while (content == null)
                try {
                    content = getResponseContent(url);
                    logger.info(url + ": Downloading");
                } catch (SocketTimeoutException e) {
                    logger.info(url + ": response timed out, ditching url.");
                    CrawlWorker.sendURLs(new LinkedList<>(), url);
                    //ContentHashDB.addHash(new URLInfo(url).getHostName());
                    return;
                } catch (Exception e) {
                    logger.info(url + ": error trying to connect to url - discarding");
                    CrawlWorker.sendURLs(new LinkedList<>(), url);
                    //ContentHashDB.addHash(url);
                    return;
                }
        } else {
            logger.info(url + ": bad response code - skipping");
            //ContentHashDB.addHash(new URLInfo(url).getHostName());
            CrawlWorker.sendURLs(new LinkedList<>(), url);
            return;
        }
        if (ContentHashDB.contains(content)) {
            logger.info(url + ": already seen this content - skipping");
            CrawlWorker.sendURLs(new LinkedList<>(), url);
            //ContentHashDB.addHash(url);
            return;
        } else {
            String contentType;
            if (respHeaders.get("Content-Type") == null) contentType = null;
            else contentType = respHeaders.get("Content-Type").get(0);
            logger.info("DFB emitting document: " + url);
            dpb.execute(new Tuple(this.getSchema(), (new Values<Object>(url, responseCode, contentType, content))));
            //collector.emit(new Values<Object>(url, responseCode, contentType, content));
        }

    }
}
