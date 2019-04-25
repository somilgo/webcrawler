package stormlite.bolt;

import db.ContentHashDB;
import db.DocumentDB;
import stormlite.OutputFieldsDeclarer;
import stormlite.TopologyContext;
import stormlite.routers.IStreamRouter;
import stormlite.tuple.Fields;
import stormlite.tuple.Tuple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import worker.CrawlWorker;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;

public class DocumentProcessorBolt implements IRichBolt {
    static Logger logger = LogManager.getLogger(DocumentFetcherBolt.class);
    String executorId = UUID.randomUUID().toString();
    private OutputCollector collector;
    private static final double NUM_LINKS = 20.0;

    @Override
    public void cleanup() {}

    @Override
    public void execute(Tuple input) {

        List<Object> values = input.getValues();
        String url = (String) values.get(0);
        logger.info("Received doc for parsing: " + url);
        String responseCode = (String) values.get(1);
        String contentType = (String) values.get(2);
        String document = (String) values.get(3);

        if (contentType == null || contentType.contains("html")) {
            LinkedList<String> urls = new LinkedList<String>();
            parseLinks(document, url, urls);
            CrawlWorker.sendURLs(urls, url);
        } else {
            CrawlWorker.sendURLs(new LinkedList<>(), url);
        }

        if (!responseCode.equals("304")) {
            DocumentDB.addDocumentAfterCheck(url, document);
        }
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
        declarer.declare(new Fields("Links"));
    }

    private static void parseLinks(String content, String url, LinkedList<String> urls) {
        Document doc = Jsoup.parse(content, url);
        Elements links = doc.select("a[href]");
        double numLinks = links.size() * 1.0;
        double threshHold = NUM_LINKS / numLinks;
        for (Element link : links) {
            String newlink = link.absUrl("href");

            if (!urls.contains(newlink) && Math.random() < threshHold){
                if (!ContentHashDB.contains(newlink)) {
                    ContentHashDB.addHash(newlink);
                    urls.add(newlink);
                }
            }
        }
    }

}
