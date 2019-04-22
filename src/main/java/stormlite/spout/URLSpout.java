package stormlite.spout;

import com.rabbitmq.client.DeliverCallback;
import db.DocumentDB;
import master.CrawlMaster;
import stormlite.OutputFieldsDeclarer;
import stormlite.TopologyContext;
import stormlite.bolt.DocumentFetcherBolt;
import stormlite.bolt.DocumentProcessorBolt;
import stormlite.routers.IStreamRouter;
import stormlite.tuple.Fields;
import stormlite.tuple.Tuple;
import stormlite.tuple.Values;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import worker.CrawlWorker;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;

import static spark.Spark.post;

public class URLSpout implements IRichSpout {

    private final Logger logger = LogManager.getLogger(URLSpout.class);
    private final DocumentFetcherBolt dfb = new DocumentFetcherBolt();

    private final String executorId = UUID.randomUUID().toString();
    SpoutOutputCollector collector;

//    DeliverCallback deliverCallback = (consumerTag, delivery) -> {
//        String message = new String(delivery.getBody(), "UTF-8");
//        dfb.execute(new Tuple(dfb.getSchema(), (new Values<Object>(message))));
//        System.out.println(" [x] Received '" + message + "'");
//        collector.emit(new Values<Object>(message));
//    };

    @Override
    public void open(Map<String, String> config, TopologyContext topo, SpoutOutputCollector collector) {
        this.collector = collector;
//        post("/newurl", (req, request) -> {
//            String url = req.queryParams("url");
//            collector.emit(new Values<Object>(url));
//            return "ok";
//        });
    }

    @Override
    public void close() {}


    @Override
    public void nextTuple() {
//        boolean autoAck = true; // acknowledgment is covered below
//        try {
//            CrawlWorker.channel.basicConsume(CrawlMaster.URL_Q, autoAck, deliverCallback, consumerTag -> { });
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        URL url = null;
        try {
            url = new URL(CrawlWorker.MASTER_IP + "/urlendpoint");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.getResponseCode();
            InputStream is = conn.getInputStream();
            String urlout = DocumentDB.convertStreamToString(is);
            if (urlout.length() > 0) {
                System.out.println("got url : " + urlout);
                dfb.execute(new Tuple(dfb.getSchema(), (new Values<Object>(urlout))));
            }
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (ProtocolException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("URL"));
    }


    @Override
    public String getExecutorId() {
        return executorId;
    }


    @Override
    public void setRouter(IStreamRouter router) {
        this.collector.setRouter(router);
    }

}
