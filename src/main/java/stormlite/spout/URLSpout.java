package stormlite.spout;

import stormlite.OutputFieldsDeclarer;
import stormlite.TopologyContext;
import stormlite.routers.IStreamRouter;
import stormlite.tuple.Fields;
import stormlite.tuple.Values;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;

import static spark.Spark.post;

public class URLSpout implements IRichSpout {

    private final Logger logger = LogManager.getLogger(URLSpout.class);

    private final String executorId = UUID.randomUUID().toString();
    SpoutOutputCollector collector;

    @Override
    public void open(Map<String, String> config, TopologyContext topo, SpoutOutputCollector collector) {
        this.collector = collector;
        post("/newurl", (req, request) -> {
            String url = req.queryParams("url");
            collector.emit(new Values<Object>(url));
            return "ok";
        });
    }

    @Override
    public void close() {}

    @Override
    public void nextTuple() {}

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
