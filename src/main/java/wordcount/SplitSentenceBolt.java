package wordcount;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


public class SplitSentenceBolt extends BaseRichBolt {
	private static final long serialVersionUID = 2223204156371570768L;
	private OutputCollector collector;

	public SplitSentenceBolt() {
	}

	public void execute(Tuple input) {

		Integer tupleId = (Integer) input.getValueByField("tupleId");
		String[] splitWords = input.getValueByField("sentence").toString().split("\\s+");
		int numberWords = splitWords.length;

		long timeEmitted = (Long) input.getValueByField("timeEmitted");
		for (int index = 0; index < numberWords; index++) {
			String word = splitWords[index];
			this.collector.emit(new Values(tupleId, timeEmitted, word));
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tupleId", "timeEmitted", "word"));
	}

	public void prepare(Map<String, Object> heronConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;

	}
}
