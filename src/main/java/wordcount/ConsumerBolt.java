package wordcount;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;


public class ConsumerBolt extends BaseRichBolt {
	private static final long serialVersionUID = -5470591933906954522L;
	private Map<String, Integer> countMap = null;

	private long tuplesCount = 0L;
	private String metricsFilename;
	boolean done = false;
	private int taskId;
	private String topologyName;
	private String componentId;


	public void execute(Tuple tuple) {


		String key = tuple.getValueByField("word").toString();
		if (this.countMap.get(key) == null) {
			this.countMap.put(key, 1);
		} else {
			Integer val = (Integer) this.countMap.get(key);
			this.countMap.put(key, val + 1);
		}
		long timeProcessed = Instant.now().toEpochMilli();
		long timeEmitted = (Long) tuple.getValueByField("timeEmitted");
		tuplesCount++;
		long latency = timeProcessed - timeEmitted;
		saveMetricsToFile(latency);

	}

	private void initializeMetricsFile() {
		metricsFilename = "ConsumerBolt-" + taskId + ".csv";
		File metricsFile = new File(System.getenv("HOME") + "/metrics/" + metricsFilename);
		if (!metricsFile.exists()) {
			PrintWriter pr = null;
			try {
				new File(System.getenv("HOME") + "/metrics").mkdirs();
				metricsFile.createNewFile();
				pr = new PrintWriter(new FileWriter(metricsFile, false));
				String header = "timestamp, topology, bolt, task, tuples, latency";
				pr.println(header);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				if (pr != null) {
					pr.close();
				}
			}
		}
	}

	private void saveMetricsToFile(long latency) {
		System.out.println("ConsumerBolt:" + taskId + " saving metrics to file");
		long timestamp = Instant.now().toEpochMilli();
		PrintWriter pr = null;
		try {
			File metricsFile = new File(System.getenv("HOME") + "/metrics/" + metricsFilename);

			pr = new PrintWriter(new FileWriter(metricsFile, true));
			String metrics = String.format("%d,%s,%s,%s,%d,%d", timestamp, topologyName, componentId, taskId,
					tuplesCount, latency);
			pr.println(metrics);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (pr != null) {
				pr.close();
			}
		}
	}

	public void prepare(Map<String, Object> heronConf, TopologyContext context, OutputCollector collector) {
		this.countMap = new HashMap<String, Integer>();
		taskId = context.getThisTaskId();
		topologyName = context.getStormId();
		componentId = context.getThisComponentId();
		taskId = context.getThisTaskId();
		initializeMetricsFile();
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}
}
