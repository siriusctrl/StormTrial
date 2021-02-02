package wordcount;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.time.Instant;
import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class SentenceSpout extends BaseRichSpout {
	private static final long serialVersionUID = 2879005791639364028L;
	private SpoutOutputCollector collector;
	private BufferedReader br = null;
	private String fileName;
	private Integer tupleId = 0;
	boolean complete = false;
	private String topologyName;
	private String componentId;
	private int taskId;

	// Metrics
	private long emitted = 0;
	private long startTime;
	private long endTime;
	private Thread thread = null;

	public SentenceSpout(String fileName, int splitSentenceBoltParallelism) {
		this.fileName = fileName;
	}

	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("tupleId", "timeEmitted", "sentence"));
	}

	@Override
	public void close() {
		super.close();
		thread.interrupt();
	}

	public void open(Map map, TopologyContext context, SpoutOutputCollector spoutOutputCollector) {
		this.collector = spoutOutputCollector;
		topologyName = context.getStormId();
		componentId = context.getThisComponentId();
		taskId = context.getThisTaskId();
		InputStream in = null;
		try {
			in = loadByFileName(fileName);
			this.br = new BufferedReader(new InputStreamReader(in));
		} catch (FileNotFoundException e) {
			System.err.println("SentenceSpout:" + taskId + " file not found: " + e.getLocalizedMessage());

		}

		initializeMetricsFile();

		startTime = System.currentTimeMillis();

		thread = new Thread(new Runnable() {

			public void run() {
				while (!thread.isInterrupted()) {
					try {
						String st;
						if ((st = br.readLine()) != null) {
							tupleId++;
							emitted++;
							long timeEmitted = Instant.now().toEpochMilli();

							Values tuple = new Values(tupleId, timeEmitted, st.toLowerCase());
							collector.emit(tuple);

							saveMetricsToFile(st.toCharArray().length);
							try {
								Thread.sleep(0, 10);
							} catch (InterruptedException e) {
								e.printStackTrace();
							}

							long start = System.nanoTime(); // 500000
							while (start + 1000 >= System.nanoTime())
								;
						} else {
							if (!complete) {
								endTime = System.currentTimeMillis();
								System.out
										.println("SentenceSpout:" + taskId + " -----This is the end of the file!-----");
								saveMetricsToFile();
								complete = true;
							}

						}
					} catch (IOException e) {
						e.printStackTrace();
					}

				}
			}
		});

		thread.start();

	}

	public void nextTuple() {
		/*startTime = System.currentTimeMillis();

		String st;
		try {
			if ((st = br.readLine()) != null) {
				tupleId++;
				emitted++;
				long timeEmitted = Instant.now().toEpochMilli();// System.currentTimeMillis();

				Values tuple = new Values(tupleId, timeEmitted, st.toLowerCase());
				collector.emit(tuple);

				saveMetricsToFile(st.toCharArray().length);
				/*
				 * try { Thread.sleep(0, 10); } catch (InterruptedException e) { // TODO
				 * Auto-generated catch block e.printStackTrace(); }
				 */
				//long start = System.nanoTime();
				// 500000
				// while (start + 1000 >= System.nanoTime())
				;
			/*} else {
				if (!complete) {
					endTime = System.currentTimeMillis();
					System.out.println("SentenceSpout:" + taskId + " -----This is the end of the file!-----");
					saveMetricsToFile();
					complete = true;
				}

			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/

	}

	private void initializeMetricsFile() {
		File metricsFile = new File(System.getenv("HOME") + "/metrics/spoutOutputMetrics-" + taskId + ".csv");
		if (!metricsFile.exists()) {
			PrintWriter pr = null;
			try {
				new File(System.getenv("HOME") + "/metrics").mkdirs();
				metricsFile.createNewFile();
				pr = new PrintWriter(new FileWriter(metricsFile, false));
				String header = "timestamp, topology, bolt, task, bytes";
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

	private void saveMetricsToFile(long bytes) {
		long timestamp = Instant.now().toEpochMilli();
		PrintWriter pr = null;

		try {
			File metricsFile = new File(System.getenv("HOME") + "/metrics/spoutOutputMetrics-" + taskId + ".csv");

			pr = new PrintWriter(new FileWriter(metricsFile, true));
			String metrics = String.format("%d,%s,%s,%s,%d", timestamp, topologyName, componentId, taskId, bytes);
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

	private void saveMetricsToFile() {
		System.out.println("SentenceSpout:" + taskId + " saving metrics to file");
		PrintWriter pr = null;
		try {
			boolean printHeader = false;
			File metricsFile = new File(System.getenv("HOME") + "/metrics/spoutMetrics-" + taskId + ".csv");
			if (!metricsFile.exists()) {
				new File(System.getenv("HOME") + "/metrics").mkdirs();
				metricsFile.createNewFile();
				printHeader = true;
			}

			pr = new PrintWriter(new FileWriter(metricsFile, true));
			long duration = endTime - startTime;
			String metrics = String.format("%s,%s,%s,%d,%d,%d,%d", topologyName, componentId, taskId, emitted,
					startTime, endTime, duration);

			if (printHeader) {
				String header = "topology, spout, task, emitted, start time, end time, duration";
				pr.println(header);
			}
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

	/**
	 * @param name
	 * @return
	 * @throws FileNotFoundException
	 */
	private InputStream loadByFileName(String name) throws FileNotFoundException {
		File f = new File(name);
		if (f.isFile()) {
			return new FileInputStream(f);
		} else {
			return getClass().getClassLoader().getResourceAsStream(name);
		}
	}
}
