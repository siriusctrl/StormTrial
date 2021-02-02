package wordcount;

import org.apache.storm.Config;
import org.apache.storm.topology.ConfigurableTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;


public class WordCountTopology extends ConfigurableTopology {
	
	public static void main(String[] args) throws Exception {
        ConfigurableTopology.start(new WordCountTopology(), args);
    }

	protected int run(String[] args) throws Exception {
        final int sentenceSpoutParallelism = 1;
        final int splitSentenceParallelism = 10;
        final int consumerBoltParallelism = 10;
        
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("sentence", new SentenceSpout(System.getenv("HOME")+"/sentences.txt",splitSentenceParallelism),
        		sentenceSpoutParallelism).setNumTasks(sentenceSpoutParallelism);
        builder.setBolt("split", new SplitSentenceBolt(), 
        		splitSentenceParallelism)
               .shuffleGrouping("sentence").setNumTasks(splitSentenceParallelism);
        builder.setBolt("consumer", new ConsumerBolt(), consumerBoltParallelism)
               .fieldsGrouping("split", new Fields("word")).setNumTasks(consumerBoltParallelism);
        
        // configuration
        Config conf = new Config();
        conf.setNumWorkers(1);
        //disable reliability
        conf.setNumAckers(0);
        
        return submit(args[0], conf, builder);
        
    }
}
