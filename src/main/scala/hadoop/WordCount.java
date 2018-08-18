package hadoop;

/**
 * Created by PerkinsZhu on 2018/8/15 11:43
 **/

import hdfs.HdfsUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

public class WordCount {


    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);

        private List<String> keys =new ArrayList<>();

        public Map() {
            keys.add("易分期");
            keys.add("替你还");
            keys.add("抵押贷");
            keys.add("员工贷");
        }

        @Override
        public void map(LongWritable longWritable, Text text, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
            String line = text.toString();

                keys.forEach((String key)->{
                    if(line.contains(key)){
                        System.out.println(key);
                        Text word = new Text();
                        word.set(key);
                        try {
                            outputCollector.collect(word, one);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                });

               // word.set(next);
                //outputCollector.collect(word, one);
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text text, Iterator<IntWritable> iterator, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
            int sum = 0;
            while (iterator.hasNext()) {
                sum += iterator.next().get();
            }
            outputCollector.collect(text, new IntWritable(sum));
        }
    }
   public static class  MyPartitioner implements Partitioner<Text, IntWritable> {
       @Override
       public int getPartition(Text text, IntWritable intWritable, int i) {
           return text.getLength() % 2 ;
       }

       @Override
       public void configure(JobConf jobConf) {
       }
   }

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "jinzhao");
        HdfsUtils.deleteDir("/test/out");
        JobConf conf = new JobConf(WordCount.class);
        conf.setJobName("wordcount");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(Map.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        conf.setPartitionerClass(MyPartitioner.class);
        conf.setNumReduceTasks(2);
        FileInputFormat.setInputPaths(conf, new Path("hdfs://192.168.10.156:9000/test/input/log1.txt"));
        //        FileOutputFormat.setOutputPath(conf, new Path("hdfs://master:9000/thesis/output8"));
        FileOutputFormat.setOutputPath(conf, new Path("hdfs://192.168.10.156:9000/test/out"));
        //FileOutputFormat.setOutputPath(conf, new Path(args[0]));

        JobClient.runJob(conf);
    }

}