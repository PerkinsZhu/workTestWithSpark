package hadoop;

/**
 * Created by PerkinsZhu on 2018/8/15 11:43
 **/

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.StringTokenizer;

public class WordCount {
    private static String uri = "hdfs://192.168.10.156:9000";

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();


        @Override
        public void map(LongWritable longWritable, Text text, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
            String line = text.toString();
            System.out.println("line -->" + line);
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                String next = tokenizer.nextToken();
                System.out.println("next -->" + next);
                word.set(next);
                outputCollector.collect(word, one);
            }
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

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "jinzhao");
        deleteDir("/test/out");
        JobConf conf = new JobConf(WordCount.class);
        conf.setJobName("wordcount");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(Map.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path("hdfs://192.168.10.156:9000/test/input/test.txt"));
        //        FileOutputFormat.setOutputPath(conf, new Path("hdfs://master:9000/thesis/output8"));
        FileOutputFormat.setOutputPath(conf, new Path("hdfs://192.168.10.156:9000/test/out"));
        //FileOutputFormat.setOutputPath(conf, new Path(args[0]));

        JobClient.runJob(conf);
    }


    /**
     * delete a dir in the hdfs.
     * if dir not exists, it will throw FileNotFoundException
     *
     * @param dir the dir may like '/tmp/testdir'
     * @return boolean true-success, false-failed
     * @throws IOException something wrong happends when operating files
     */
    public static boolean deleteDir(String dir) throws IOException {
        if (StringUtils.isBlank(dir)) {
            return false;
        }
        dir = uri + dir;
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(dir), conf);
        fs.delete(new Path(dir), true);
        fs.close();
        return true;


    }
}