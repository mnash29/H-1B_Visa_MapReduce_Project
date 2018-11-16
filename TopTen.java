import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;
import java.util.StringTokenizer;

public class TopTen {
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), " .,-!?");
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class IntSumCombiner
            extends Reducer<Text,IntWritable,Text,IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class TopTenReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private TreeMap<Integer, LinkedList<Text>> topTenFrequentWords = new TreeMap<>();
        private Text word;

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int freq = 0;
            for (IntWritable val : values) {
                freq += val.get();
            }
            word = new Text(key.toString());

            offerToTopTen(freq, word, topTenFrequentWords);
        }

        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            for (int freq : topTenFrequentWords.descendingKeySet()) {
                LinkedList<Text> wordList = topTenFrequentWords.get(freq);
                for(Text word : wordList){
                    context.write(word, new IntWritable(freq));
                }
            }
        }
    }

    public static void offerToTopTen(int freq, Text word, TreeMap<Integer, LinkedList<Text>> topTenFrequentWords){
        if(topTenFrequentWords.size() < 10 || freq == topTenFrequentWords.firstKey())
            addToTopTen(freq, word, topTenFrequentWords);
        else if(freq > topTenFrequentWords.firstKey()){
            topTenFrequentWords.pollFirstEntry();
            addToTopTen(freq, word, topTenFrequentWords);
        }
    }

    public static void addToTopTen(int freq, Text word, TreeMap<Integer, LinkedList<Text>> topTenFrequentWords){
        LinkedList<Text> wordList = topTenFrequentWords.get(freq);
        if(wordList == null){
            //topTenFrequentWords.size() < 10 at this point
            wordList = new LinkedList<Text>();
            wordList.add(word);
            topTenFrequentWords.put(freq, wordList);
        }
        else
            wordList.add(word);
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Top Ten Word Frequency");
        job.setJarByClass(TopTen.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumCombiner.class);
        job.setReducerClass(TopTenReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
//        job.setInputFormatClass(KeyValueTextInputFormat.class);
//        KeyValueTextInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}