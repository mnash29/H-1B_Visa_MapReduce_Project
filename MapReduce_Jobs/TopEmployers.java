import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.LinkedList;
import java.util.TreeMap;

//import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

public class TopEmployers {
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private String employer;


        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            employer = parseEmployer(value.toString().split(","));
            if(employer.length() > 1)
                context.write(new Text(employer), one);
        }

        /**
         * Some employers have commas in their name, so we cannot parse the csv to get employer simply by using
         * split(",") calls.
         * @param split - A String array of a csv line split by ","
         * @return A string of the employer
         */
        public String parseEmployer(String[] split){
            String employer = "";
            int index = 2; //starting index of employer

            /**
             * This while loop is to account for scenarios where split splits an employer name that has a comma in it.
             * This is necessary because we are using a csv file whose natural delimeter is a comma as well.
             */
            while(index < split.length){
                String temp = split[index];
                if(temp.length() < 1) //account for records where employer or unexpected behavior is occurring
                    return employer;
                employer += temp;

                if(temp.charAt(temp.length()-1) == '"')
                    break;
                employer += ",";
                index++;
            }
            //return employer's name without the quotes
            return employer.substring(1, employer.length()-1);
        }
    }

    public static class IntSumCombiner
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class TopTenReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private TreeMap<Integer, LinkedList<Text>> topTenEmployers = new TreeMap<>();
        private Text employer;

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int freq = 0;
            for (IntWritable val : values) {
                freq += val.get();
            }
            employer = new Text(key.toString());

            offerToTopTen(freq, employer, topTenEmployers);
        }

        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            for (int freq : topTenEmployers.descendingKeySet()) {
                LinkedList<Text> employerList = topTenEmployers.get(freq);
                for (Text employer : employerList) {
                    context.write(employer, new IntWritable(freq));
                }
            }
        }
    }

    public static void offerToTopTen(int freq, Text employer, TreeMap<Integer, LinkedList<Text>> topTenEmployers) {
        if (topTenEmployers.size() < 10 || freq == topTenEmployers.firstKey())
            addToTopTen(freq, employer, topTenEmployers);
        else if (freq > topTenEmployers.firstKey()) {
            topTenEmployers.pollFirstEntry();
            addToTopTen(freq, employer, topTenEmployers);
        }
    }

    public static void addToTopTen(int freq, Text employer, TreeMap<Integer, LinkedList<Text>> topTenEmployers) {
        LinkedList<Text> employerList = topTenEmployers.get(freq);
        if (employerList == null) {
            //topTenEmployers.size() < 10 at this point
            employerList = new LinkedList<Text>();
            employerList.add(employer);
            topTenEmployers.put(freq, employerList);
        } else
            employerList.add(employer);
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", " - ");
        Job job = Job.getInstance(conf, "Employers Submitting Most Applications");
        job.setJarByClass(TopEmployers.class);
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