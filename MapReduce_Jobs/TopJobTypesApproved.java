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


public class TopJobTypesApproved {
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private String[] row;
        private String jobTitle;

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            row = value.toString().split(",");

            // Use "CER" instead of full "CERTIFIED" to speed up execution. It is only contained by CASE_STATUS fields
            // that are CERTIFIED or CERTIFIED-WITHDRAWN
            if (row[1].length() == 0 || !row[1].contains("CER"))
                return; // If application is not certified or has blank CASE_STATUS, return to discard it


            jobTitle = getJobTitle(row);
            if (jobTitle.length() > 1)
                context.write(new Text(jobTitle), one);
        }

        public String getJobTitle(String[] row) {
            int employerNameStartIndex = 2;
            int socNameStartIndex = getColumnStringEndIndex(employerNameStartIndex, row) + 1;
            if (socNameStartIndex == 0) // getColumnStringEndIndex returned - 1
                return "";

            int jobTitleStartIndex = getColumnStringEndIndex(socNameStartIndex, row) + 1;
            if (jobTitleStartIndex == 0) // getColumnStringEndIndex returned - 1
                return "";
            else {
                String JTStart = row[jobTitleStartIndex];
                if( JTStart.length() < 2 )
                    return "";
                // If job title is not split accross multiple elements, return it
                if (JTStart.charAt(JTStart.length() - 1) == '"' && JTStart.charAt(0) == '"')
                    return JTStart.substring(1, JTStart.length() - 1); // return Job Title without quotes
            }

            int jobTitleEndIndex = getColumnStringEndIndex(jobTitleStartIndex, row);
            if (jobTitleEndIndex == -1) // getColumnStringEndIndex returned -1
                return "";

            String jobTitle = "";
            for (int index = jobTitleStartIndex; index <= jobTitleEndIndex; index++) {
                jobTitle += row[index];
                if (index != jobTitleEndIndex)
                    jobTitle += ",";
            }
            return jobTitle.substring(1, jobTitle.length() - 1); // return Job Title without quotes
        }



        public int getColumnStringEndIndex(int startIndex, String[] row) {
            int index = startIndex;
            while (index < row.length) {
                String token = row[index];
                if (token.length() < 1 || token.charAt(token.length() - 1) == '"')
                    return index; // return index if column is empty or we've reached end of String for this column
                index++;
            }
            return -1; // Incorrectly formatted record, return -1
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

    public static class TopTwentyReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private TreeMap<Integer, LinkedList<Text>> topTwentyJobTitles = new TreeMap<>();
        private Text jobTitle;

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int freq = 0;
            for (IntWritable val : values) {
                freq += val.get();
            }
            jobTitle = new Text(key.toString());

            offerToTopTwenty(freq, jobTitle, topTwentyJobTitles);
        }

        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            for (int freq : topTwentyJobTitles.descendingKeySet()) {
                LinkedList<Text> jobTitleList = topTwentyJobTitles.get(freq);
                for (Text jobTitle : jobTitleList) {
                    context.write(jobTitle, new IntWritable(freq));
                }
            }
        }
    }

    public static void offerToTopTwenty(int freq, Text jobTitle, TreeMap<Integer, LinkedList<Text>> topTwentyJobTitles) {
        if (topTwentyJobTitles.size() < 20 || freq == topTwentyJobTitles.firstKey())
            addToTopTwenty(freq, jobTitle, topTwentyJobTitles);
        else if (freq > topTwentyJobTitles.firstKey()) {
            topTwentyJobTitles.pollFirstEntry();
            addToTopTwenty(freq, jobTitle, topTwentyJobTitles);
        }
    }

    public static void addToTopTwenty(int freq, Text jobTitle, TreeMap<Integer, LinkedList<Text>> topTwentyJobTitles) {
        LinkedList<Text> jobTitleList = topTwentyJobTitles.get(freq);
        if (jobTitleList == null) {
            //topTwentyJobTitles.size() < 20 at this point
            jobTitleList = new LinkedList<Text>();
            jobTitleList.add(jobTitle);
            topTwentyJobTitles.put(freq, jobTitleList);
        }
        else
            jobTitleList.add(jobTitle);
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", " : ");
        Job job = Job.getInstance(conf, "Most Commonly Certified Job Titles");
        job.setJarByClass(TopJobTypesApproved.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumCombiner.class);
        job.setReducerClass(TopTwentyReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}