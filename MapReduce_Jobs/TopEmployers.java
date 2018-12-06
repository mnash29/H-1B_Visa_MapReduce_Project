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

public class TopEmployers {
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private String employer;

        /**
         * Writes a <Text, IntWritable> key/value pair to the Context.
         * The Text object will contain the Employer's name, and the IntWritable will be wrapping an int value of 1
         * which will be used for a frequency count in the combiner.
         *
         * @param key     (Object) - Input key.  Not used in this function.
         * @param value   (Text) - value is the Text object wrapping a row in the dataset csv
         * @param context (Context) - The Context that we will be writing map output <key, value> pairs to
         */
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            employer = getEmployer(value.toString().split(","));
            if (employer.length() > 1) // Filter out employer entries left blank
                context.write(new Text(employer), one);
        }

        /**
         * This helper function is necessary because of the difficulties of parsing a csv file. Some of the employer
         * names have commas in them, meaning that using split(",") on a row from the csv (comma separated value file)
         * would not result in a clean separation of the columns.  If the employer's name contains a comma in it, then
         * it will be split across multiple elements of row[].
         *
         * @param row - A String array of a csv line split by ","
         * @return A string of the employer
         */
        public String getEmployer(String[] row) {
            String employer = "";
            int index = 2; //starting index of employer

            // Build the string of the employer's name
            while (index < row.length) {
                String temp = row[index];
                if (temp.length() < 1) //account for records where employer or unexpected behavior is occurring
                    return employer;
                employer += temp;

                if (temp.charAt(temp.length() - 1) == '"') // reached end of employer's name
                    break;
                employer += ",";
                index++;
            }
            //return employer's name without the quotes, commas included if they exist
            return employer.substring(1, employer.length() - 1);
        }
    }

    /**
     * This Combiner extends Reducer and reduces by frequency count on each employer name written to the context by the
     * mapper it is assigned to.
     * It writes <Text, IntWritable> key/value pairs to the context which will be the input for the reducer
     */
    public static class IntSumCombiner
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        /**
         * @param key     (Text) - Text wrapper for the Employer Name
         * @param values  (Iterable<IntWritable>) - An Iterable of IntWritables each storing int values of 1.
         * @param context (Context) - The context to write the key/value pairs to
         */
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            // Sum the frequencies for this key (Employer Name)
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class TopTenReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        /**
         * topTenEmployers will be a TreeMap with keys = int frequency counts, and values = LinkedList of Employer Names
         * (Text Objects) that have the corresponding frequency count specified by their key. The TreeMap is ranked by
         * frequency count in descending order.
         * The values must be LinkedLists if you want to include Employer Names that have the same frequency counts (are
         * tied for their place in the top ten).  While this may only only occur on rare occasion, it did not slow
         * down execution times much.
         */
        private TreeMap<Integer, LinkedList<Text>> topTenEmployers = new TreeMap<>();
        private Text employer;

        /**
         * Reduce the combiner frequency outputs for a single employer into its total frequency count, and offer that
         * employer's name and its frequency to the Top Ten helper function that will determine where, if anywhere it
         * belongs in the Top Ten.
         *
         * @param key     (Text) - Text object wrapping the employer's name String
         * @param values  (Iterable<IntWritable>) - The Iterable containing the frequency counts (as IntWritables) for
         *                the corresponding employer output by the combiners.
         * @param context Unused, but required as a parameter.
         */
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int freq = 0;
            // Sum the frequencies for this key (Employer Name)
            for (IntWritable val : values) {
                freq += val.get();
            }
            employer = new Text(key.toString());

            // Offer this employer and its corresponding total frequency to the Top Twenty
            offerToTopTen(freq, employer);
        }

        /**
         * This function is called after the last call to reduce() is made.
         * At this point we will write the ranking stored in topTenEmployers to the Context for output
         *
         * @param context (Context) - The context to write the output to
         */
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            for (int freq : topTenEmployers.descendingKeySet()) {
                LinkedList<Text> employerList = topTenEmployers.get(freq);
                for (Text employer : employerList) {
                    context.write(employer, new IntWritable(freq));
                }
            }
        }

        /**
         * This is a helper function whose only job is to determine if an employer belongs in the Top Ten.
         * If it does, then it will pass that employer and its frequency to addToTopTen which will insert them into
         * their proper place in topTenEmployers.
         *
         * @param freq     (int) - Total frequency count applications submitted by employer
         * @param employer (Text) - Text object wrapping employer's name String
         */
        public void offerToTopTen(int freq, Text employer) {
            if (topTenEmployers.size() < 10 || freq == topTenEmployers.firstKey())
                addToTopTen(freq, employer);
            else if (freq > topTenEmployers.firstKey()) {
                topTenEmployers.pollFirstEntry(); // IMPORTANT: Removes the rank 10 element to make room for new addition
                addToTopTen(freq, employer);
            }
        }

        /**
         * This function inserts a Job Title into its corresponding LinkedList in topTwentyJobTitles, or inserts it as the
         * first element of a new LinkedList with a certain frequency count and inserts that LinkedList into the
         * topTwentyJobTitles. (See documentation above the topTwentyJobTitles declaration)
         * topTwentyJobTitles will always contain 19 elements or less before this function is called, leaving room for
         * whatever <freq, jobTitle> pair is to be inserted.
         *
         * @param freq     (int) - number of application submitted by employer
         * @param employer (Text) - Text object of the employer's name String
         */
        public void addToTopTen(int freq, Text employer) {
            LinkedList<Text> employerList = topTenEmployers.get(freq);
            if (employerList == null) {
                //topTenEmployers.size() < 10 at this point
                employerList = new LinkedList<Text>(); // Add employer as the first element of this list
                employerList.add(employer);
                topTenEmployers.put(freq, employerList);
            } else // There is a tie, there is already a list of employer names for this frequency.
                employerList.add(employer);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", " - ");
        Job job = Job.getInstance(conf, "Employers Submitting Most Applications");
        job.setJarByClass(TopEmployers.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumCombiner.class);
        job.setReducerClass(TopTenReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}