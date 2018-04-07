/**
 * Created by lchen on 11/24/17.
 * @author: Linlin Chen
 * lchen96@hawk.iit.edu
 *
 * Reference:
 * https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
 * https://hadoop.apache.org/docs/r2.7.1/api/org/apache/hadoop/examples/terasort/package-summary.html#package_description
 *
 *
 * This class is built for implementing terasort using hadoop
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


import java.io.IOException;


/**
 * Class used for hadoop sorting
 * All the configurations and running as implemented in run threads
 */
public class Hadoop_Terasort extends Configured implements Tool{
    //log file
    //private static final Log log = LogFactory.getLog(Hadoop_Terasort.class);

    private static final int TEXTLENGTH = 10;       //first 10 characters used as the contents

    public int run(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("Hadoop_Terasort [hdfs input path] [hdfs output path]");
            return 1;
        }
        //log.info("starting");

        //configuration
        Configuration conf = new Configuration();

        long starttime = 0;
        long endtime = 0;
        try {

            //create new a sort job
            Job job = Job.getInstance(conf, "Tera Sort");

            //set mapper and reducer
            job.setJarByClass(Hadoop_Terasort.class);
            job.setMapperClass(SortMapper.class);
            job.setCombinerClass(SortReducer.class);
            job.setReducerClass(SortReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);


            starttime = System.currentTimeMillis();
            FileInputFormat.addInputPath(job, new Path(args[0]));
            endtime = System.currentTimeMillis();
            //System.out.println("Adding file time: " + timeinsecond(endtime - starttime) + " s");

            starttime = System.currentTimeMillis();
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            endtime = System.currentTimeMillis();
            //System.out.println("Writing file time: " + timeinsecond(endtime - starttime) + " s");

            starttime = System.currentTimeMillis();
            System.exit(job.waitForCompletion(true) ? 0 : 1);
            endtime = System.currentTimeMillis();
            System.out.println("MapReduce cost: " + timeinsecond(endtime - starttime) + " s");



        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        //log.info("done");
        return 0;
    }

    private static double timeinsecond (double ms) {
        return (ms/1000);
    }

    /**
     * This is the mapper class
     * Refer to the word count example
     * Since mapper will automatically sort at the end before sending to reducer,
     * there is no need to automatiacally implement sorting algorithms
     * For accuracy, since only first 10 characters will be used for sorting, so split one line into <key, value> pair
     */
    public static class SortMapper extends Mapper<Text, Text, Text, Text> {

        public void map (Object key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            Text linekey = new Text();
            Text linevalue = new Text();

            linekey.set(line.substring(0, TEXTLENGTH));     // key used for sorting
            linevalue.set(line.substring(TEXTLENGTH));
            context.write(linekey, linevalue);

        }
    }

    /**
     * This is the reducer class
     * Based on the <key, value> pair returned by mapper, merge them into one line string and write back to disk
     */

    public static class SortReducer extends Reducer<Text, Text, Text, Text> {

        public void reduc (Text key, Text value, Context context) throws IOException, InterruptedException{
            Text linekey = new Text();
            Text linevalue = new Text();

            linekey.set(key.toString() + value.toString() );
            linevalue.set("");
            context.write(linekey, linevalue);
        }
    }


    /**
     * Main function
     * @param args
     * @throws Exception
     */
    public static void main (String[] args) throws Exception {
        int returnCode = ToolRunner.run(new JobConf(), new Hadoop_Terasort(), args);
        System.exit(returnCode);
    }

}

