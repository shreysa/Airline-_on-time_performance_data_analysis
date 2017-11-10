package org.neu.pdpmrA8.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.neu.pdpmrA8.mapper.AirlineAirportMapper;
import org.neu.pdpmrA8.reducer.AirlineAirportReducer;
import org.neu.pdpmrA8.util.Args;

import java.io.IOException;

/**
 * @author shreysa
 * Class to set initiate the and run the mapper and reducer and output the result
 */
public class JobExecutor extends Configured implements Tool {

    private boolean runDataCleaningJob(String jobName, Path inputPath, Path outputPath)
            throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = getConf();
        conf.set("mapreduce.output.textoutputformat.separator", ",");

        Job j = Job.getInstance(conf, jobName);
        j.setJarByClass(JobExecutor.class);
        j.setMapperClass(AirlineAirportMapper.class);
        j.setReducerClass(AirlineAirportReducer.class);
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(FloatWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(Text.class);
        j.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(j, inputPath);
        FileOutputFormat.setOutputPath(j, outputPath);

        return j.waitForCompletion(true);
    }

    @Override
    public int run(String[] args_) throws Exception {
        Args args = new Args(args_);
        String input = args.getOption("-input");
        String output = args.getOption("-output");

        String dataCleaningJobName = "DataCleaning";
        Path dataCleaningInputPath = new Path(input);
        Path dataCleaningOutputPath = new Path(output + dataCleaningJobName);
        runDataCleaningJob(dataCleaningJobName, dataCleaningInputPath, dataCleaningOutputPath);

        return 0;
    }
}
