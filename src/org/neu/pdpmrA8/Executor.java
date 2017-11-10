package org.neu.pdpmrA8;
/*
* @author shreysa
*/

import org.apache.hadoop.util.ToolRunner;
import org.neu.pdpmrA8.job.JobExecutor;

/**
 * Class to run entire airline-airport delay data analysis program
 */
public class Executor {
    public static void main(String[] args) throws Exception {
        JobExecutor je = new JobExecutor();
        int exitCode = ToolRunner.run(je, args);
        System.exit(exitCode);
    }
}
