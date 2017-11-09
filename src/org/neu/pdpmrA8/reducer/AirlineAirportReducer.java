package org.neu.pdpmrA8.reducer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/**
 * @author shreysa
 */
public class AirlineAirportReducer extends Reducer<Text, FloatWritable, Text, Text> {
    /**
     * Read input by line where each line is of following format.
     * key: Text<F, airlineId, year> or Text<A, AirportId, year>
     * value: Text<month, normalizedDelay>
     *
     * Transform above input into following format.
     * key: (airlineId, year) or (airportId, year)
     * value: type, totalMeanDelay, meanDelayJan, meanDelayFeb...meanDelayDec
     * here type is 0 for airline and 1 for airport
     */

    public static final String sep = ",";
    public static final String airportSubKey = "A";

    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Context context)
            throws IOException, InterruptedException {
        int count = 1;
        float delaySum = 0f;
        float[] monthDelaySum = new float[13];
        int[] monthCount = new int[13];
        Text resultKey = new Text();
        Text resultValue = new Text();

        // Determine record type. 0 = Flight, 1 = airport
        String type = "0";
        String[] keyElems = key.toString().split(sep);
        if (keyElems[0].equals(airportSubKey)) { type = "1"; }
        StringBuilder recordKey = new StringBuilder();
        recordKey.append(keyElems[1]).append(sep).append(keyElems[2]).append(sep).append(keyElems[3]);

        for (FloatWritable entry : values) {
            delaySum += entry.get();
            count++;
        }

        // meanDelay,count
        StringBuilder outValue = new StringBuilder();
        outValue.append(type).append(sep).append(delaySum / count).append(sep).append(count);
        resultKey.set(recordKey.toString());
        resultValue.set(outValue.toString());
        context.write(resultKey, resultValue);
    }
}
