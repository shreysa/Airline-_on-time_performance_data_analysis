package org.neu.pdpmrA8.reducer;
/*
 @author shreysa
 */

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/**
 *
 * Read input line by line where each line is of following format.
 * key: Text<F, airlineId, year, month> or Text<A, AirportId, year, month>
 * value: FloatWritable<normalizedDelay>
 *
 * Transform above input into following format.
 * key: (airlineId, year, month) or (airportId, year, month)
 * value: type, totalMeanDelay,count
 * here type is 0 for airline and 1 for airport
 */

public class AirlineAirportReducer extends Reducer<Text, FloatWritable, Text, Text> {


    public static final String sep = ",";
    public static final String airportSubKey = "A";

    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Context context)
            throws IOException, InterruptedException {
        int count = 1;
        float delaySum = 0f;
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
