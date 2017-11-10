package org.neu.pdpmrA8.mapper;
/**
 * @author Shreysa
 */
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.neu.pdpmrA8.util.CSVRecord;
import org.neu.pdpmrA8.util.FlightRecord;

import java.io.IOException;

/**
 * Given a single record from flight data csv, parse the record to extract following data -
 * key: F, AirlineId, year, month
 * value: normalizedDelay
 * or
 * key: A, AirportId, year, month
 * value:  normalizedDelay
 */
public class AirlineAirportMapper extends Mapper<Object, Text, Text, FloatWritable> {
    String sep = ",";
    String flightSubKey = "F" + sep;
    String airportSubkey = "A" + sep;
    Text fkey = new Text();
    FloatWritable fvalue = new FloatWritable();
    Text akey = new Text();
    FloatWritable avalue = new FloatWritable();

    @Override
    protected void map(Object ignore, Text value, Context context) throws IOException, InterruptedException {
        FlightRecord fr = new FlightRecord(new CSVRecord(value.toString()));
        if (fr.isParsed()) {
            fkey.set(flightSubKey +  fr.getAirlineId().toString()
                    + sep + fr.getYear().toString() + sep + fr.getMonth().toString());
            fvalue.set(fr.getNormalizedDelay());
            akey.set(airportSubkey + fr.getDestAirportId().toString()
                    + sep + fr.getYear().toString() + sep + fr.getMonth().toString());
            avalue.set(fr.getNormalizedDelay());
            context.write(fkey, fvalue);
            context.write(akey, avalue);
        }
    }
}
