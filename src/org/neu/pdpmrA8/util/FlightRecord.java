package org.neu.pdpmrA8.util;

/**
 * @author Shreysa
 */
public class FlightRecord {
    private static final int fieldCount = 110;
    private int year;
    private int month;
    private int airlineId;
    private int destAirportId;
    private int originAirportId;
    private int originAirportSeqId;
    private int destAirportSeqId;
    private int originCityMarketId;
    private int destCityMarketId;
    private int originStateFips;
    private int destStateFips;
    private int originWac;
    private int crsArrTime;
    private int crsDepTime;
    private int destWac;
    private int arrTime;
    private int depTime;
    private int actualElapsedTime;
    private int arrDelay;
    private int crsElapsedTime;
    private int arrDelayNew;
    private String origin;
    private String dest;
    private String originCityName;
    private String destCityName;
    private String originStateAbr;
    private String destStateAbr;
    private String originStateNm;
    private String destStateNm;
    private float normalizedDelay;
    private boolean cancelled;
    private boolean arrDel15;
    private boolean isParsed = false;

    public FlightRecord(CSVRecord r) {
        try {
            parseCSVRecord(r);
        } catch (NumberFormatException e) {
            // Exception occurs when expected column of integer is not of type integer
            // isParsed is already false so record will be skipped.
        }
    }

    private void parseCSVRecord(CSVRecord r) throws NumberFormatException {
        if (r.getFieldCount() != fieldCount) return;
        else{
            getColVal(r);
            if (crsArrTime == 0 || (crsDepTime == 0)) return;
            int timeZone = crsArrTime - crsDepTime - crsElapsedTime;
            if ((timeZone % 60) != 0) return;
            if ((originAirportId <= 0) || (destAirportId <= 0) || (originAirportSeqId <= 0) ||
                    (destAirportSeqId <= 0) || (originCityMarketId <= 0) || (destCityMarketId <= 0) ||
                    (originStateFips <= 0) || (destStateFips <= 0) || (originWac <= 0) || (destWac <= 0))
                return;
            if (isNotValid(origin) || isNotValid(dest) || isNotValid(originCityName) ||
                    isNotValid(destCityName) || isNotValid(originStateAbr) || isNotValid(destStateAbr) ||
                    isNotValid(originStateNm) || isNotValid(destStateNm))
                return;
            if (!cancelled) {
                if ((arrTime - depTime - actualElapsedTime - timeZone) != 0)
                    return;
                if (arrDelay > 0) {
                    if (arrDelay != arrDelayNew)
                        return;
                } else if (arrDelayNew != 0)
                    return;
                if (arrDelay >= 15) {
                    if (!arrDel15)
                        return;
                }
            }
            setNormalizedDelay();
            isParsed = true;
        }
    }

    private void getColVal(CSVRecord r) throws NumberFormatException {
        year = Integer.parseInt(r.get(0));
        month = Integer.parseInt(r.get(2));
        airlineId = Integer.parseInt(r.get(7));
        originAirportId = Integer.parseInt(r.get(11));
        originAirportSeqId = Integer.parseInt(r.get(12));
        originCityMarketId = Integer.parseInt(r.get(13));
        origin = r.get(14);
        originCityName = r.get(15);
        originStateAbr = r.get(16);
        originStateFips = Integer.parseInt(r.get(17));
        originStateNm = r.get(18);
        originWac = Integer.parseInt(r.get(19));
        destAirportId = Integer.parseInt(r.get(20));
        destAirportSeqId = Integer.parseInt(r.get(21));
        destCityMarketId = Integer.parseInt(r.get(22));
        dest = r.get(23);
        destCityName = r.get(24);
        destStateAbr = r.get(25);
        destStateFips = Integer.parseInt(r.get(26));
        destStateNm = r.get(27);
        destWac = Integer.parseInt(r.get(28));
        crsDepTime = getTimeInMinutes(r.get(29));
        depTime = getTimeInMinutes(r.get(30));
        crsArrTime = getTimeInMinutes(r.get(40));
        arrTime = getTimeInMinutes(r.get(41));
        arrDelay = (int) Float.parseFloat(r.get(42));
        arrDelayNew = (int) Float.parseFloat(r.get(43));
        arrDel15 = parseBoolean("" + (int) Float.parseFloat(r.get(44)));
        cancelled = parseBoolean(r.get(47));
        crsElapsedTime = (int) Float.parseFloat(r.get(50));
        actualElapsedTime = (int) Float.parseFloat(r.get(51));
 }

    private boolean isNotValid(String s) {
        return   s == null || s.isEmpty() || !s.matches("[a-zA-Z ,]+");
    }

    private int getTimeInMinutes(String time){
        int actTime = Integer.parseInt(time);
        return  (((int)(actTime/ 100)) * 60) + (actTime % 100);
    }
    private void setNormalizedDelay() {
        if (cancelled) normalizedDelay = 4f;
        else normalizedDelay = ((float) arrDelayNew) / crsElapsedTime;
    }

    private boolean parseBoolean(String boolS) throws NumberFormatException {
        int boolI = Integer.parseInt(boolS);
        if (boolI == 0) return false;
        else if (boolI == 1) return true;
        else throw new NumberFormatException();
    }

    public boolean isParsed() {
        return isParsed;
    }

    public Integer getAirlineId() {
        return airlineId;
    }
    public Integer getMonth() {
        return month;
    }
    public Integer getYear() {
        return year;
    }
    public Float getNormalizedDelay() { return normalizedDelay; }
    public Integer getDestAirportId() { return destAirportId; }
}

