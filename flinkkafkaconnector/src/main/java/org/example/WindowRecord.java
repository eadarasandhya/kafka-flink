package org.example;

import java.io.Serializable;

//Ignore this program
public class WindowRecord implements Serializable {
    public String windowStart;
    public String windowEnd;
    public long count;
    public String aggregatedValue; // or any aggregated payload

    // Required: no-arg constructor for Avro reflection
    public WindowRecord() {}

    public WindowRecord(String windowStart, String windowEnd, long count, String aggregatedValue) {
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.count = count;
        this.aggregatedValue = aggregatedValue;
    }

    @Override
    public String toString() {
        return "WindowRecord{" +
                "windowStart='" + windowStart + '\'' +
                ", windowEnd='" + windowEnd + '\'' +
                ", count=" + count +
                ", aggregatedValue='" + aggregatedValue + '\'' +
                '}';
    }
}