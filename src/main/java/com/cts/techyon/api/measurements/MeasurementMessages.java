package com.cts.techyon.api.measurements;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MeasurementMessages {

    public List<MeasurementMessage> measurementMessages = new ArrayList<>();

    public static class MeasurementMessage {
        public String deviceId;
        public String sensorType;
        public int resolution;
        public String unit;
        public String direction;
        public List<Index> indexes = new ArrayList<>();
    }

    public static class Index {
        public String readTime;
        public DataPoint dataPoint;


        public static class DataPoint {
            public String origin;
            public BigDecimal value;
            public String createdTime;
            public List<Map<String,String>> tags;
        }
    }
}
