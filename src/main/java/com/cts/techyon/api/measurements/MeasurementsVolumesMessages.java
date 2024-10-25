package com.cts.techyon.api.measurements;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

public class MeasurementsVolumesMessages {

    public List<MeasurementVolumeMessage> measurementMessages = new ArrayList<>();

    public static class MeasurementVolumeMessage {
        public String deviceId;
        public String sensorType;
        public int resolution;
        public String unit;
        public String direction;
        public List<MeasurementsVolumesMessages.Volume> volumes = new ArrayList<>();
    }

    public static class Volume {
        public String from;
        public String to;
        public MeasurementsVolumesMessages.Volume.DataPoint dataPoint;


        public static class DataPoint {
            public String origin;
            public BigDecimal value;
            public String createdTime;
        }
    }
}
