package com.cts.generator;

import com.cts.techyon.api.measurements.MeasurementMessages;
import com.cts.utils.ProfileIndexArgs;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MeasurementJsonGeneratorForMBus {
    private String transactionType;
    private List<String> allJsonFiles = new ArrayList<>();

    public MeasurementJsonGeneratorForMBus(String transactionType, List<ProfileIndexArgs> jsonPayloads,int resolution, int maxTransactionsPerFile) throws JsonProcessingException {
        this.transactionType = transactionType;
        DateTimeFormatter jsonOutput = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssZZ");
        MeasurementMessages msg = new MeasurementMessages();
        ObjectMapper mpr = new ObjectMapper();
        msg.measurementMessages = new ArrayList<>();
        MeasurementMessages.MeasurementMessage m = new MeasurementMessages.MeasurementMessage();
        int transactionCounter =  0;
        String previousDevice = "@";
        StringBuilder oneJsonFile = new StringBuilder();
        for (ProfileIndexArgs thisPayLoad : jsonPayloads) {
            if (transactionCounter >= maxTransactionsPerFile && !previousDevice.equalsIgnoreCase(thisPayLoad.deviceId)){
                previousDevice = thisPayLoad.deviceId;
                if (m.indexes.size() > 0) {
                    msg.measurementMessages.add(m);
                    oneJsonFile.append(mpr.writeValueAsString(msg));
                    allJsonFiles.add(oneJsonFile.toString());
                }
                msg = new MeasurementMessages();
                msg.measurementMessages = new ArrayList<>();
                m = new MeasurementMessages.MeasurementMessage();
                m.deviceId = thisPayLoad.deviceId;
                m.resolution = resolution;
                m.sensorType = thisPayLoad.sensorType;
                m.direction = thisPayLoad.direction;
                m.unit = thisPayLoad.unit;
                m.indexes = new ArrayList<>();
                oneJsonFile = new StringBuilder();
                transactionCounter = 0;
            }
            transactionCounter ++;
            if (!previousDevice.equalsIgnoreCase(thisPayLoad.deviceId)){
                previousDevice = thisPayLoad.deviceId;
                if (m.indexes.size() > 0) {
                    msg.measurementMessages.add(m);
                }
                m = new MeasurementMessages.MeasurementMessage();
                m.deviceId = thisPayLoad.deviceId;
                m.resolution = resolution;
                m.sensorType = thisPayLoad.sensorType;
                m.direction = thisPayLoad.direction;
                m.unit = thisPayLoad.unit;
                m.indexes = new ArrayList<>();
            }
            MeasurementMessages.Index idx = new MeasurementMessages.Index();
            if (transactionType.equalsIgnoreCase("hourlyStandAtOffset1")){
                idx.readTime = thisPayLoad.readTimeForHourlyTransaction;
            }
            else { //Day stand using real-time timeZone (gmt+1/winter and gmt+2/DST - summer).
                idx.readTime = thisPayLoad.readTime;
            }
            idx.dataPoint = new MeasurementMessages.Index.DataPoint();
            idx.dataPoint.tags = new ArrayList<>();
            /**
             * For NullStilling of Step meters - add this tag:
             *             Map oneTag = new HashMap();
             *             oneTag.put("key", "endReading");
             *             oneTag.put("value", "true");
             *             idx.dataPoint.tags.add(oneTag);
             */
            Map oneTag = new HashMap();
            if (thisPayLoad.originType.equalsIgnoreCase("measured_1")) {
                oneTag.put("key", "readBy");
                oneTag.put("value", "ReadByEviny");
                idx.dataPoint.tags.add(oneTag);
            }
            else if (thisPayLoad.originType.equalsIgnoreCase("measured_11")){
                oneTag.put("key", "readBy");
                oneTag.put("value", "ReadByExternalParty");
                idx.dataPoint.tags.add(oneTag);
            }
            else if (thisPayLoad.originType.equalsIgnoreCase("measured_50")){
                oneTag.put("key", "readBy");
                oneTag.put("value", "ReadByMeter");
                idx.dataPoint.tags.add(oneTag);
            }
            else if (thisPayLoad.originType.equalsIgnoreCase("stipulation")){
                oneTag.put("key", "estimationType");
                oneTag.put("value", "Stipulated");
                idx.dataPoint.tags.add(oneTag);
                oneTag = new HashMap();
                oneTag.put("key", "comment");
                oneTag.put("value", "QualityV1=Estimated");
                idx.dataPoint.tags.add(oneTag);
            }
            else if (thisPayLoad.originType.equalsIgnoreCase("estimated")){
                oneTag.put("key", "estimationType");
                oneTag.put("value", "Interpolated");
                idx.dataPoint.tags.add(oneTag);
                oneTag = new HashMap();
                oneTag.put("key", "comment");
                oneTag.put("value", "QualityV1=Estimated");
                idx.dataPoint.tags.add(oneTag);
            }
            else if (thisPayLoad.originType.equalsIgnoreCase("estimated_4")){
                oneTag.put("key", "estimationType");
                oneTag.put("value", "Manual");
                idx.dataPoint.tags.add(oneTag);
                oneTag = new HashMap();
                oneTag.put("key", "comment");
                oneTag.put("value", "QualityV1=Estimated");
                idx.dataPoint.tags.add(oneTag);
            }
            else {
                oneTag.put("key", "readBy");
                oneTag.put("value", "ReadByEviny");
                idx.dataPoint.tags.add(oneTag);
            }
            idx.dataPoint.origin = thisPayLoad.origin;
            m.indexes.add(idx);
            idx.dataPoint.createdTime = jsonOutput.print(new DateTime());
            idx.dataPoint.value = thisPayLoad.indexValue;
        }
        if (m.indexes.size() > 0) {
            msg.measurementMessages.add(m);
        }
        oneJsonFile.append(mpr.writeValueAsString(msg));
        allJsonFiles.add(oneJsonFile.toString());
    }

    public List<String> getAllJsonFiles() {
        return allJsonFiles;
    }
}
