package com.cts.generator;

import com.cts.techyon.api.measurements.MeasurementsVolumesMessages;
import com.cts.utils.ProfileIndexArgs;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class MeasurementVolumeJsonGeneratorForForbruk {

    private final List<String> allJsonFiles = new ArrayList<>();

    public MeasurementVolumeJsonGeneratorForForbruk(List<ProfileIndexArgs> jsonPayloads, int resolution, int maxTransactionsPerFile) throws JsonProcessingException {
        DateTimeFormatter jsonOutput = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssZZ");
        MeasurementsVolumesMessages msg = new MeasurementsVolumesMessages();
        ObjectMapper mpr = new ObjectMapper();
        msg.measurementMessages = new ArrayList<>();
        MeasurementsVolumesMessages.MeasurementVolumeMessage m = new MeasurementsVolumesMessages.MeasurementVolumeMessage();
        int transactionCounter =  0;
        String previousDevice = "@";
        StringBuilder oneJsonFile = new StringBuilder();
        for (ProfileIndexArgs thisPayLoad : jsonPayloads) {
            if (transactionCounter >= maxTransactionsPerFile && !previousDevice.equalsIgnoreCase(thisPayLoad.deviceId)){
                previousDevice = thisPayLoad.deviceId;
                if (!m.volumes.isEmpty()) {
                    msg.measurementMessages.add(m);
                    oneJsonFile.append(mpr.writeValueAsString(msg));
                    allJsonFiles.add(oneJsonFile.toString());
                }
                msg =  new MeasurementsVolumesMessages();
                msg.measurementMessages = new ArrayList<>();
                m = new MeasurementsVolumesMessages.MeasurementVolumeMessage();
                m.deviceId = thisPayLoad.deviceId;
                m.resolution = resolution;
                m.sensorType = thisPayLoad.sensorType;
                m.direction = thisPayLoad.direction;
                m.unit = thisPayLoad.unit;
                m.volumes = new ArrayList<>();
                oneJsonFile = new StringBuilder();
                transactionCounter = 0;
            }
            transactionCounter ++;
            if (!previousDevice.equalsIgnoreCase(thisPayLoad.deviceId)){
                previousDevice = thisPayLoad.deviceId;
                if (!m.volumes.isEmpty()) {
                    msg.measurementMessages.add(m);
                }
                m = new MeasurementsVolumesMessages.MeasurementVolumeMessage();
                m.deviceId = thisPayLoad.deviceId;
                m.resolution = resolution;
                m.sensorType = thisPayLoad.sensorType;
                m.direction = thisPayLoad.direction;
                m.unit = thisPayLoad.unit;
                m.volumes = new ArrayList<>();
            }
            MeasurementsVolumesMessages.Volume vol = new MeasurementsVolumesMessages.Volume();
            vol.from = thisPayLoad.fromTime;
            vol.to = thisPayLoad.toTime;
            vol.dataPoint = new MeasurementsVolumesMessages.Volume.DataPoint();
            vol.dataPoint.origin = thisPayLoad.origin;
            vol.dataPoint.createdTime = jsonOutput.format(ZonedDateTime.now().withZoneSameInstant(ZoneId.of("Europe/Oslo")));
            vol.dataPoint.value = thisPayLoad.indexValue;
            m.volumes.add(vol);
        }
        if (!m.volumes.isEmpty()) {
            msg.measurementMessages.add(m);
        }
        oneJsonFile.append(mpr.writeValueAsString(msg));
        allJsonFiles.add(oneJsonFile.toString());
    }

    public List<String> getAllJsonFiles() {
        return allJsonFiles;
    }
}
