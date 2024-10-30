package com.cts.generator;

import com.cts.utils.ProfileIndexArgs;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class CreateJSONForProfileIndexD0 {

    private List<String> listOfJSONFiles;
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final DateTimeFormatter input = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssxxx");
    private final Map<String,String>formatNameToFormatReference;

    public CreateJSONForProfileIndexD0(Map<String,String>formatNameToFormatReference,String currentFileFormat,Map<String,String> formatNameToDateFormat,List<String[]> payloadDetailsForIndex, JdbcTemplate jdbcTemplate, int maxTransactionsPerFile,boolean publishHourlyStand) throws JsonProcessingException {
        this.formatNameToFormatReference = formatNameToFormatReference;
        List<ProfileIndexArgs> jsonPayload = createListOfProfileIndexArgs(currentFileFormat,formatNameToDateFormat,payloadDetailsForIndex,jdbcTemplate);
        if (currentFileFormat.equalsIgnoreCase(formatNameToFormatReference.get("mBusFormat"))) {
            Set<String> metersWithHourlyStands = getSetOfMetersWithHourlyStands(jsonPayload);
            List<ProfileIndexArgs> payloadForHourlyStands = initiateListForHourlyStand(metersWithHourlyStands, jsonPayload);
            List<ProfileIndexArgs> jsonPayloadFiltered = excludeNonRelevantReadings(jsonPayload);// Only readings @ 00:00:00 for daily stand.
            if (!jsonPayloadFiltered.isEmpty()) {
                logger.info("Creating jsonPayload for daily stand M-Bus format");
                listOfJSONFiles = new MeasurementJsonGeneratorForMBus("dayStandAtMidnight",jsonPayloadFiltered, 0, maxTransactionsPerFile).getAllJsonFiles();
            }
            if (publishHourlyStand) {
                if (!payloadForHourlyStands.isEmpty()) {
                    logger.info("Creating jsonPayload for hourly stand 'M-Bus' format");
                    logger.info("Found {} meters with hourly stands. Will create payload for hourly stand for those.", metersWithHourlyStands.size());
                    listOfJSONFiles.addAll(new MeasurementJsonGeneratorForMBus("hourlyStandAtOffset1", payloadForHourlyStands, 60, maxTransactionsPerFile).getAllJsonFiles());
                } else {
                    logger.info("No reading suitable for hourly stand transaction where found.");
                }
            }
            else{
                logger.info("Will not publish hourly stand as the value for the 'publishHourlyStand' parameter in the config file is false.");
            }
        }
        else if (currentFileFormat.equalsIgnoreCase(formatNameToFormatReference.get("forbruksImportFormat"))) {
            if (!jsonPayload.isEmpty()) {
                logger.info("Creating jsonPayload for periodic consumption 'Forbruk' format");
                listOfJSONFiles = new MeasurementVolumeJsonGeneratorForForbruk(jsonPayload, 0, maxTransactionsPerFile).getAllJsonFiles();
            } else {
                logger.info("No reading suitable for hourlyVolume transaction where found.");
            }
        }
        else {
            logger.info("Unknown file format ' {} '. The known file formats are 'M-Bus' & 'Forbruk'.",currentFileFormat);
            listOfJSONFiles = new ArrayList<>();
        }
    }

    private Set<String> getSetOfMetersWithHourlyStands(List<ProfileIndexArgs> jsonPayload) {
        Set<String>metersHasHourlyStands = new HashSet<>();
        for (ProfileIndexArgs thisStand : jsonPayload){
            try {
                LocalDateTime ldt = LocalDateTime.parse(thisStand.readTime,input);
                if (ldt.getHour() > 0) {
                    metersHasHourlyStands.add(thisStand.deviceId);
                }
            }catch (NullPointerException e){
                logger.info("Null found for readingTime for daily stand. Hourly stand transaction will be created with GMT-offset = +1 for the entire year.");
            }
        }
        return metersHasHourlyStands;
    }

    private List<ProfileIndexArgs> initiateListForHourlyStand(Set<String> metersWithHourlyStands, List<ProfileIndexArgs> jsonPayload) {
        List<ProfileIndexArgs> l = new ArrayList<>();
        for (String meter : metersWithHourlyStands){
            for (ProfileIndexArgs thisArgs : jsonPayload){
                if (thisArgs.deviceId.equalsIgnoreCase(meter)){
                    l.add(thisArgs);
                }
            }
        }
        return l;
    }

    private List<ProfileIndexArgs> excludeNonRelevantReadings(List<ProfileIndexArgs> jsonPayload) {
        List<ProfileIndexArgs> onlyRelevantReadings = new ArrayList<>(); // Only readings at 00:00:00 hour
        for (ProfileIndexArgs thisTransaction : jsonPayload){
            try {
                LocalDateTime ldt = LocalDateTime.parse(thisTransaction.readTime,input);
                if (ldt.getHour() == 0 && ldt.getMinute() == 0 && ldt.getSecond() == 0) {
                    onlyRelevantReadings.add(thisTransaction);
                }
            }catch (NullPointerException e){
                logger.info("Null found for readingTime for daily stand. Hourly stand transaction will be created with GMT-offset = +1 for the entire year.");
            }
        }
        logger.info("There are {} readings for timestamp = 00:00:00. Only readings @00:00:00 are relevant for creating dayStand payload.", onlyRelevantReadings.size());
        return onlyRelevantReadings;
    }

    private List<ProfileIndexArgs> createListOfProfileIndexArgs(String currentFileFormat,Map<String,String> formatNameToDateFormat,List<String[]> payloadDetailsForIndex,JdbcTemplate jdbcTemplate) {
        List<ProfileIndexArgs> payloadDetails = new ArrayList<>();
        String currentFileDateFormat = formatNameToDateFormat.get(currentFileFormat);
        for (String[] row : payloadDetailsForIndex){
            payloadDetails.add(new ProfileIndexArgs(formatNameToFormatReference,currentFileFormat,currentFileDateFormat,row,jdbcTemplate));
        }
        return payloadDetails;
    }

    public List<String> getListOfJsonFiles(){
        return listOfJSONFiles;
    }

}
