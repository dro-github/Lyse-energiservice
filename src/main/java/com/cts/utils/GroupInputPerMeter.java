package com.cts.utils;

import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class GroupInputPerMeter {

    private Logger logger = LoggerFactory.getLogger(getClass());
    private List<String[]> inputOrderedPerMeter;
    private List<String[]> latestOccurrencePerMeter;
    private List<String> distinctMetersInFile;
    private String currentFileFormat;
    private Map<String, String> formatNameToFormatReference;
    private Map<String, String> formatNameToDateFormat;

    public GroupInputPerMeter(String fileFormat, List<String[]> inputRows, Map<String, String> formatNameToFormatReference, Map<String, String> formatNameToDateFormat) {
        currentFileFormat = fileFormat;
        this.formatNameToFormatReference = formatNameToFormatReference;
        this.formatNameToDateFormat = formatNameToDateFormat;
        inputOrderedPerMeter = groupInputPerMeter(inputRows);
    }

    private List<String[]> groupInputPerMeter(List<String[]> inputRows) {
        distinctMetersInFile = getListOfDistinctMetersInFile(inputRows);
        List<String[]> groupedInput = new ArrayList<>();
        for (int i = 0; i < distinctMetersInFile.size(); i++) {
            String currMeterID = distinctMetersInFile.get(i);
            for (String[] str : inputRows) {
                if (str[0].equalsIgnoreCase(currMeterID)) {
                    groupedInput.add(str);
                }
            }
        }
        List<String[]> groupedAndOrdered = orderInputByTimestamp(distinctMetersInFile, groupedInput);
        return groupedAndOrdered;
    }

    private List<String[]> orderInputByTimestamp(List<String> distinctMetersInFile, List<String[]> groupedInput) {
        List<String[]> groupedAndOrderedInput = new ArrayList<String[]>();
        latestOccurrencePerMeter = new ArrayList<>();
        for (int i = 0; i < distinctMetersInFile.size(); i++) {
            String meter = distinctMetersInFile.get(i);
            List<String[]> occurrencesForOneMeter = new ArrayList<>();
            for (String[] str : groupedInput) {
                if (str[0].equalsIgnoreCase(meter)) {
                    occurrencesForOneMeter.add(str);
                }
            }
            List<String[]> sortedAndOrdered = sortInputByTimestamp(occurrencesForOneMeter);
            groupedAndOrderedInput.addAll(sortedAndOrdered);
        }
        return groupedAndOrderedInput;
    }

    private List<String[]> sortInputByTimestamp(List<String[]> occurrencesForOneMeter) {
        String dateFormatString = GetDateFormatForFile.getDateFormatForFile(currentFileFormat,formatNameToDateFormat);
        DateTimeFormatter inputFormat = DateTimeFormat.forPattern(dateFormatString);
        try {
            Comparator<String[]> compByDate = Comparator.comparing((String[] o) -> inputFormat.parseDateTime(o[1]));
            Collections.sort(occurrencesForOneMeter,compByDate);
            return occurrencesForOneMeter;
        }catch (IllegalArgumentException e){
            try {
                DateTimeFormatter withoutGMT = DateTimeFormat.forPattern("MM/dd/yyyy HH:mm:ss");
                Comparator<String[]> compByDate = Comparator.comparing((String[] o) -> withoutGMT.parseDateTime(o[1]));
                Collections.sort(occurrencesForOneMeter,compByDate);
                return occurrencesForOneMeter;
            }catch (IllegalArgumentException ex){
                logger.info("Date format does not match any known date format");
                return occurrencesForOneMeter;
            }
        }
    }

    private List<String> getListOfDistinctMetersInFile(List<String[]> inputRows) {
        return GetDistinctMetersInFile.initiateList(inputRows);
    }

    public List<String[]> getInputOrderedPerMeter() {
        return inputOrderedPerMeter;
    }
}
