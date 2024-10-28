package com.cts.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class GroupInputPerMeter {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final List<String[]> inputOrderedPerMeter;
    private final String currentFileFormat;
    private final Map<String, String> formatNameToDateFormat;

    public GroupInputPerMeter(String fileFormat, List<String[]> inputRows, Map<String, String> formatNameToDateFormat) {
        currentFileFormat = fileFormat;
        this.formatNameToDateFormat = formatNameToDateFormat;
        inputOrderedPerMeter = groupInputPerMeter(inputRows);
    }

    private List<String[]> groupInputPerMeter(List<String[]> inputRows) {
        List<String> distinctMetersInFile = getListOfDistinctMetersInFile(inputRows);
        List<String[]> groupedInput = new ArrayList<>();
        for (String currMeterID : distinctMetersInFile) {
            for (String[] str : inputRows) {
                if (str[0].equalsIgnoreCase(currMeterID)) {
                    groupedInput.add(str);
                }
            }
        }
        return orderInputByTimestamp(distinctMetersInFile, groupedInput);
    }

    private List<String[]> orderInputByTimestamp(List<String> distinctMetersInFile, List<String[]> groupedInput) {
        List<String[]> groupedAndOrderedInput = new ArrayList<String[]>();
        for (String meter : distinctMetersInFile) {
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
        DateTimeFormatter inputFormat = DateTimeFormatter.ofPattern(dateFormatString);
        try {
            Comparator<String[]> compByDate = Comparator.comparing((String[] o) -> ZonedDateTime.parse(o[1],inputFormat));
            occurrencesForOneMeter.sort(compByDate);
            return occurrencesForOneMeter;
        }catch (IllegalArgumentException e){
            try {
                DateTimeFormatter withoutGMT = DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss");
                Comparator<String[]> compByDate = Comparator.comparing((String[] o) -> ZonedDateTime.parse(o[1],withoutGMT));
                occurrencesForOneMeter.sort(compByDate);
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
