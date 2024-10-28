package com.cts.utils;

import com.cts.generator.ProcessInput;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class VerifyDateFormat {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private String currentFileFormat;
    private List<String[]> inputRowsHavingCorrectDateFormat;


    public VerifyDateFormat(List<String[]> allInputRows, String fileFormat, Map<String, String> formatNameToFormatReference,Map<String,String> formatNameToDateFormat,StringBuilder avvikRapport){
        currentFileFormat = fileFormat;
        inputRowsHavingCorrectDateFormat = verifyDateFormat(allInputRows,formatNameToFormatReference,formatNameToDateFormat,avvikRapport);
    }

    private List<String[]> verifyDateFormat(List<String[]> allInputRows,Map<String, String> formatNameToFormatReference,Map<String,String> formatNameToDateFormat,StringBuilder avvikRapport) {
        List<String[]> verifiedDates = new ArrayList<>();
        int counter = 0;
        long startTime = System.currentTimeMillis();
        for (String[] oneInputRow : allInputRows){
            counter ++;
            /**
            if (counter == 1 && currentFileFormat.equalsIgnoreCase(formatNameToFormatReference.get("forbruksImportFormat"))) { //header row  - for forbruk format must be included in the deviation report.
                verifiedDates.add(oneInputRow);
                continue;
            }
             */
            if (hasCorrectDateFormat(oneInputRow,formatNameToFormatReference,formatNameToDateFormat,avvikRapport)){
                verifiedDates.add(oneInputRow);
                if(counter % 100 == 0){
                    logger.info("So far checked {} rows, out of which {} have correct date format.",counter,verifiedDates.size());
                }
            }
        }
        logger.info("Completed checking {} rows, out of which {} have correct date format.",counter,verifiedDates.size());
        if ((System.currentTimeMillis() - startTime) / 1000 < 1){
            logger.info("Checked date for {} rows in less than a second.",counter);
        }
        else {
            logger.info("Checked date for {} rows in {} seconds.", counter, (System.currentTimeMillis() - startTime) / 1000);
        }
        return verifiedDates;
    }

    private boolean hasCorrectDateFormat(String[] oneRow,Map<String, String> formatNameToFormatReference,Map<String,String> formatNameToDateFormat,StringBuilder avvikRapport) {
        /*TODO - For resending of old files after introducing hourly stand - make sure that hourlyStand (ONLY HOURLY STAND! NOT DayStand!) are parsed in UTC.
         *  If offset reference from config file is UTC then parse in UTC - call to verifyDateInUTC(String[] oneRow) - Won't reject based on DST transition days. */
        if (currentFileFormat.equalsIgnoreCase(formatNameToFormatReference.get("mBusFormat"))){ // M-Bus format = MM/dd/yyyy (USA date format)
            try{
                String dateFormatString = formatNameToDateFormat.get(formatNameToFormatReference.get("mBusFormat"));
                DateTimeFormatter inputFormat =  DateTimeFormatter.ofPattern(dateFormatString);
                inputFormat.parse(oneRow[1].trim());
                return true;
            } catch (IllegalArgumentException e) {
                try {
                    DateTimeFormatter withoutGMT = DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss");
                    withoutGMT.parse(oneRow[1].trim());
                    return true;
                } catch (IllegalArgumentException ex) {
                    logger.info("This input row contains non parsable date parameter: meter_id = {}; date = {}. ", oneRow[0], oneRow[1]);
                    appendRowToDeviationReport(avvikRapport, oneRow);
                    return false;
                }
            }
        }
        else if (currentFileFormat.equalsIgnoreCase(formatNameToFormatReference.get("forbruksImportFormat"))){
            try{
                String dateFormatString = formatNameToDateFormat.get(formatNameToFormatReference.get("forbruksImportFormat")) ;
                DateTimeFormatter inputFormat = java.time.format.DateTimeFormatter.ofPattern(dateFormatString).withZone(ZoneId.of("Europe/Oslo"));
                //inputFormat.parse(oneRow[1]);
                //inputFormat.parse(oneRow[2]);
                return true;
            } catch (IllegalArgumentException e){
                logger.info("This input row contains non parsable from_date or to_date parameter: meter_id = {}; From_date = {}; To_date = {}. ",oneRow[0],oneRow[1],oneRow[2]);
                return false;
            }
        }
        logger.info("FileFormat = {} is not recognized ",currentFileFormat);
        return false;
    }

    private boolean verifyDateInUTC(String[] oneRow){
        DateTimeFormatter withoutGMT = DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss");
        DateTimeFormatter utc = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss+00:00");
        try {
            utc.format(LocalDateTime.parse(oneRow[1].trim(),withoutGMT));
            return true;
        }catch (IllegalArgumentException exe) {
            logger.info("VerifyDateFormat: Date format does not match any known date format");
            return false;
        }
    }

    public void appendRowToDeviationReport(StringBuilder avvikRapport, String[] excludedRow) {
        for (int i = 0; i < excludedRow.length ; i++) {
            avvikRapport.append(excludedRow[i]);
            if(i < excludedRow.length - 1) {
                avvikRapport.append(";");
            }
        }
        avvikRapport.append(("\r\n"));
    }

    public List<String[]> getInputRowsHavingCorrectDateFormat() {
        return inputRowsHavingCorrectDateFormat;
    }
}
