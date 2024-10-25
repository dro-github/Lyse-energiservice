package com.cts.utils;

import org.apache.commons.lang3.tuple.Pair;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.IllegalInstantException;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.math.BigDecimal;
import java.util.Locale;
import java.util.Map;


public class ProfileIndexArgs {
    public String transactionType;
    public String deviceId;
    public String unit;
    public String direction;
    public String createdTime;
    public String originType;
    public String origin;
    public BigDecimal indexValue;
    public String sensorType;
    public String readTime;
    public String readTimeForHourlyTransaction;
    public String fromTime;
    public  String toTime;
    private JdbcTemplate jdbcTemplate;
    private DateTimeFormatter inputFormat;
    private DateTimeFormatter outputFormat;
    private DateTimeFormatter jsonOutput;
    private String fileFormat;
    private Logger logger = LoggerFactory.getLogger(getClass());
    private Map<String,String>formatNameToFormatReference;

    public ProfileIndexArgs(Map<String,String> formatNameToFormatReference,String currentFileFormat, String currentFileDateFormat, String[] row, JdbcTemplate jdbcTemplate){
        this.formatNameToFormatReference = formatNameToFormatReference;
        this.deviceId = row[0];
        this.jdbcTemplate = jdbcTemplate;
        this.fileFormat = currentFileFormat;
        inputFormat = DateTimeFormat.forPattern(currentFileDateFormat); //M-Bus = MM/dd/yyyy HH:mm:ss
        jsonOutput = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssZZ");
        unit = getUnitFromH2DB(deviceId);
        if (unit.equalsIgnoreCase("M3")){
            unit = "m3";
        }
        sensorType = getSensorType();
        direction = "Downstream";
        readTime = setReadTime(row[1]);//Parsed using real-time timeZone (gmt+1/winter and gmt+2/DST - summer).;
        readTimeForHourlyTransaction = setReadTime(row[1]);//Hourly reading are received and registered in current
        /*TODO - For resending of old files after introducing hourly stand - make sure that hourlyStand (ONLY HOURLY STAND! NOT DayStand!) is assigned in UTC.
        *  If offset reference from config file is UTC then parse in UTC - call to setHourlyReadTimeFromInputInUTC(String dateFromInput) - Won't reject based on DST transition days. */
        originType = setOriginType(row);
        origin = setOrigin();
        indexValue = getValue(currentFileFormat,row);
        createdTime = jsonOutput.print(new DateTime(DateTimeZone.getDefault()));
        fromTime = getFromOrToTimeForVolume(currentFileFormat,row,1);
        toTime = getFromOrToTimeForVolume(currentFileFormat,row,2);
    }

    private String setReadTime(String dateFromInput) {
        try{
            return jsonOutput.print(inputFormat.parseDateTime(dateFromInput));
        }catch (IllegalInstantException e){
            logger.info("Non-parsable date was detected {}. Hourly stand transaction will be created with GMT offset = +1 for the entire year. Message: {}",dateFromInput,e.getMessage());
            return null;
        }catch (IllegalArgumentException ex){
            try{
                DateTimeFormatter withoutGMT = DateTimeFormat.forPattern("MM/dd/yyyy HH:mm:ss");
                return jsonOutput.print(withoutGMT.parseDateTime(dateFromInput));
            }catch (IllegalArgumentException exe){
                logger.info("Date format does not match any known date format");
                return null;
            }
        }
    }

    private String setHourlyReadTimeFromInputInUTC(String dateFromInput){
        DateTimeFormatter withoutGMT = DateTimeFormat.forPattern("MM/dd/yyyy HH:mm:ss");
        DateTimeFormatter utc = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss+00:00");
        return utc.print(withoutGMT.parseLocalDateTime(dateFromInput));
    }

    private String setOriginType(String[] row) {
        String originQualityType;
        try {
            originQualityType = row[3];
            return originQualityType;
        }catch (IndexOutOfBoundsException e){
            return "Measured_1";
        }catch ( Exception ex){
            return "Measured_1";
        }
    }

    private String setOrigin() {
        if (originType.equalsIgnoreCase("stipulation") || originType.equalsIgnoreCase("estimated") || originType.equalsIgnoreCase("estimated_4")){
            return "Estimated";
        }
        return "Measured";
    }

    private String getFromOrToTimeForVolume(String currentFileFormat, String[] row,int pos) {
        if (currentFileFormat.equalsIgnoreCase(formatNameToFormatReference.get("forbruksImportFormat"))) {
            return jsonOutput.print(inputFormat.parseDateTime(row[pos]));
        }
        return null;
    }

    private BigDecimal getValue(String currentFileFormat, String[] row) {
        if (currentFileFormat.equalsIgnoreCase(formatNameToFormatReference.get("mBusFormat"))){
            return new BigDecimal(String.valueOf(row[2]).replace(",","."));
        }
        else if (currentFileFormat.equalsIgnoreCase(formatNameToFormatReference.get("forbruksImportFormat"))){
            return new BigDecimal(String.valueOf(row[3]).replace(",","."));
        }
        return null;
    }

    private String getUnitFromH2DB(String deviceId) {
        Pair p = jdbcTemplate.queryForObject("select meter_id,unit from meter_unit where meter_id=?", (resultSet, i) -> Pair.of(resultSet.getString("meter_id"), resultSet.getString("unit")), deviceId);
        logger.debug("Got meter details for meter {}, UNIT {}, from internal local H2 DB.",p.getKey(), p.getValue());
        return String.valueOf(p.getValue());
    }

    private String getSensorType() {
        if (unit.equalsIgnoreCase("step")){
            return "Step";
        }
        else if (unit.equalsIgnoreCase("m3")){
            return "Volume";
        }
        else if (unit.equalsIgnoreCase("kWh")){
            return "ActiveEnergy";
        }
        return "ActiveEnergy";
    }
}
