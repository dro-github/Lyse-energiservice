package com.cts.utils;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
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
    private final JdbcTemplate jdbcTemplate;
    private final DateTimeFormatter inputFormat;
    private DateTimeFormatter outputFormat;
    private final DateTimeFormatter jsonOutput;
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Map<String,String>formatNameToFormatReference;

    public ProfileIndexArgs(Map<String,String> formatNameToFormatReference,String currentFileFormat, String currentFileDateFormat, String[] row, JdbcTemplate jdbcTemplate){
        this.formatNameToFormatReference = formatNameToFormatReference;
        this.deviceId = row[0];
        this.jdbcTemplate = jdbcTemplate;
        inputFormat = DateTimeFormatter.ofPattern(currentFileDateFormat);
        jsonOutput = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssxxx");
        unit = getUnitFromH2DB(deviceId);
        if (unit.equalsIgnoreCase("M3")){
            unit = "m3";
        }
        sensorType = getSensorType();
        direction = "Downstream";
        readTime = setReadTime(currentFileFormat,row[1]);//Parsed using real-time timeZone (gmt+1/winter and gmt+2/DST - summer).;
        readTimeForHourlyTransaction = setReadTime(currentFileFormat,row[1]);//Hourly reading are received and registered in current
        /*TODO - For resending of old files after introducing hourly stand - make sure that hourlyStand (ONLY HOURLY STAND! NOT DayStand!) is assigned in UTC.
        *  If offset reference from config file is UTC then parse in UTC - call to setHourlyReadTimeFromInputInUTC(String dateFromInput) - Won't reject based on DST transition days. */
        originType = setOriginType(row);
        origin = setOrigin();
        indexValue = getValue(currentFileFormat,row);
        createdTime = jsonOutput.format(ZonedDateTime.now().withZoneSameInstant(ZoneId.of("Europe/Oslo")));
        fromTime = getFromOrToTimeForVolume(currentFileFormat,row,1);
        toTime = getFromOrToTimeForVolume(currentFileFormat,row,2);
    }

    private String setReadTime(String currentFileFormat,String dateFromInput) {
        if (currentFileFormat.equalsIgnoreCase("MVV")) {
            try {
                LocalDateTime ldt = LocalDateTime.parse(dateFromInput, inputFormat);
                ZonedDateTime zdt = ldt.atZone(ZoneId.of("Europe/Oslo"));
                return jsonOutput.format(zdt);
            } catch (Exception e) {
                logger.info("FileFormat = MVV. Non-parsable date was detected {}. Hourly stand transaction will be created with GMT offset = +1 for the entire year. Message: {}",dateFromInput, e.getMessage());
                return null;
            }
        }
        else {
            try {
                LocalDate ld = LocalDate.parse(dateFromInput, inputFormat);
                ZonedDateTime zdt = ld.atStartOfDay().atZone(ZoneId.of("Europe/Oslo"));
                return jsonOutput.format(zdt);
            } catch (Exception e) {
                logger.info("FileFormat = Zaptec. Non-parsable date was detected {}. Hourly stand transaction will be created with GMT offset = +1 for the entire year. Message: {}", dateFromInput, e.getMessage());
                return null;
            }
        }
    }

    private String setOriginType(String[] row) {
        String originQualityType;
        try {
            originQualityType = row[3];
            return originQualityType;
        } catch (Exception ex){
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
            LocalDate ldt = LocalDate.parse(row[pos],inputFormat);
            ZonedDateTime zdt = ldt.atStartOfDay().atZone(ZoneId.of("Europe/Oslo"));
            return jsonOutput.format(zdt);
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
