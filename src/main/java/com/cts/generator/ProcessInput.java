package com.cts.generator;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.cts.nanoMeteringGateway.CheckUnitInNanoMetering;
import com.cts.ioutils.InputFileReader;
import com.cts.utils.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;


public class ProcessInput {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private List<String> jsonFiles;
    private static final StringBuilder avvikRapport = new StringBuilder("");
    private List<String[]> inputFileUnFiltered;
    private List<String[]> inputRowsAsStringArrays;
    private String currentFileFormat;
    private InputFileReader inputFileReader;
    private String url_iscustomer;
    private String userName;
    private String passWord;
    private Map<String, String> formatNameToFormatReference;
    private Map<String,String> formatNameToDateFormat;
    private JdbcTemplate jdbcTemplate;
    private int max_transactions_per_file;


    public ProcessInput(File csvFile, String fileFormat, String url_iscustomer, String userName, String passWord, JdbcTemplate jdbcTemplate,
                        int max_transactions_per_file, Map<String, String> formatNameToFormatReference,Map<String,String> formatNameToDateFormat,boolean publishHourlyStand) throws IOException {
        this.url_iscustomer = url_iscustomer;
        this.userName = userName;
        this.passWord = passWord;
        this.formatNameToFormatReference = formatNameToFormatReference;
        this.formatNameToDateFormat = formatNameToDateFormat;
        this.jdbcTemplate = jdbcTemplate;
        this.max_transactions_per_file = max_transactions_per_file;
        currentFileFormat = fileFormat;
        logger.info("Start processing input file {}.", csvFile.getName());
        inputFileReader = new InputFileReader(csvFile, formatNameToFormatReference, fileFormat);
        inputFileUnFiltered = inputFileReader.getInputFileUnFiltered();//All input rows.
        inputRowsAsStringArrays = inputFileReader.getData();//rows with correct/minimum number of required parameters.
        logger.info("Collected {} input rows.", inputRowsAsStringArrays.size());
        logger.info("Checking correct date format.");
        List<String[]> hasCorrectDateFormat = checkDateFormat(inputRowsAsStringArrays, currentFileFormat);
        if (hasCorrectDateFormat.size() > 0) {
            logger.info("Checking correct number format.");
            List<String[]> hasCorrectNumberFormat = checkNumberFormat(hasCorrectDateFormat, currentFileFormat, formatNameToFormatReference);
            if (hasCorrectNumberFormat.size() > 0) {
                List<String[]> hasCustomerEntryAndCorrectDateAndNumberFormat = excludeMetersNotInCustomer(hasCorrectNumberFormat);
                logger.info("proceeding to process {} input rows.", hasCustomerEntryAndCorrectDateAndNumberFormat.size());
                if (hasCustomerEntryAndCorrectDateAndNumberFormat.size() > 0 && hasCustomerEntryAndCorrectDateAndNumberFormat != null){
                    List<String[]> standReadingsToProcess = GroupAndSortInput(formatNameToFormatReference,formatNameToDateFormat,hasCustomerEntryAndCorrectDateAndNumberFormat);
                    createLastStandFile(standReadingsToProcess, currentFileFormat, formatNameToDateFormat,publishHourlyStand);
                }
            }
        } else{// no data to print - i.e. all input rows are wrong
            createDeviationReportForWholeFile();
            logger.info("Did not find any processable input. Writing a deviation report for the whole input file.");
        }
    }

    private List<String[]> checkDateFormat(List<String[]> inputRowsAsStringArrays, String fileName) {
        return new VerifyDateFormat(inputRowsAsStringArrays, fileName, formatNameToFormatReference,formatNameToDateFormat,avvikRapport).getInputRowsHavingCorrectDateFormat();
    }

    private List<String[]> checkNumberFormat(List<String[]> hasCorrectDateFormat, String fileName, Map<String, String> formatNameToFormatReference) {
        return new VerifyNumberFormat(hasCorrectDateFormat, fileName, formatNameToFormatReference,avvikRapport).getInputRowsHavingCorrectNumberFormat();
    }

    private List<String[]> excludeMetersNotInCustomer(List<String[]> completeDataFromInput) {
        logger.info("Received {} input rows, will check for an entry in customer for distinct meters in the input.", completeDataFromInput.size());
        return new CheckUnitInNanoMetering(completeDataFromInput,jdbcTemplate,avvikRapport).getMetersInCustomer();
    }

    public List<String[]> GroupAndSortInput(Map<String, String> formatNameToFormatReference, Map<String, String> formatNameToDateFormat,List<String[]> hasCustomerEntryAndCorrectDateAndNumberFormat) throws IOException {
            GroupInputPerMeter groupInputPerMeter = new GroupInputPerMeter(currentFileFormat, hasCustomerEntryAndCorrectDateAndNumberFormat, formatNameToFormatReference, formatNameToDateFormat);
        List<String[]> standReadingsToProcess = null;
        if (currentFileFormat.equalsIgnoreCase(formatNameToFormatReference.get("mBusFormat"))) {
            standReadingsToProcess = groupInputPerMeter.getInputOrderedPerMeter(); //Ordered per meter and sorted per standTimeStamp
            logger.info("Will process {} records for json file - mBusFormat.", standReadingsToProcess.size());
        }
        else if (currentFileFormat.equalsIgnoreCase(formatNameToFormatReference.get("forbruksImportFormat"))) {
            standReadingsToProcess = groupInputPerMeter.getInputOrderedPerMeter(); //Ordered per meter and sorted per standTimeStamp
            logger.info("Will process {} records for json file - forbruksImportFormat.", standReadingsToProcess.size());
        }
            updateDeviationReportFromFileReader();
            return standReadingsToProcess;
    }

    private void createLastStandFile(List<String[]> inputOrderedPerMeterAndSortedPerTimestamp,String currentFileFormat,Map<String,String> formatNameToDateFormat,boolean publishHourlyStand) throws JsonProcessingException {
        jsonFiles = new CreateJSONForProfileIndexD0(formatNameToFormatReference,currentFileFormat,formatNameToDateFormat,inputOrderedPerMeterAndSortedPerTimestamp,jdbcTemplate, max_transactions_per_file,publishHourlyStand).getListOfJsonFiles();
        logger.info("Created json file/s from input.");
    }

    public List<String> getLastHourlyStandReadyToPrintFiles() {
        if (jsonFiles != null) {
            return jsonFiles;
        }
        return new ArrayList<>();
    }

    private void updateDeviationReportFromFileReader() {
        List<String> completeInputFileAsStrings = new ArrayList<>();
        List<String> inputFilteredInFileReader = new ArrayList<>();
        if (inputFileUnFiltered.size() > inputRowsAsStringArrays.size()) {
            for (String[] inputRow : inputFileUnFiltered) {
                String oneRowFromArray = Arrays.stream(inputRow).collect(Collectors.joining(" "));
                completeInputFileAsStrings.add(oneRowFromArray);
            }
            for (String[] filteredRow : inputRowsAsStringArrays) {
                String oneRowFromFilteredArray = Arrays.stream(filteredRow).collect(Collectors.joining(" "));
                inputFilteredInFileReader.add(oneRowFromFilteredArray);
            }
            for (String s : completeInputFileAsStrings){
                if (!inputFilteredInFileReader.contains(s)) {
                    appendRowToDeviationReport(inputFileUnFiltered.get(completeInputFileAsStrings.indexOf(s)));
                }
            }
        }
    }

    private void createDeviationReportForWholeFile() {
        for (String[] excludedRow : inputRowsAsStringArrays){
            appendRowToDeviationReport(excludedRow);
        }
    }

    public void appendRowToDeviationReport(String[] excludedRow) {
        for (int i = 0; i < excludedRow.length ; i++) {
            avvikRapport.append(excludedRow[i]);
            if(i < excludedRow.length - 1) {
                avvikRapport.append(";");
            }
        }
        avvikRapport.append(("\r\n"));
    }

    public StringBuilder getAvvikRapport(){
        return  avvikRapport;
    }
}
