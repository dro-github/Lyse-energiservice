package com.cts.ioutils;

import com.cts.logger.BasicConfApp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class InputFileReader {

    private List<String[]> inputFileUnFiltered;
    private List<String[]> rowsCombined;
    private Map<String,String> formatNameToFormatReference;
    private String fileFormat;
    final static Logger logger = LoggerFactory.getLogger(BasicConfApp.class);

    public InputFileReader(File csvFile, Map<String,String> formatNameToFormatReference, String fileFormat){
        logger.info("Starting to read {}.",csvFile.getName());
        this.formatNameToFormatReference = formatNameToFormatReference;
        this.fileFormat = fileFormat;
        inputFileUnFiltered = new ArrayList<>();
        rowsCombined = new ArrayList<>();
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(csvFile);
            readCSVFileIntoListOfStringArrays1(fis);
        }
        catch (IOException e){
            logger.error("Error occurred while reading file. Message: {}",e.getMessage());
        }
        finally {
            try {
                fis.close();
            }
            catch (IOException e) {
                logger.error("Error occurred while closing file. Message: {}",e.getMessage());
            }
        }
    }

    public void readCSVFileIntoListOfStringArrays1(FileInputStream fis) throws IOException {
        BufferedReader inputStreamReader = new BufferedReader(new InputStreamReader(fis));
        String thisLine;
        int rowCounter = 0;
        while ((thisLine = inputStreamReader.readLine()) != null) {
            inputFileUnFiltered.add(thisLine.split(";"));
            if (thisLine.trim().equals("") || !thisLine.trim().contains(";") || thisLine.length() < 1 ||
                    (!thisLine.matches(".*[0-9].*") && !thisLine.matches(".*[a-zA-Z].*"))){
                continue;
            }
            rowCounter ++;
            String[] oneRow = thisLine.split(";");
            String[]oneRowTrimmed = trimRow(oneRow);
            if (hasMinNumbeOfRequieredParameters(oneRowTrimmed)) { // minimum required number of details = 3
                rowsCombined.add(oneRowTrimmed);
            }
            if (rowCounter % 100 == 0){
                logger.info("So far read {} rows.",rowCounter);
            }
        }
        logger.info("Completed reading. Total rows read = {}.",rowCounter);
    }

    private boolean hasMinNumbeOfRequieredParameters(String[] oneRowTrimmed) {
        if(fileFormat.equalsIgnoreCase(formatNameToFormatReference.get("mBusFormat"))){
            if(oneRowTrimmed.length > 2){ // numberToCheck in oneRowTrimmed[2]
                return true;
            }
            return false;
        }
        else if (fileFormat.equalsIgnoreCase(formatNameToFormatReference.get("forbruksImportFormat"))){
            if (oneRowTrimmed.length > 3){ // numberToCheck in oneRowTrimmed[3]
                return true;
            }
            return false;
        }
        logger.info("FileFormat = {} is not recognized ",fileFormat);
        return false;
    }

    private String[] trimRow(String[] oneRow) {
        String[]trimmedRow = new String[oneRow.length];
        for (int i = 0; i < oneRow.length; i++){
            String str = oneRow[i].trim();
            trimmedRow[i] = str;
        }
        return trimmedRow;
    }

    public List<String[]> getInputFileUnFiltered() {
        return inputFileUnFiltered;
    }

    public List<String[]> getData(){
        return rowsCombined;
    }

    /**
     public static void main(String[] args) {
     new InputfileReader(new File("C:\\Users\\ronend\\Desktop\\BKK_INPUT_TXT_FILES\\test_1.txt"));
     }*/
}
