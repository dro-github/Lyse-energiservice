package com.cts.utils;
import com.cts.logger.BasicConfApp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class VerifyNumberFormat {

    private final static Logger logger = LoggerFactory.getLogger(BasicConfApp.class);
    private String currentFileFormat;
    private List<String[]> inputRowsHavingCorrectNumberFormat;

    public VerifyNumberFormat(List<String[]> inputRowsHavingCorrectDateFormat, String fileFormat, Map<String,String> formatNameToFormatReference,StringBuilder avvikRapport){
        currentFileFormat = fileFormat;
        inputRowsHavingCorrectNumberFormat = verifyCorrectNumberFormat(inputRowsHavingCorrectDateFormat,formatNameToFormatReference,avvikRapport);
    }

    private List<String[]> verifyCorrectNumberFormat(List<String[]> inputRowsHavingCorrectDateFormat,Map<String,String> formatNameToFormatReference,StringBuilder avvikRapport) {
        List<String[]> verifiedNumber = new ArrayList<>();
        int counter = 0;
        long startTime = System.currentTimeMillis();
        for (String[] oneRow :inputRowsHavingCorrectDateFormat){
            counter ++;
            /**
            if (counter == 1 && currentFileFormat.equalsIgnoreCase(formatNameToFormatReference.get("forbruksImportFormat"))) { //header row  - for forbruk format must be included in the deviation report.
                verifiedNumber.add(oneRow);
                continue;
            }
             */
            if (hasCorrectNumberFormat(oneRow,formatNameToFormatReference,avvikRapport)){
                verifiedNumber.add(oneRow);
                if(counter % 100 == 0){
                    logger.info("So far checked {} rows, out of which {} have correct number format.",counter,verifiedNumber.size());
                }
            }
        }
        logger.info("Completed checking {} rows, out of which {} have correct number format.",counter,verifiedNumber.size());
        if ((System.currentTimeMillis() - startTime) / 1000 < 1){
            logger.info("Checked number for {} rows in less than a second.",counter);
        }
        else {
            logger.info("Checked number for {} rows in {} seconds.", counter, (System.currentTimeMillis() - startTime) / 1000);
        }
        return verifiedNumber;
    }

    private boolean hasCorrectNumberFormat(String[] oneRow,Map<String,String> formatNameToFormatReference,StringBuilder avvikRapport) {
        if (currentFileFormat.equalsIgnoreCase(formatNameToFormatReference.get("mBusFormat"))){
            try{
                BigDecimal bd = new BigDecimal(oneRow[2].replace(",", ".")).setScale(3, RoundingMode.HALF_EVEN);
                return true;
            } catch (NumberFormatException e){
                logger.info("This input row contains non parsable stand parameter: meter_id = {}; stand = {}. ",oneRow[0],oneRow[2]);
                appendRowToDeviationReport(avvikRapport,oneRow);
                return false;
            }
        }
        else if (currentFileFormat.equalsIgnoreCase(formatNameToFormatReference.get("forbruksImportFormat"))){ //  Forbruksimport format
            try{
                BigDecimal bd = new BigDecimal(oneRow[3].replace(",", ".")).setScale(3, RoundingMode.HALF_EVEN);
                return true;
            } catch (NumberFormatException e){
                logger.info("This input row contains non parsable stand/volume parameter: meter_id = {}; Number = {}. ",oneRow[0],oneRow[3]);
                return false;
            }
        }
        logger.info("FileFormat = {} is not recognized ",currentFileFormat);
        return false;
    }

    public void appendRowToDeviationReport(StringBuilder avvikRapport,String[] excludedRow) {
        for (int i = 0; i < excludedRow.length; i++) {
            avvikRapport.append(excludedRow[i]);
            if (i < excludedRow.length - 1) {
                avvikRapport.append(";");
            }
        }
        avvikRapport.append(("\r\n"));
    }

    public List<String[]> getInputRowsHavingCorrectNumberFormat() {
        return inputRowsHavingCorrectNumberFormat;
    }
}
