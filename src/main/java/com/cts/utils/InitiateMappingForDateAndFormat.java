package com.cts.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class InitiateMappingForDateAndFormat {

    private Logger logger = LoggerFactory.getLogger(getClass());
    private Map<String,String> formatNameToFormatReference;

    public InitiateMappingForDateAndFormat(String typeOfMapping, String mBusNameValue,String forbruksNameValue,String M_busDateValue, String ForbrukDateValue){
        if (typeOfMapping.equalsIgnoreCase("formatName")) {
            formatNameToFormatReference = initiateMapping(mBusNameValue, forbruksNameValue);
        }
        else {
            formatNameToFormatReference = getFormatNameToDateFormat(mBusNameValue,forbruksNameValue,M_busDateValue,ForbrukDateValue);
        }
    }

    private Map<String, String> initiateMapping(String mBusValue,String forbruksValue ) {
        try {
            Map<String, String> m = new HashMap<>();
            m.put("mBusFormat", mBusValue);
            m.put("forbruksImportFormat", forbruksValue);
            return m;
        } catch (Exception e) {
            logger.info("Could not initiate mapping between formatName and corresponding formatReference. Check application.properties file for correct configuration.");
            return null;
        }
    }

    public Map<String, String> getFormatNameToDateFormat(String mBusNameValue,String forbruksNameValue,String M_busDateValue,String ForbrukDateValue) {
        try {
            Map<String, String> m = new HashMap<>();
            m.put(mBusNameValue,M_busDateValue);
            m.put(forbruksNameValue,ForbrukDateValue);
            return m;
        }catch (Exception e){
            logger.info("Could not initiate mapping between formatName and corresponding date format. Check application.properties file for correct configuration.");
            return null;
        }
    }

    public Map<String, String> getFormatNameToFormatReference() {
        return formatNameToFormatReference;
    }
}
