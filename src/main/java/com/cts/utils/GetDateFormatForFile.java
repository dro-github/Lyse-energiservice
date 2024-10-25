package com.cts.utils;

import java.util.Map;

public class GetDateFormatForFile {

    public static String getDateFormatForFile(String currentFileFormat,Map<String,String> formatNameToDateFormat){
        String dateFormatString = formatNameToDateFormat.get(currentFileFormat);
        return dateFormatString;
    }
}
