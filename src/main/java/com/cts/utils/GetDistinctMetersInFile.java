package com.cts.utils;

import java.util.ArrayList;
import java.util.List;

public class GetDistinctMetersInFile {

    public static List<String> initiateList(List<String[]> inputRows) {
        List<String> distinctMetersIDS = new ArrayList<>();
        String currMeterID = inputRows.get(0)[0];
        distinctMetersIDS.add(currMeterID);
        for(String[] str : inputRows){
            if(!str[0].equalsIgnoreCase(currMeterID) && !distinctMetersIDS.contains(str[0])){
                distinctMetersIDS.add(str[0]);
                currMeterID = str[0];
            }
        }
        return distinctMetersIDS;
    }
}
