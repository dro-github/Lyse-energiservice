package com.cts.customerGateway;

import com.cts.devicemeteringpointconnections.GetMeteringPointForDevice;
import com.cts.devicemeteringpointconnections.GetUnitForMeteringPoint;
import com.cts.logger.BasicConfApp;
import com.cts.utils.GetDistinctMetersInFile;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.ArrayList;
import java.util.List;

public class CheckMeterInCustomer {

    private final List<String[]> metersInCustomer;
    private final JdbcTemplate jdbcTemplate;
    private final static Logger logger = LoggerFactory.getLogger(BasicConfApp.class);

    public CheckMeterInCustomer(List<String[]> allInputRows, JdbcTemplate jdbcTemplate, StringBuilder avvikRapport){
        this.jdbcTemplate = jdbcTemplate;
        logger.info("Starting to check for UNIT in nanoMetering.");
        metersInCustomer = filterInputRows(allInputRows,avvikRapport);
    }

    private List<String[]> filterInputRows(List<String[]> allInputRows,StringBuilder avvikRapport) {
        List<String[]> filteredList = new ArrayList<>();
        List<String> distinctMetersInInput = getDistinctMeters(allInputRows);
        logger.info("Found {} distinct meters. Will check for UNIT in nanoMetering registry for each.",distinctMetersInInput.size());
        List<String> metersFoundInCustomer = getDistinctMetersInNanoMetering(distinctMetersInInput);
        for (String[] row : allInputRows){
            if(metersFoundInCustomer.contains(row[0])){
                filteredList.add(row);
            }
            else {
                appendRowToDeviationReport(row,avvikRapport);
            }
        }
        return filteredList;
    }

    public List<String[]> getMetersInCustomer() {
        return metersInCustomer;
    }

    private List<String> getDistinctMetersInNanoMetering(List<String> distinctMetersInInput) {
        List<String> metersFoundInCustomer = new ArrayList<>();
        int counter = 0;
        long startTimeInMillis = System.currentTimeMillis();
        for (String meterId : distinctMetersInInput){
            counter ++;
            if(hasCustomerEntry(meterId)) {
                metersFoundInCustomer.add(meterId);
            }
            else {
                //retrieve the details from nM openAPI then call updateH2DB(String meterId, String unit);
                String meteringPointId = new GetMeteringPointForDevice(meterId).getMeteringPointId();
                String unit = new GetUnitForMeteringPoint(meteringPointId).getUnit();
                if (unit != null) {
                    metersFoundInCustomer.add(meterId);
                    updateH2DB(meterId, unit);
                }
                else {
                    logger.info("Could not get unit got meter {}.",meterId);
                }
            }
            if (counter % 100 == 0 || counter == distinctMetersInInput.size()){
                logger.info("So far checked {} meters. Out of which {} has Customer entry.",counter, metersFoundInCustomer.size());
                logger.info("Time to check {} meters =  {} seconds.",counter, (System.currentTimeMillis() - startTimeInMillis) / 1000);
            }
        }
        logger.info("Completed check for entry in Customer, found {} meters which have a Customer record.", metersFoundInCustomer.size());
        return metersFoundInCustomer;
    }

    private boolean hasCustomerEntry(String meterId){
        String unit;
        try{
            Pair<String, String> p = jdbcTemplate.queryForObject("select meter_id,unit from meter_unit where meter_id=?", (resultSet, i) -> Pair.of(resultSet.getString("meter_id"), resultSet.getString("unit")), meterId);
            assert p != null;
            unit = p.getValue();
            assert unit != null;
            logger.debug("Got meter details for meter {}, UNIT {}, from internal local H2 DB.",p.getKey(), p.getValue());
            return true;
        }
        catch (EmptyResultDataAccessException ignore){
            logger.info("Could not find meter {} in the base of previously checked meters, will try to get details by API call to ISCU.", meterId);
            return false;
        }
        catch (IndexOutOfBoundsException e){
            logger.warn("could not get UNIT value for meter_id = {}. IndexOutOfBoundsException message below:",meterId);
            logger.error("Unable to fetch UNIT.",e);
            logger.info("Meter {} will be added to the deviation report. No gs2 transaction will be created for meter {}.",meterId,meterId);
            return false;
        }
        catch (Exception ex){
            logger.warn("could not get UNIT value for meter_id = {}. This meter is ignored. General Exception. Exception message below:",meterId);
            logger.error("Unable to fetch UNIT.",ex);
            return false;
        }
    }

    private void updateH2DB(String meterId, String unit) {
        logger.debug("will try to insert meter {}, unit {},to local H2 database.",meterId,unit);
        try {
            jdbcTemplate.update("insert into meter_unit values (?, ?)", meterId, unit);
            logger.debug("Inserted meter {}, UNIT {} to the H2 database.", meterId, unit);
        }catch (Exception e){
            logger.warn("Could not update theH2 database. Error message below:");
            logger.error(e.getMessage());
        }
    }

    private List<String> getDistinctMeters(List<String[]> allInputRows) {
        return GetDistinctMetersInFile.initiateList(allInputRows);
    }

    public void appendRowToDeviationReport(String[] excludedRow,StringBuilder avvikRapport) {
        for (int i = 0; i < excludedRow.length ; i++) {
            avvikRapport.append(excludedRow[i]);
            if(i < excludedRow.length - 1) {
                avvikRapport.append(";");
            }
        }
        avvikRapport.append(("\r\n"));
    }
}
