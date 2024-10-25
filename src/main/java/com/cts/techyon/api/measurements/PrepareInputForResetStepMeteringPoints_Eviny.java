/**
 * This will prepare input file to run for creating json file for reset of 'Step' meters @ Eviny.
 * Un comment the extra tag for overwriting endIndex = 'true' in MeasurementJsonGeneratorForMBus class.
 * Run the input creation from the main() method here.
 * A file with the EAN;meter_id;Constant;Date (e.g. C:\Users\ronend\Desktop\evinyStepMeters.txt) is read.
 * The input file will be written to C:\Users\ronend\Desktop\Eviny-inputForStepMeters.txt.
 * Then place the file in C:\Users\ronend\Desktop\BKKE_BASE_FOLDER\MVV
 * Then copy the .json output from C:\Users\ronend\Desktop\techyon_In\measurements into Smart24 -> (Eviny Test 3part) P:\PROD\techyon_in\measurements for import into nM.
 */
package com.cts.techyon.api.measurements;

import com.cts.logger.BasicConfApp;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class PrepareInputForResetStepMeteringPoints_Eviny {

    private static Logger logger = LoggerFactory.getLogger(BasicConfApp.class);
    static RestTemplate restTemplate = new RestTemplate();
    private List<String[]> inputFromDB;
    public PrepareInputForResetStepMeteringPoints_Eviny() throws IOException {
        inputFromDB = new ArrayList<>();
        readFileIntoListOfStringArrays();
        printInputFile();
    }

    private void readFileIntoListOfStringArrays() throws IOException {
        FileInputStream fis = new FileInputStream(new File("C:\\Users\\ronend\\Desktop\\evinyStepMeters.txt"));
        BufferedReader inputStreamReader = new BufferedReader(new InputStreamReader(fis));
        String thisLine;
        int rowCounter = 0;
        while ((thisLine = inputStreamReader.readLine()) != null) {
            inputFromDB.add(thisLine.split(";"));
            if (!thisLine.trim().contains(";") || thisLine.length() < 1 ||
                    (!thisLine.matches(".*[0-9].*") && !thisLine.matches(".*[a-zA-Z].*"))){
            }
        }
        logger.info("Completed reading. Total rows read = {}.",rowCounter);
        System.out.println(inputFromDB.size());
    }

    private void printInputFile() throws IOException {
        BufferedWriter bw = null;
        FileWriter fw = null;
        try {
            DateTimeFormatter dbFormat = DateTimeFormat.forPattern("yyyy-MM-dd");
            DateTimeFormatter outputFormat = DateTimeFormat.forPattern("MM/dd/yyyy 00:00:00+01:00");
            fw = new FileWriter("C:\\Users\\ronend\\Desktop\\Eviny-inputForStepMeters.txt");
            bw = new BufferedWriter(fw);
            for (String[] sa : inputFromDB) {
                DateTime dbDate = dbFormat.parseDateTime(sa[3]);
                dbDate = dbDate.minusDays(1);
                String dateToApi = dbFormat.print(dbDate);
                bw.write(sa[1] + ";" + outputFormat.print(dbFormat.parseDateTime(sa[3])) + ";" + getIndexValue(sa[0],dateToApi,dateToApi) + ";Measured_50" +"\n");
            }
        }catch (IOException e){
            e.printStackTrace();
        }finally {
            try {
                if (bw != null)
                    bw.close();

                if (fw != null)
                    fw.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }

    private String getIndexValue(String meteringPointId,String from,String to) {
        String url = "https://eviny.prod.techyon.io/api/measurements";
        String apiKey = "ec9a3e94-bcd6-411b-a027-0ba3b6094af0";
        String payload = "meteringPointId=" + meteringPointId + "&from=" + from + "&to=" + to + "&sensorType=Step" + "&direction=Downstream&requestType=IndexesInPeriod";
        var headers = new HttpHeaders();
        headers.set("Authorization", apiKey);
        headers.setAccept(List.of(MediaType.APPLICATION_JSON));
        headers.setContentType(MediaType.parseMediaType("application/vnd.techyon.measurements-v1+json"));
        var entity = new HttpEntity<>(null, headers);
        ResponseEntity<String> response;
        try {
            response = restTemplate.exchange(url + "?" + payload, HttpMethod.GET, entity, String.class);
        } catch (Exception e){
            logger.error(e.getMessage());
            return null;
        }
        //System.out.println(response);
        if (!response.getStatusCode().is2xxSuccessful()) {
            throw new RuntimeException(response.toString());
        }
        try {
            var res = JsonPath.read(response.getBody(), "$['measurementMessages'][0]['indexes'][0]['dataPoint']['value']");
            return res.toString();
        } catch (PathNotFoundException e) {
            System.out.println(e.getMessage());
            return  null;
        }
    }


    public static void main(String[] args) throws IOException {
        PrepareInputForResetStepMeteringPoints_Eviny resetStepMeteringPoints = new PrepareInputForResetStepMeteringPoints_Eviny();
    }
}

