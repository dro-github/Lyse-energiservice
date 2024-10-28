package com.cts.nanoMeteringGateway.devicemeteringpointconnections;

import com.cts.logger.BasicConfApp;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;

import java.util.List;

public class GetUnitForMeteringPoint {
    private static final Logger logger = LoggerFactory.getLogger(BasicConfApp.class);

    private String unit;

    public GetUnitForMeteringPoint(String meteringPointId){
        RestTemplate restTemplate = new RestTemplate();
        String url = "https://energiservice.prod.techyon.io/api/meteringpoints/" + meteringPointId + "/sensors";
        String apiKey = "6c58d115-6cac-4478-be5d-f08374411e4d";
        var headers = new HttpHeaders();
        headers.set("Authorization", apiKey);
        headers.setAccept(List.of(MediaType.APPLICATION_JSON));
        headers.setContentType(MediaType.parseMediaType("application/vnd.techyon.sensors-v1+json"));
        var entity = new HttpEntity<>(null, headers);
        ResponseEntity<String> response;
        try {
            response = restTemplate.exchange(url, HttpMethod.GET, entity, String.class);
        } catch (Exception e) {
            logger.error("meter = {}: {}", meteringPointId, e.getMessage());
            return;
        }
        try {
            String sensorType = JsonPath.read(response.getBody(), "$[0]['sensorType']");
            switch (sensorType){
                case "Volume":
                    unit = "M3";
                    break;
                case "ActiveEnergy":
                    unit = "kWh";
                    break;
                case "Step":
                    unit = "Step";
            }
        } catch (PathNotFoundException e) {
            logger.info(e.getMessage());
        }
    }

    public String getUnit() {
        return unit;
    }
}
