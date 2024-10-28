package com.cts.nanoMeteringGateway.devicemeteringpointconnections;

import com.cts.logger.BasicConfApp;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;

import java.util.List;

public class GetMeteringPointForDevice {
    private static final Logger logger = LoggerFactory.getLogger(BasicConfApp.class);
    private String meteringPointId;

    public GetMeteringPointForDevice(String deviceId){
        RestTemplate restTemplate = new RestTemplate();
        String url = "https://energiservice.prod.techyon.io/api/devicemeteringpointconnections";
        String apiKey = "6c58d115-6cac-4478-be5d-f08374411e4d";
        var headers = new HttpHeaders();
        headers.set("Authorization", apiKey);
        headers.setAccept(List.of(MediaType.APPLICATION_JSON));
        headers.setContentType(MediaType.parseMediaType("application/vnd.techyon.devicemeteringpointconnection-v1+json"));
        var entity = new HttpEntity<>(null, headers);
        ResponseEntity<String> response;
        try {
            response = restTemplate.exchange(url + "?device_id=" + deviceId, HttpMethod.GET, entity, String.class);
        } catch (Exception e) {
            logger.error("meter = {}: {}", deviceId, e.getMessage());
            return;
        }
        if (!response.getStatusCode().is2xxSuccessful()) {
            throw new RuntimeException(response.toString());
        }
        try {
            meteringPointId = JsonPath.read(response.getBody(), "$['meteringPointId']");
        } catch (PathNotFoundException e) {
            logger.info(e.getMessage());
        }
    }

    public String getMeteringPointId() {
        return meteringPointId;
    }
}
