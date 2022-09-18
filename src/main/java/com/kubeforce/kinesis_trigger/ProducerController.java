package com.kubeforce.kinesis_trigger;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;


@RestController
@RequestMapping(value = "/api")
public class ProducerController {
    @Autowired
    private ProducerService producerService;

    @PostMapping(value = "/stream")
    public ResponseEntity<String> putIntoKinesis(@RequestBody  TrackDetail
                                                         trackDetail) {
        ObjectMapper mapper = new ObjectMapper();
        String data = "";
        try {
            data = mapper.writeValueAsString(trackDetail);
            producerService.putDataIntoKinesis(data);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ResponseEntity.ok("Saved data into Kinessis sucessfully!");
    }

}