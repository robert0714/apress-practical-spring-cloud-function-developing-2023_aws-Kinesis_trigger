package com.kubeforce.kinesis_trigger;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import java.util.function.Function;

public class ProducerFunction implements Function<TrackDetail,String> {
    @Autowired
    private ProducerService producerService;
    @Override
    public String apply(TrackDetail trackDetail) {
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
        return "Saved data into Kinesis successfully!";
    }
}