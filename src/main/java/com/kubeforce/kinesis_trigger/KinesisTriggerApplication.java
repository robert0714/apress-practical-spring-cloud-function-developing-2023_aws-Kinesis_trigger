package com.kubeforce.kinesis_trigger;

import java.util.function.Function;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class KinesisTriggerApplication {

    public static void main(String[] args) {
        SpringApplication.run(KinesisTriggerApplication.class, args);
    }
    @Bean
	public Function<TrackDetail,String> producerFunction() {
		return new ProducerFunction();
	}
    @Bean
	public AwsProperties awsProperties() {
		return new AwsProperties();
	}
    @Bean
    public ProducerService producerServiceImpl(AwsProperties awsProperties) {
		return new ProducerServiceImpl(awsProperties);
	}
    
}
