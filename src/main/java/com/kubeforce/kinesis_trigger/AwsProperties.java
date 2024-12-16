package com.kubeforce.kinesis_trigger;
 

import org.springframework.boot.context.properties.ConfigurationProperties; 

/**
 * -Dfile.encoding=UTF-8
 */ 
@ConfigurationProperties(prefix = "aws", ignoreInvalidFields = true)
public class AwsProperties {
	private String streamName;
	private String accessKey;
	private String secretKey;
	private String region;
	public String getStreamName() {
		return streamName;
	}
	public void setStreamName(String streamName) {
		this.streamName = streamName;
	}
	public String getAccessKey() {
		return accessKey;
	}
	public void setAccessKey(String accessKey) {
		this.accessKey = accessKey;
	}
	public String getSecretKey() {
		return secretKey;
	}
	public void setSecretKey(String secretKey) {
		this.secretKey = secretKey;
	}
	public String getRegion() {
		return region;
	}
	public void setRegion(String region) {
		this.region = region;
	}
	
}
