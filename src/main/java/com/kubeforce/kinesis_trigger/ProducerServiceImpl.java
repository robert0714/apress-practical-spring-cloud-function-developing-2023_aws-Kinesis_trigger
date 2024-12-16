package com.kubeforce.kinesis_trigger;
 
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;  
 

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;
 
public class ProducerServiceImpl implements ProducerService {

    private static final Logger LOG = LoggerFactory.getLogger(ProducerServiceImpl.class); 
     
    private final AwsProperties props ;

    private final KinesisAsyncClient kinesisClient;


    // The number of records that have finished (either successfully put, or failed)
    final AtomicLong completed = new AtomicLong(0);

    private static final ExecutorService callbackThreadPool = Executors.newCachedThreadPool();


    public ProducerServiceImpl(final AwsProperties props) {   
    	this.props = props ;    	
        this.kinesisClient = createKinesisAsyncClient();
        
    }

    private KinesisAsyncClient createKinesisAsyncClient() {
    	String region = this.props.getRegion() ; 
    	String awsAccessKey = this.props.getAccessKey();
    	String awsSecretKey = this.props.getSecretKey();
    	
		LOG.info("aws AccessKey: {}", awsAccessKey);
		LOG.info("aws SecretKey: {}", awsSecretKey);
		LOG.info("aws region: {}", region);
    	
        return KinesisAsyncClient.builder()
                .region(Region.of(region))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(awsAccessKey, awsSecretKey)))
                .build();
    }

    @Override
    public void putDataIntoKinesis(String payload) throws Exception {
        ByteBuffer data = ByteBuffer.wrap(payload.getBytes(StandardCharsets.UTF_8));
        SdkBytes sdkData = SdkBytes.fromByteBuffer(data);
        PutRecordRequest request = PutRecordRequest.builder()
                .streamName(props.getStreamName())
                .partitionKey(Long.toString(System.currentTimeMillis()))
                .data(sdkData)
                .build();

        CompletableFuture<PutRecordResponse> future = this.kinesisClient.putRecord(request);


        future.whenCompleteAsync((response, exception) -> {
            if (exception != null) {
                LOG.error("Failed to put record into Kinesis", exception);
            } else {
                LOG.info("Successfully put record into Kinesis with Sequence Number: {}", response.sequenceNumber());
                completed.incrementAndGet();
            }
        }, callbackThreadPool);

    }

    @Override
    public void stop() {
        if (kinesisClient != null) {
            kinesisClient.close();
            callbackThreadPool.shutdown();
        }

    }

}