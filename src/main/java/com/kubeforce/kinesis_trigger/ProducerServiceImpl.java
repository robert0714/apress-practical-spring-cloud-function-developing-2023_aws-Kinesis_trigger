package com.kubeforce.kinesis_trigger;


import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.producer.Attempt;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordFailedException;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

@Service
public class ProducerServiceImpl implements ProducerService {

   // private static final Logger LOG = LoggerFactory.getLogger(ProducerServiceImpl.class);

    @Value(value = "${aws.stream_name}")
    private String streamName;

    @Value(value = "${aws.region}")
    private String awsRegion;

    @Value(value = "${aws.access_key}")
    private String awsAccessKey;

    @Value(value = "${aws.secret_key}")
    private String awsSecretKey;

    private KinesisProducer kinesisProducer = null;


    // The number of records that have finished (either successfully put, or failed)
    final AtomicLong completed = new AtomicLong(0);


    private static final String TIMESTAMP_AS_PARTITION_KEY =
            Long.toString(System.currentTimeMillis());


    public ProducerServiceImpl() {
        this.kinesisProducer = getKinesisProducer();
    }

    private KinesisProducer getKinesisProducer() {
        if (kinesisProducer == null) {

            BasicAWSCredentials awsCreds = new BasicAWSCredentials("AKIAZKRBTXXLPA3V2RW5", "kUqeO3bJyHEGziM09ru83/yU5vulbYagqHXmM4zG");

            KinesisProducerConfiguration config = new KinesisProducerConfiguration();
            config.setRegion("us-east-1");
            config.setCredentialsProvider(new AWSStaticCredentialsProvider(awsCreds));
            config.setMaxConnections(1);
            config.setRequestTimeout(6000); // 6 seconds
            config.setRecordMaxBufferedTime(5000); // 5 seconds

            kinesisProducer = new KinesisProducer(config);
        }

        return kinesisProducer;
    }

    @Override
    public void putDataIntoKinesis(String payload) throws Exception {

        FutureCallback<UserRecordResult> myCallback = new FutureCallback<UserRecordResult>() {

            @Override
            public void onFailure(Throwable t) {

                // If we see any failures, we will log them.
                int attempts = ((UserRecordFailedException) t).getResult().getAttempts().size() - 1;
                if (t instanceof UserRecordFailedException) {
                    Attempt last =
                            ((UserRecordFailedException) t).getResult().getAttempts().get(attempts);
                    if (attempts > 1) {
                        Attempt previous = ((UserRecordFailedException) t).getResult().getAttempts()
                                .get(attempts - 1);
                     /*   LOG.error(String.format(
                                "Failed to put record - %s : %s. Previous failure - %s : %s",
                                last.getErrorCode(), last.getErrorMessage(),
                                previous.getErrorCode(), previous.getErrorMessage()));*/
                    } else {
                      //  LOG.error(String.format("Failed to put record - %s : %s.",
                         //       last.getErrorCode(), last.getErrorMessage()));
                    }

                }
             //   LOG.error("Exception during put", t);
            }

            @Override
            public void onSuccess(UserRecordResult result) {

                long totalTime = result.getAttempts().stream()
                        .mapToLong(a -> a.getDelay() + a.getDuration()).sum();

              //  LOG.info("Data writing success. Total time taken to write data = {}", totalTime);

                completed.getAndIncrement();
            }
        };


        final ExecutorService callbackThreadPool = Executors.newCachedThreadPool();

        ByteBuffer data = null;

        try {
            data = ByteBuffer.wrap(payload.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        // wait until unfinished records are processed
        while (kinesisProducer.getOutstandingRecordsCount() > 1e4) {
            Thread.sleep(1);
        }

        // write data to Kinesis stream
        ListenableFuture<UserRecordResult> f =
                kinesisProducer.addUserRecord(streamName, TIMESTAMP_AS_PARTITION_KEY, data);

        Futures.addCallback(f, myCallback, callbackThreadPool);

    }

    @Override
    public void stop() {
        if (kinesisProducer != null) {
            kinesisProducer.flushSync();
            kinesisProducer.destroy();
        }

    }

}