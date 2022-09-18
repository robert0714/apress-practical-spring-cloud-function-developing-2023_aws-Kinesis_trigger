package com.kubeforce.kinesis_trigger;

public interface ProducerService {

    public void putDataIntoKinesis(String payload) throws Exception;
    public void stop();

}
