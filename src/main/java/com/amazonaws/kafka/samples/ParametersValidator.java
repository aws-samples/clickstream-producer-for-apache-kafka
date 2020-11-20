package com.amazonaws.kafka.samples;

import com.beust.jcommander.ParameterException;

class ParametersValidator {

    static void validate() throws ParameterException{
        if (KafkaClickstreamClient.saslscramEnable && KafkaClickstreamClient.mTLSEnable) {
            throw new ParameterException("Specify either --mTLSEnable (or -mtls) or --saslscramEnable (or -sse). Not both.");
        }
        if (KafkaClickstreamClient.saslscramEnable && (KafkaClickstreamClient.saslscramUser == null || KafkaClickstreamClient.saslscramUser.equalsIgnoreCase(""))) {
            throw new ParameterException("If parameter --saslscramEnable (or -sse) is specified, the parameter --saslscramUser (or -ssu) needs to be specified.");
        }
        if (!KafkaClickstreamClient.saslscramEnable && KafkaClickstreamClient.saslscramUser != null) {
            throw new ParameterException("If parameter --saslscramUser (or -ssu) is specified, the parameter --saslscramEnable (or -sse) needs to be specified.");
        }
        if (KafkaClickstreamClient.saslscramEnable && KafkaClickstreamClient.sslEnable) {
            throw new ParameterException("Specify either --sslEnable (or -ssl) or --saslscramEnable (or -sse). Not both.");
        }
    }
}
