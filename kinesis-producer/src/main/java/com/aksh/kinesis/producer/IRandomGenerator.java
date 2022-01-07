package com.aksh.kinesis.producer;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.annotation.Value;

import java.io.IOException;

public interface IRandomGenerator {
    public String createPayload() throws Exception;
}
