package com.aksh.kinesis.producer;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

@Component
public  class RandomGeneratorFactory implements FactoryBean<IRandomGenerator> {
    @Value("${randomizationType:RandomGenerator}")
    String randomizationType;
    @Autowired
    private RandomGenerator randomGenerator;
    @Autowired
    private FakeRandomGenerator fakeRandomGenerator;
    @Override
    public boolean isSingleton() {
        return true;
    }

    @Override
    public IRandomGenerator getObject() throws Exception {
        if("RandomGenerator".equalsIgnoreCase(randomizationType)){
            return randomGenerator;
        }else{
            return fakeRandomGenerator;
        }
    }

    @Override
    public Class<?> getObjectType() {
        if("RandomGenerator".equalsIgnoreCase(randomizationType)){
            return RandomGenerator.class;
        }else{
            return FakeRandomGenerator.class;
        }
    }
}