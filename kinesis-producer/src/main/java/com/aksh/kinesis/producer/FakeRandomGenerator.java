package com.aksh.kinesis.producer;

import com.google.gson.Gson;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.Arrays;

@Component
public class FakeRandomGenerator implements IRandomGenerator{
    @Value("${FakeRandomGenerator.templateClass:com.aksh.model.fake.TradeData}")
    private String className;
    private Gson gson=new Gson();
    Class clazz;
    @PostConstruct
    void init() throws Exception{
        clazz= Class.forName(className);
    }
    @Override
    public String createPayload() throws Exception {
        return gson.toJson(clazz.newInstance() );
    }
}
