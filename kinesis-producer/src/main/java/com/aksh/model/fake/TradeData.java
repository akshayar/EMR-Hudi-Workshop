package com.aksh.model.fake;

import com.github.javafaker.Faker;

import java.util.Date;
import java.util.concurrent.TimeUnit;

public class TradeData {
    static Faker faker=new Faker();
    private Date dateTime=faker.date().past(2, TimeUnit.HOURS);
    private String tradeId=faker.numerify("##########");
    private String symbol=faker.regexify("[A-Z]{4}");
    private double quantity=faker.number().randomDouble(2,10,100);
    private double price=faker.number().randomDouble(2,10,100);;
    private long timestamp=dateTime.getTime();
    private String description=faker.regexify("This is a description [a-z1-9]{10}");
    private String traderName=faker.regexify("Trader [a-z1-9]{10}");
    private String traderFirm=faker.regexify("Trader Firm [a-z1-9]{10}");
    private String orderId=faker.numerify("##########");
    private String portfolioId=faker.numerify("##########");
    private String customerId=faker.numerify("##########");
    private boolean buy=faker.bool().bool();
    private String orderTimestamp=dateTime.getTime()+"";
    private String currentPosition=faker.number().digits(4);
    private double buyPrice=faker.number().randomDouble(2,10,100);
    private double sellPrice=faker.number().randomDouble(2,10,100);
    private double profit=sellPrice-buyPrice;
}
