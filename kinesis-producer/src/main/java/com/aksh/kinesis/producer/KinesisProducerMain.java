/**
 * 
 */
package com.aksh.kinesis.producer;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;

import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagementClientBuilder;
import com.amazonaws.services.simplesystemsmanagement.model.ParameterType;
import com.amazonaws.services.simplesystemsmanagement.model.PutParameterRequest;

import software.amazon.awssdk.regions.Region;

/**
 * Sample Amazon Kinesis Application.
 */
@SpringBootApplication
public class KinesisProducerMain {

	@Value("${region:us-east-1}")
	private String regionString;

	public static void main(String[] args) {
		SpringApplication.run(KinesisProducerMain.class, args);
	}

	@Bean
	public Region region() {
//		AWSSimpleSystemsManagementClientBuilder.defaultClient().getParameter(new GetParameterRequest().withName("name"))
//		.getParameter().getValue();
//		
//		AWSSimpleSystemsManagementClientBuilder.defaultClient().putParameter(new PutParameterRequest().withName("").withType(ParameterType.String).withOverwrite(true)))
		return Region.of(regionString);

		
	}

	@Bean
	public CommandLineRunner commandLineRunner(ApplicationContext ctx) {
		return args -> {

		};
	}
}
