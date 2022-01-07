package com.aksh.kinesis.producer;

import org.junit.jupiter.api.Test;

class RandomGeneratorTest {

	@Test
	void test() {
		System.out.println(RandomGenerator.generateRandom("RANDOM_FLOAT5"));
	}

}
