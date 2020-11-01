package io.confluent.developer.livestreams2;

import java.util.Arrays;
import java.util.List;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.stereotype.Component;

@SpringBootApplication
@EnableKafkaStreams
public class Livestreams2Application {

	@Bean
	NewTopic quotes() {
		return new NewTopic("quotes", 6, (short) 3);
	}

	@Bean
	NewTopic counts() {
		return new NewTopic("counts", 6, (short) 3);
	}

	public static void main(String[] args) {
		SpringApplication.run(Livestreams2Application.class, args);
	}

}

@Component
class Processor {

	@Autowired
	public void process(final StreamsBuilder builder) {
		// read some data
		// count words
		// write them to result topic

		final Serde<String> stringSerde = Serdes.String();

		KStream<String, Long> counts = builder.stream("quotes", Consumed.with(stringSerde, stringSerde))

				.flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))

				.groupBy(((key, value) -> value), Grouped.with(stringSerde, stringSerde))

				.count().toStream();

		counts.print(Printed.toSysOut());

		counts.to("counts");

	}
}
