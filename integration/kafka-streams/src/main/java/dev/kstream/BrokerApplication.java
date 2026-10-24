package dev.kstream;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

/** Public-API broker application; Kafka Streams 3.9.1 EOS v2 reference. */
public final class BrokerApplication {
    public static void main(String[] args) throws Exception {
        if (args.length != 6) throw new IllegalArgumentException("scenario brokers app input output state required");
        StreamsBuilder builder = new StreamsBuilder();
        var source = builder.stream(args[3], Consumed.with(Serdes.String(), Serdes.String()));
        switch(args[0]) {
            case "filter" -> source.filter((k,v) -> v != null && !v.isEmpty())
                    .to(args[4], Produced.with(Serdes.String(), Serdes.String()));
            case "count" -> source.groupByKey().count(Materialized.as("counts"))
                    .toStream().to(args[4], Produced.with(Serdes.String(), Serdes.Long()));
            default -> throw new IllegalArgumentException("unknown scenario");
        }
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, args[2]);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, args[1]);
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        config.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        config.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10);
        config.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        config.put(StreamsConfig.STATE_DIR_CONFIG, Path.of(args[5]).toString());
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        config.put(StreamsConfig.consumerPrefix("max.poll.records"), 4096);
        config.put(StreamsConfig.producerPrefix("linger.ms"), 0);
        config.put(StreamsConfig.producerPrefix("compression.type"), "none");
        CountDownLatch stopped = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        try (KafkaStreams streams = new KafkaStreams(builder.build(), config)) {
            streams.setUncaughtExceptionHandler(error -> {
                failure.set(error); stopped.countDown();
                return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
            });
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                streams.close(Duration.ofSeconds(10)); stopped.countDown();
            }));
            streams.start();
            stopped.await();
            if (failure.get() != null) throw new RuntimeException("stream worker failed", failure.get());
        }
    }
}
