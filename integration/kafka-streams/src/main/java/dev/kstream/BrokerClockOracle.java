package dev.kstream;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;

/** Verify task clock boundaries in live Kafka Streams, independently of TTD. */
public final class BrokerClockOracle {
    private static KStream<String,String> clock(KStream<String,String> input) {
        return input.process(() -> new ContextualProcessor<String,String,String,String>() {
            @Override public void process(Record<String,String> record) {
                context().forward(record.withValue(Long.toString(context().currentStreamTimeMs())));
            }
        });
    }

    private static KafkaConsumer<String,String> reader(String brokers, String application, String topic) {
        var config = new Properties();
        config.put("bootstrap.servers", brokers);
        config.put("group.id", application + "-observer");
        config.put("enable.auto.commit", false);
        config.put("isolation.level", "read_committed");
        config.put("auto.offset.reset", "earliest");
        var reader = new KafkaConsumer<>(config, new StringDeserializer(), new StringDeserializer());
        reader.assign(List.of(new TopicPartition(topic, 0)));
        reader.seekToBeginning(reader.assignment());
        return reader;
    }

    private static void await(KafkaConsumer<String,String> reader, String key, long clock, AtomicReference<Throwable> failure) {
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (System.nanoTime() < deadline) {
            if (failure.get() != null) throw new IllegalStateException("live Kafka Streams worker failed", failure.get());
            var records = reader.poll(Duration.ofMillis(100));
            if (records.isEmpty()) continue;
            if (records.count() != 1) throw new IllegalStateException("unexpected extra committed clock output");
            var record = records.iterator().next();
            if (!Objects.equals(record.key(), key) || !Long.toString(clock).equals(record.value())) {
                throw new IllegalStateException("clock boundary mismatch: " + record + "; expected key=" + key + " clock=" + clock);
            }
            return;
        }
        throw new IllegalStateException("timed out waiting for committed clock " + clock);
    }

    private static void send(KafkaProducer<String,String> producer, String topic, String key, long stamp) throws Exception {
        producer.send(new ProducerRecord<>(topic, 0, stamp, key, "x")).get(30, TimeUnit.SECONDS);
    }

    private static void verify(String brokers, boolean disconnected) throws Exception {
        var application = "clock-live-" + UUID.randomUUID();
        var input = application + "-in";
        var other = application + "-other";
        var output = application + "-out";
        var acknowledgments = application + "-ack";
        var state = Files.createTempDirectory("kstream-live-clock-");
        var adminConfig = new Properties();
        adminConfig.put("bootstrap.servers", brokers);
        try (var admin = Admin.create(adminConfig)) {
            var topics = List.of(input, other, output, acknowledgments);
            try {
                admin.createTopics(topics.stream().map(t -> new NewTopic(t, 1, (short)1)).toList()).all().get(30, TimeUnit.SECONDS);
                var builder = new StreamsBuilder();
                var strings = Consumed.with(Serdes.String(), Serdes.String());
                var produced = Produced.with(Serdes.String(), Serdes.String());
                var events = builder.stream(input, strings);
                if (disconnected) {
                    clock(events).to(output, produced);
                    clock(builder.stream(other, strings)).to(acknowledgments, produced);
                } else {
                    // The committed ack proves upstream time reached 10000 before
                    // we send the late record to the downstream task.
                    clock(events).to(acknowledgments, produced);
                    clock(events.repartition(Repartitioned.with(Serdes.String(), Serdes.String())
                            .withName("exchange").withNumberOfPartitions(1))).to(output, produced);
                }
                var topology = builder.build();
                if (topology.describe().subtopologies().size() != 2) throw new IllegalStateException("expected two live task components");
                var config = new Properties();
                config.put(StreamsConfig.APPLICATION_ID_CONFIG, application);
                config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
                config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
                config.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
                config.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
                config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10);
                config.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
                config.put(StreamsConfig.STATE_DIR_CONFIG, state.toString());
                config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
                config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
                var failure = new AtomicReference<Throwable>();
                var streams = new KafkaStreams(topology, config);
                streams.setUncaughtExceptionHandler(error -> {
                    failure.set(error);
                    return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
                });
                var producerConfig = new Properties();
                producerConfig.put("bootstrap.servers", brokers);
                producerConfig.put("acks", "all");
                producerConfig.put("enable.idempotence", true);
                try (var producer = new KafkaProducer<>(producerConfig, new StringSerializer(), new StringSerializer());
                     var out = reader(brokers, application, output);
                     var ack = reader(brokers, application, acknowledgments)) {
                    streams.start();
                    send(producer, input, "start", 9);
                    await(out, "start", 9, failure);
                    if (!disconnected) await(ack, "start", 9, failure);
                    send(producer, disconnected ? other : input, null, 10000);
                    await(ack, null, 10000, failure);
                    send(producer, input, "late", 3);
                    await(out, "late", 9, failure);
                    if (!disconnected) await(ack, "late", 10000, failure);
                    System.out.println("PASS live Kafka Streams clock: " + (disconnected ? "disconnected sources" : "repartition stages")
                            + "; upstream/unrelated=10000; downstream=9; EOS v2; read-committed outputs");
                } finally {
                    if (!streams.close(Duration.ofSeconds(20))) throw new IllegalStateException("clock worker did not close");
                }
            } finally {
                var owned = admin.listTopics().names().get(30, TimeUnit.SECONDS).stream().filter(t -> t.startsWith(application + "-")).toList();
                if (!owned.isEmpty()) admin.deleteTopics(owned).all().get(30, TimeUnit.SECONDS);
            }
        } finally {
            try (var paths = Files.walk(state)) {
                for (var path : paths.sorted(Comparator.reverseOrder()).toList()) Files.deleteIfExists(path);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) throw new IllegalArgumentException("broker address required");
        verify(args[0], true);
        verify(args[0], false);
    }
}
