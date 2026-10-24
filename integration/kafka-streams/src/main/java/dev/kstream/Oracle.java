package dev.kstream;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;

/** Independent executable oracle using only Kafka Streams' public DSL. */
public final class Oracle {
    private static String nullable(String s) { return s.equals("~") ? null : s; }
    private static String field(String s) { return s == null ? "~" : s; }
    public static void main(String[] args) throws Exception {
        if (args.length != 3) throw new IllegalArgumentException("scenario input.tsv output.tsv required");
        String scenario = args[0];
        StreamsBuilder b = new StreamsBuilder();
        var strings = Consumed.with(Serdes.String(), Serdes.String());
        var produced = Produced.with(Serdes.String(), Serdes.String());
        // Declare the table before the stream to match the logical Go graph.
        var table = scenario.equals("join") || scenario.equals("join-null") || scenario.equals("left-join") || scenario.equals("table") || scenario.startsWith("table-")
                ? b.table("table", strings, Materialized.as("lookup")) : null;
        if (scenario.equals("table") || (scenario.startsWith("table-") && !scenario.equals("table-transform-join"))) {
            switch (scenario) {
                case "table" -> table.toStream().to("out", produced);
                case "table-filter" -> table.filter((k,v) -> !v.isEmpty(), Materialized.as("filtered")).toStream().to("out", produced);
                case "table-filter-not" -> table.filterNot((k,v) -> !v.isEmpty(), Materialized.as("filtered")).toStream().to("out", produced);
                case "table-mapvalues" -> table.mapValues((k,v) -> v.equals("!null-output") ? null : (long)(k.length()+v.length()),
                        Materialized.with(Serdes.String(), Serdes.Long()))
                        .toStream().mapValues(v -> v == null ? null : v.toString()).to("out", produced);
                default -> throw new IllegalArgumentException("unknown table scenario: " + scenario);
            }
        } else {
            var source = b.stream("in", strings);
            switch (scenario) {
                case "filter" -> source.filter((k,v) -> v != null && !v.isEmpty()).to("out", produced);
                case "filter-not" -> source.filterNot((k,v) -> v != null && !v.isEmpty()).to("out", produced);
                case "branch", "branch-default" -> {
                    var split = source.split(Named.as("routes-"))
                            .branch((k,v) -> v != null && v.isEmpty(), Branched.as("empty"))
                            .branch((k,v) -> k == null || k.equals("a"), Branched.as("a-or-null"));
                    var branches = scenario.equals("branch-default")
                            ? split.defaultBranch(Branched.as("other")) : split.noDefaultBranch();
                    branches.get("routes-empty").mapValues(v -> "0:" + (v == null ? "null" : v)).to("out", produced);
                    branches.get("routes-a-or-null").mapValues(v -> "1:" + (v == null ? "null" : v)).to("out", produced);
                    if (scenario.equals("branch-default")) {
                        branches.get("routes-other").mapValues(v -> "2:" + (v == null ? "null" : v)).to("out", produced);
                    }
                }
                case "mapvalues" -> source.mapValues(v -> v == null ? "null" : v + "!").to("out", produced);
                case "flatmapvalues" -> source.flatMapValues(v -> Arrays.asList(v, v == null ? null : v + "2")).to("out", produced);
                case "flatmap" -> source.flatMap((k,v) -> Arrays.asList(KeyValue.pair(k,v), KeyValue.pair(k == null ? null : k + "2",v))).to("out", produced);
                case "merge" -> source.merge(b.stream("right",strings)).to("out", produced);
                case "metadata", "metadata-fanout" -> {
                    var merged = source.merge(b.stream("right", strings));
                    if (scenario.equals("metadata-fanout")) merged = merged.flatMapValues(v -> Arrays.asList(v, v));
                    merged.process(() -> new ContextualProcessor<String,String,String,String>() {
                        @Override public void process(Record<String,String> record) {
                            var meta = context().recordMetadata().orElseThrow();
                            context().forward(record.withValue(meta.topic() + ":" + meta.partition() + ":" + meta.offset() + ":" + (record.value() == null ? "null" : record.value())));
                        }
                    }).to("out", produced);
                }
                case "process-typed", "process-typed-fanout" -> source.merge(b.stream("right", strings))
                        .process(() -> new ContextualProcessor<String,String,Long,Long>() {
                            @Override public void process(Record<String,String> record) {
                                if ("!skip".equals(record.value())) return;
                                var meta = context().recordMetadata().orElseThrow();
                                Long key = record.key() == null ? null : (long)record.key().getBytes(StandardCharsets.UTF_8).length;
                                Long value = record.value() == null ? null : (long)record.value().getBytes(StandardCharsets.UTF_8).length + meta.offset();
                                var out = new Record<Long,Long>(key, value, record.timestamp() + 1);
                                context().forward(out);
                                if (scenario.equals("process-typed-fanout")) {
                                    context().forward(out.withValue(value == null ? null : value + 1).withTimestamp(record.timestamp() + 2));
                                }
                            }
                        }).map((k,v) -> KeyValue.pair(k == null ? null : k.toString(), v == null ? null : v.toString()))
                        .to("out", produced);
                case "count" -> source.groupByKey().count(Materialized.as("counts"))
                        .toStream().mapValues(v -> v.toString()).to("out", produced);
                case "reduce" -> source.groupByKey().reduce((a,v) -> v.equals("!delete") ? null : a + ":" + v,
                        Materialized.as("reduced")).toStream().to("out", produced);
                case "aggregate", "aggregate-null" -> source.groupByKey().aggregate(
                        () -> scenario.equals("aggregate-null") ? null : 10L,
                        (k,v,a) -> v.equals("!delete") ? null : (a == null ? 1000L : a) + k.length() + v.length(),
                        Materialized.with(Serdes.String(), Serdes.Long()))
                        .toStream().mapValues(v -> v == null ? null : v.toString()).to("out", produced);
                case "join" -> source.join(table, (a,c) -> a + ":" + c).to("out", produced);
                case "join-null" -> source.join(table, (a,c) -> a.equals("!null-output") ? null : a + ":" + c).to("out", produced);
                case "left-join" -> source.leftJoin(table, (a,c) -> a.equals("!null-output") ? null : a + ":" + (c == null ? "missing" : c)).to("out", produced);
                case "table-transform-join" -> source.leftJoin(
                        table.filter((k,v) -> !v.isEmpty(), Materialized.as("filtered"))
                                .mapValues(v -> v.equals("!null-output") ? null : v + "!", Materialized.as("mapped")),
                        (a,c) -> a + ":" + (c == null ? "missing" : c)).to("out", produced);
                case "window" -> source.groupByKey()
                        .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofMillis(10), Duration.ofMillis(5)))
                        .count(Materialized.as("windows")).toStream()
                        .map((k,v) -> KeyValue.pair(k.key() + "@" + k.window().start() + ":" + k.window().end(), v.toString()))
                        .to("out", produced);
                case "hopping", "hopping-uneven" -> source.groupByKey()
                        .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofMillis(10), Duration.ofMillis(5))
                                .advanceBy(Duration.ofMillis(scenario.equals("hopping") ? 5 : 6)))
                        .count(Materialized.as("windows")).toStream()
                        .map((k,v) -> KeyValue.pair(k.key() + "@" + k.window().start() + ":" + k.window().end(), v.toString()))
                        .to("out", produced);
                default -> throw new IllegalArgumentException("unknown scenario: " + scenario);
            }
        }
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kstream-go-oracle");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "unused:9092");
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        Path state = Files.createTempDirectory("kstream-java-state-");
        props.put(StreamsConfig.STATE_DIR_CONFIG, state.toString());
        try (var driver = new TopologyTestDriver(b.build(), props)) {
            HashMap<String, TestInputTopic<String,String>> inputs = new HashMap<>();
            for (String topic : scenario.equals("merge") || scenario.equals("metadata") || scenario.equals("metadata-fanout") || scenario.startsWith("process-typed") ? List.of("in", "right")
                    : scenario.equals("join") || scenario.equals("join-null") || scenario.equals("left-join") || scenario.equals("table-transform-join") ? List.of("table", "in")
                    : scenario.equals("table") || scenario.startsWith("table-") ? List.of("table") : List.of("in")) {
                inputs.put(topic, driver.createInputTopic(topic, new StringSerializer(), new StringSerializer()));
            }
            var output = driver.createOutputTopic("out", new StringDeserializer(), new StringDeserializer());
            StringBuilder result = new StringBuilder();
            for (String line : Files.readAllLines(Path.of(args[1]))) {
                if (line.isEmpty() || line.startsWith("#")) continue;
                String[] f = line.split("\t", -1);
                if (f.length != 4) throw new IllegalArgumentException("invalid trace row");
                inputs.get(f[0]).pipeInput(nullable(f[2]), nullable(f[3]), Long.parseLong(f[1]));
                while (!output.isEmpty()) {
                    var r = output.readRecord();
                    result.append("out\t").append(field(r.key())).append('\t')
                            .append(field(r.value())).append('\t').append(r.timestamp()).append('\n');
                }
            }
            Files.writeString(Path.of(args[2]), result.toString());
        } finally {
            try (var paths = Files.walk(state)) {
                for (Path p : paths.sorted(java.util.Comparator.reverseOrder()).toList()) Files.deleteIfExists(p);
            }
        }
    }
}
