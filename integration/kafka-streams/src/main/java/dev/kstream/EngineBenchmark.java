package dev.kstream;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;

/** Exploratory memory-store driver benchmark, not a broker throughput claim. */
public final class EngineBenchmark {
    private static long sum = -3750763034362895579L;
    private static void hashByte(int v) { sum = (sum ^ (v & 255)) * 1099511628211L; }
    private static void hashText(String s) { for (int i=0; i<s.length(); i++) hashByte(s.charAt(i)); hashByte(0); }
    private static void hashLong(long v) { for (int shift=56; shift>=0; shift-=8) hashByte((int)(v >>> shift)); }
    private static long rss() throws Exception {
        for (String line : Files.readAllLines(Path.of("/proc/self/status"))) {
            if (line.startsWith("VmHWM:")) return Long.parseLong(line.trim().split("\\s+")[1]);
        }
        return -1;
    }
    public static void main(String[] args) throws Exception {
        String scenario = args[0]; int n = Integer.parseInt(args[1]); int warmup = Integer.parseInt(args[2]);
        StreamsBuilder builder = new StreamsBuilder();
        var source = builder.stream("in", Consumed.with(Serdes.String(), Serdes.String()));
        boolean count = scenario.equals("count");
        if (count) {
            source.groupByKey().count(Materialized.<String,Long>as(Stores.inMemoryKeyValueStore("counts"))
                    .withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()).withCachingDisabled().withLoggingDisabled())
                    .toStream().to("out", Produced.with(Serdes.String(), Serdes.Long()));
        } else if (scenario.equals("filter")) {
            source.filter((k,v) -> v != null && !v.isEmpty()).to("out", Produced.with(Serdes.String(), Serdes.String()));
        } else throw new IllegalArgumentException("unknown scenario");
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "benchmark");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "unused:9092");
        p.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        Path state = Files.createTempDirectory("kstream-benchmark-");p.put(StreamsConfig.STATE_DIR_CONFIG, state.toString());
        try (var driver = new TopologyTestDriver(builder.build(),p)) {
            TestInputTopic<String,String> input=driver.createInputTopic("in",new StringSerializer(),new StringSerializer());
            TestOutputTopic<String,String> strings=count?null:driver.createOutputTopic("out",new StringDeserializer(),new StringDeserializer());
            TestOutputTopic<String,Long> longs=count?driver.createOutputTopic("out",new StringDeserializer(),new LongDeserializer()):null;
            String[] keys=new String[64];for(int i=0;i<keys.length;i++)keys[i]=Integer.toString(i);
            long start=0;
            for(int i=0;i<warmup+n;i++) {
                if(i==warmup){sum=-3750763034362895579L;start=System.nanoTime();}
                input.pipeInput(keys[i%64],"value",(long)i);
                if(count){var r=longs.readRecord();hashText(r.key());hashLong(r.value());}
                else{var r=strings.readRecord();hashText(r.key());hashText(r.value());}
            }
            long elapsed=System.nanoTime()-start;
            System.out.printf("kafka,%s,%d,%d,%016x,%d%n",scenario,n,elapsed,sum,rss());
        } finally {
            try(var paths=Files.walk(state)){for(Path path:paths.sorted(Comparator.reverseOrder()).toList())Files.deleteIfExists(path);}
        }
    }
}
