import javafx.util.Pair;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.stream.StreamTransformer;

import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import java.net.URL;

public class Client {
    public static class StreamingCacheEntryProcessor implements CacheEntryProcessor<String, Pair<Integer, Integer>, Object> {

        @Override
        public Object process(MutableEntry<String, Pair<Integer, Integer>> entry, Object... arguments) throws EntryProcessorException {
            System.out.println("\n" +
                    "******************************\n" +
                    "* Client.CacheEntryProcessor *\n" +
                    "******************************\n");
            Pair<Integer, Integer> pair = entry.getValue() == null ? new Pair<>(0,0) : entry.getValue();
            entry.setValue(new Pair<>(pair.getKey() + 1, pair.getValue() + 1));
            return entry.getValue();
        }
    }

    public static class StreamTransformerCacheEntryProcessor extends StreamTransformer<String, Pair<Integer, Integer>> {

        @Override
        public Object process(MutableEntry<String, Pair<Integer, Integer>> entry, Object... arguments) throws EntryProcessorException {
            System.out.println("\n" +
                    "****************************\n" +
                    "* Client.StreamTransformer *\n" +
                    "****************************\n");
            Pair<Integer, Integer> pair = entry.getValue() == null ? new Pair<>(0,0) : entry.getValue();
            entry.setValue(new Pair<>(pair.getKey() + 1, pair.getValue() + 1));
            return entry.getValue();
        }
    }

    public static void main(String[] args) throws Exception {
        Ignition.setClientMode(true);
        URL resource = Client.class.getResource("example-ignite.xml");
        try (Ignite ignite = Ignition.start(resource)) {
            assert ignite.configuration().isClientMode();
            IgniteCache<String, Pair<Integer, Integer>> cache = ignite.getOrCreateCache("test");
            assert new Pair<>(0, 0).equals(cache.get("zero"));

            try (IgniteDataStreamer<String, Pair<Integer, Integer>> streamer = ignite.dataStreamer("test")) {
                streamer.allowOverwrite(true);
//                streamer.receiver(StreamTransformer.from(new StreamingCacheEntryProcessor()));
                streamer.receiver(new StreamTransformerCacheEntryProcessor());
                streamer.addData("nine", new Pair<>(5, 4));
            }
        }
    }
}
