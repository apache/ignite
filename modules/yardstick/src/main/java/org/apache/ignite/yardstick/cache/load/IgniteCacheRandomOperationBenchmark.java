package org.apache.ignite.yardstick.cache.load;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.reflections.Reflections;
import org.reflections.scanners.ResourcesScanner;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by pyatkov-vd on 07.04.2016.
 */
public class IgniteCacheRandomOperationBenchmark extends IgniteAbstractBenchmark {

    public static final String LOAD_MODEL_PACKAGE = "org.apache.ignite.yardstick.cache.load.model";
    public static final int operations = Operation.values().length;

    private Map<IgniteCache, CacheFrame> availableCaches;

    private void searchCache() throws Exception {

        availableCaches = new HashMap<>();

        Ignite ignite = ignite();

        Class[] classes = loadModelClasses();

        for (String name: ignite.cacheNames()) {

            IgniteCache<Object, Object> cache = ignite.cache(name);

            Constructor keyConstructor = classes[nextRandom(classes.length)].getConstructor(Integer.class);
            Constructor valueConstructor = classes[nextRandom(classes.length)].getConstructor(Integer.class);
            CacheFrame cacheFrame = new CacheFrame();
            cacheFrame.setKeyConstructor(keyConstructor);
            cacheFrame.setValueConstructor(valueConstructor);
            CacheConfiguration configuration = cache.getConfiguration(CacheConfiguration.class);
            if (configuration.getAtomicityMode() == CacheAtomicityMode.TRANSACTIONAL) {
                cacheFrame.setTransactional(true);
            }
            availableCaches.put(cache, cacheFrame);
        }
    }

    private Class[] loadModelClasses() {
        List<ClassLoader> classLoadersList = new LinkedList<>();
        classLoadersList.add(ClasspathHelper.contextClassLoader());
        classLoadersList.add(ClasspathHelper.staticClassLoader());

        Reflections reflections = new Reflections(new ConfigurationBuilder()
            .setScanners(new SubTypesScanner(false), new ResourcesScanner())
            .setUrls(ClasspathHelper.forClassLoader(classLoadersList.toArray(new ClassLoader[0])))
            .filterInputsBy(new FilterBuilder().include(FilterBuilder.prefix(LOAD_MODEL_PACKAGE))));
        return reflections.getSubTypesOf(Object.class).toArray(new Class[] {});
    }

    private void preLoading() throws InstantiationException, IllegalAccessException,
        java.lang.reflect.InvocationTargetException {
        int preLoadRange = args.range()/2;
        for (Map.Entry<IgniteCache, CacheFrame> entry: availableCaches.entrySet())
            for (int i=0; i<preLoadRange; i++)
                entry.getKey().put(entry.getValue().getKeyConstructor().newInstance(i),
                    entry.getValue().getValueConstructor().newInstance(i));
    }

    @Override
    protected void init() throws Exception {
        super.init();
        searchCache();
        preLoading();
    }

    @Override
    public boolean test(Map<Object, Object> map) throws Exception {

        for (Map.Entry<IgniteCache, CacheFrame> entry: availableCaches.entrySet()) {
            switch (Operation.valueOf(nextRandom(operations))) {
                case PUT:
                    doPut(entry);
                    break;
                case PUTALL:
                    doPutAll(entry);
                    break;
                case GET:
                    doGet(entry);
                    break;
                case GETALL:
                    doGetAll(entry);
                    break;
                case INVOKE:
                    doInvoke(entry);
                    break;
                case INVOKEALL:
                    doInvokeAll(entry);
                    break;
                case REMOVE:
                    doRemove(entry);
                    break;
                case REMOVEALL:
                    doRemoveAll(entry);
                    break;
                case PUTIFABSENT:
                    doPutIfAbsent(entry);
                    break;
                case REPLACE:
                    doReplace(entry);
            }
        }
        return true;
    }

    private void doPut(Map.Entry<IgniteCache, CacheFrame> entry) throws Exception {
        int i = nextRandom(args.range()/2, args.range());
        entry.getKey().put(entry.getValue().getKeyConstructor().newInstance(i),
            entry.getValue().getValueConstructor().newInstance(i));
    }

    private void doPutAll(Map.Entry<IgniteCache, CacheFrame> entry) throws Exception {
        Map putMap = new HashMap(args.batch());
        for (int cnt=0; cnt<args.batch(); cnt++) {
            int i = nextRandom(args.range()/2, args.range());
            putMap.put(entry.getValue().getKeyConstructor().newInstance(i),
                entry.getValue().getValueConstructor().newInstance(i));
        }
        entry.getKey().putAll(putMap);
    }

    private void doGet(Map.Entry<IgniteCache, CacheFrame> entry) throws Exception {
        int i = nextRandom(args.range()/2);
        entry.getKey().get(entry.getValue().getKeyConstructor().newInstance(i));
    }

    private void doGetAll(Map.Entry<IgniteCache, CacheFrame> entry) throws Exception {
        Set keys = new HashSet(args.batch());
        for (int cnt=0; cnt<args.batch(); cnt++) {
            int i = nextRandom(args.range()/2);
            keys.add(entry.getValue().getKeyConstructor().newInstance(i));
        }
        entry.getKey().get(keys);
    }

    private void doInvoke(final Map.Entry<IgniteCache, CacheFrame> entry) throws Exception {
        final int i = nextRandom(args.range()/2);
        entry.getKey().invoke(entry.getValue().getKeyConstructor().newInstance(i), new EntryProcessor() {
                @Override public Object process(MutableEntry entry1, Object... arguments) throws EntryProcessorException {
                    try {
                        entry1.setValue(entry.getValue().getValueConstructor().newInstance(i + 1));
                    } catch (Exception e) {
                        throw new EntryProcessorException(e);
                    }
                    return null;
                }
            }
        );
    }

    private void doInvokeAll(final Map.Entry<IgniteCache, CacheFrame> entry) throws Exception {
        Set keys = new HashSet(args.batch());
        for (int cnt=0; cnt<args.batch(); cnt++) {
            int i = nextRandom(args.range()/2);
            keys.add(entry.getValue().getKeyConstructor().newInstance(i));
        }
        entry.getKey().invokeAll(keys, new EntryProcessor() {
                @Override public Object process(MutableEntry entry1, Object... arguments) throws EntryProcessorException {
                    try {
                        int i = nextRandom(args.range()/2);
                        entry1.setValue(entry.getValue().getValueConstructor().newInstance(i + 1));
                    } catch (Exception e) {
                        throw new EntryProcessorException(e);
                    }
                    return null;
                }
            }
        );
    }

    private void doRemove(Map.Entry<IgniteCache, CacheFrame> entry) throws Exception {
        int i = nextRandom(args.range()/2);
        entry.getKey().remove(entry.getValue().getKeyConstructor().newInstance(i));
    }

    private void doRemoveAll(Map.Entry<IgniteCache, CacheFrame> entry) throws Exception {
        Set keys = new HashSet(args.batch());
        for (int cnt=0; cnt<args.batch(); cnt++) {
            int i = nextRandom(args.range()/2);
            keys.add(entry.getValue().getKeyConstructor().newInstance(i));
        }
        entry.getKey().removeAll(keys);
    }


    private void doPutIfAbsent(Map.Entry<IgniteCache, CacheFrame> entry) throws Exception {
        int i = nextRandom(args.range());
        entry.getKey().putIfAbsent(entry.getValue().getKeyConstructor().newInstance(i),
            entry.getValue().getValueConstructor().newInstance(i));
    }

    private void doReplace(Map.Entry<IgniteCache, CacheFrame> entry) throws Exception {
        int i = nextRandom(args.range()/2);
        entry.getKey().replace(entry.getValue().getKeyConstructor().newInstance(i),
            entry.getValue().getValueConstructor().newInstance(i),
            entry.getValue().getValueConstructor().newInstance(i + 1));
    }

    static enum Operation {
        PUT,
        PUTALL,
        GET ,
        GETALL ,
        INVOKE,
        INVOKEALL,
        REMOVE,
        REMOVEALL,
        PUTIFABSENT,
        REPLACE;

        public static Operation valueOf(int num) {
            return values()[num];
        }
    }

}
