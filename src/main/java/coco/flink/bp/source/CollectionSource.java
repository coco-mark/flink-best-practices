package coco.flink.bp.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Collection;

/**
 * @author coco
 */
public class CollectionSource<T> implements SourceFunction<T> {

    private final Collection<T> elements;
    private final long delay;

    public CollectionSource(Collection<T> elements, long delay) {
        this.elements = elements;
        this.delay = delay;
    }

    public static <T> CollectionSource<T> of(Collection<T> elements, long delay) {
        return new CollectionSource<>(elements,delay);
    }

    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        for (T element : elements) {
            ctx.collect(element);
            Thread.sleep(delay);
        }

        Thread.sleep(delay);
    }

    @Override
    public void cancel() {

    }
}
