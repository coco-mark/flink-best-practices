package coco.flink.bp.template;

import coco.flink.bp.entity.DroppedEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author coco
 */
public class TemplateDistinctFunction<K,I> extends KeyedProcessFunction<K,I,I> {
    private final Time windowTime;
    private transient ValueState<Long> state;
    public static final OutputTag<DroppedEvent> droppedTag = new OutputTag<DroppedEvent>("dropped"){};

    public TemplateDistinctFunction(Time windowTime) {
        this.windowTime = windowTime;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getState(new ValueStateDescriptor<Long>("distinctState", LongSerializer.INSTANCE));
    }

    @Override
    public void processElement(I value, Context ctx, Collector<I> out) throws Exception {
        Long timestamp = state.value();
        Long curr = ctx.timestamp();
        if (timestamp == null || timestamp < curr) {
            long expireAt = (curr / windowTime.toMilliseconds() + 1) * windowTime.toMilliseconds();
            state.update(expireAt);
            out.collect(value);
        }else{
            ctx.output(droppedTag,new DroppedEvent(value));
        }
    }
}
