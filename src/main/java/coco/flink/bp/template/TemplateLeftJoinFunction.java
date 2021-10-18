package coco.flink.bp.template;

import coco.flink.bp.entity.BufferEntry;
import coco.flink.bp.entity.JoinFailedEvent;
import coco.flink.bp.entity.LatenessEvent;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author coco
 */
public class TemplateLeftJoinFunction<K, L, R> extends KeyedCoProcessFunction<K, L, R, Tuple2<L, R>> {
    private static final Time ZERO = Time.milliseconds(0);
    private final Time windowTime;
    private final Time allowLateness;
    private final TypeSerializer<L> leftSer;
    private final TypeSerializer<R> rightSer;

    /**
     * windowEnd -> left event list
     */
    private transient MapState<Long, List<BufferEntry<L>>> leftState;
    /**
     * windowEnd -> right event list
     */
    private transient MapState<Long, List<BufferEntry<R>>> rightState;
    public static final OutputTag<LatenessEvent> latenessTag = new OutputTag<LatenessEvent>("lateness") {
    };
    public static final OutputTag<JoinFailedEvent> joinFailedTag = new OutputTag<JoinFailedEvent>("joinFailed") {
    };

    public TemplateLeftJoinFunction(Time windowTime, Time allowLateness, TypeSerializer<L> leftSer, TypeSerializer<R> rightSer) {
        this.windowTime = windowTime;
        if (allowLateness == null) {
            this.allowLateness = ZERO;
        } else {
            this.allowLateness = allowLateness;
        }
        this.leftSer = leftSer;
        this.rightSer = rightSer;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        leftState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("leftState",
                        LongSerializer.INSTANCE, new ListSerializer<>(new BufferEntry.BufferEntrySerializer<>(leftSer))));

        rightState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("rightState",
                        LongSerializer.INSTANCE, new ListSerializer<>(new BufferEntry.BufferEntrySerializer<>(rightSer))));

    }

    @Override
    public void processElement1(L value, Context ctx, Collector<Tuple2<L, R>> out) throws Exception {
        Long curr = ctx.timestamp();
        long wm = ctx.timerService().currentWatermark();

        if (isLate(curr, wm)) {
            // side out lateness events
            ctx.output(latenessTag, new LatenessEvent(value, curr, wm));
            return;
        }

        boolean hasJoined = false;
        for (Map.Entry<Long, List<BufferEntry<R>>> entry : rightState.entries()) {
            Long timestamp = entry.getKey();
            if (timestamp + allowLateness.toMilliseconds() < wm) {
                // expired window data
                continue;
            }

            for (BufferEntry<R> r : entry.getValue()) {
                hasJoined = true;
                r.joined();
                out.collect(Tuple2.of(value, r.getElement()));
            }

            rightState.put(timestamp, entry.getValue());
        }

        // add event
        long windowEnd = (curr / windowTime.toMilliseconds() + 1) * windowTime.toMilliseconds();
        List<BufferEntry<L>> list = leftState.get(windowEnd);
        if (list == null) {
            list = new ArrayList<>();
        }
        list.add(new BufferEntry<>(value, hasJoined));
        leftState.put(windowEnd, list);

        // register next timer
        ctx.timerService().registerEventTimeTimer(windowEnd + allowLateness.toMilliseconds());
    }

    @Override
    public void processElement2(R value, Context ctx, Collector<Tuple2<L, R>> out) throws Exception {
        Long curr = ctx.timestamp();
        long wm = ctx.timerService().currentWatermark();

        if (isLate(curr, wm)) {
            // side out lateness events
            ctx.output(latenessTag, new LatenessEvent(value, curr, wm));
            return;
        }

        boolean hasJoined = false;
        for (Map.Entry<Long, List<BufferEntry<L>>> entry : leftState.entries()) {
            Long timestamp = entry.getKey();
            if (timestamp + allowLateness.toMilliseconds() < wm) {
                // expired window data
                continue;
            }

            for (BufferEntry<L> l : entry.getValue()) {
                hasJoined = true;
                l.joined();
                out.collect(Tuple2.of(l.getElement(), value));
            }

            leftState.put(timestamp, entry.getValue());
        }

        // add event
        long windowEnd = (curr / windowTime.toMilliseconds() + 1) * windowTime.toMilliseconds();
        List<BufferEntry<R>> list = rightState.get(windowEnd);
        if (list == null) {
            list = new ArrayList<>();
        }
        list.add(new BufferEntry<>(value, hasJoined));
        rightState.put(windowEnd, list);

        // register next timer
        ctx.timerService().registerEventTimeTimer(windowEnd + allowLateness.toMilliseconds());
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<L, R>> out) throws Exception {
        long windowEnd = timestamp - allowLateness.toMilliseconds();
        List<BufferEntry<L>> leftList = leftState.get(windowEnd);
        List<BufferEntry<R>> rightList = rightState.get(windowEnd);

        // remove expired window
        leftState.remove(windowEnd);
        rightState.remove(windowEnd);

        System.out.println("key: " + ctx.getCurrentKey() + ", left state size: " + sizeOf(leftState));
        System.out.println("key: " + ctx.getCurrentKey() + ", right state size: " + sizeOf(rightState));

        // side output join failed events
        if (rightList != null) {
            for (BufferEntry<R> r : rightList) {
                if (!r.hasBeenJoined()) {
                    ctx.output(joinFailedTag, new JoinFailedEvent(r.getElement(), false));
                }
            }
        }

        // output empty join
        if (leftList != null) {
            for (BufferEntry<L> l : leftList) {
                if (!l.hasBeenJoined()) {
                    out.collect(Tuple2.of(l.getElement(), null));
                }
            }
        }
    }

    private static <K, V> int sizeOf(MapState<K, V> mapState) throws Exception {
        int size = 0;
        for (K key : mapState.keys()) {
            size++;
        }
        return size;
    }

    private boolean isLate(Long curr, long watermark) {
        return watermark != Long.MIN_VALUE && (curr + allowLateness.toMilliseconds()) < watermark;
    }
}
