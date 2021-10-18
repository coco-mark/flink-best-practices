package coco.flink.bp.template;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

/**
 * @author coco
 */
public class Templates {

    /**
     * left join with timed-tumbling window
     */
    public static <K, L, R> SingleOutputStreamOperator<Tuple2<L, R>> leftJoin(KeyedStream<L, K> left, KeyedStream<R, K> right, Time window, Time lateness) {
        return left.connect(right)
                .keyBy(left.getKeySelector(), right.getKeySelector())
                .process(new TemplateLeftJoinFunction<>(window, lateness,
                        left.getType().createSerializer(left.getExecutionConfig()),
                        right.getType().createSerializer(right.getExecutionConfig())))
                .returns(new TupleTypeInfo<>(left.getType(), right.getType()));
    }

    public static <I, K> SingleOutputStreamOperator<I> distinct(KeyedStream<I, K> stream, Time window) {
        return stream.process(new TemplateDistinctFunction<>(window))
                .returns(stream.getType());
    }
}
