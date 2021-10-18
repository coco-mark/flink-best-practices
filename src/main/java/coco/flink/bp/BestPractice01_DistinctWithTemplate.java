package coco.flink.bp;

import coco.flink.bp.entity.UserAction;
import coco.flink.bp.source.CollectionSource;
import coco.flink.bp.template.TemplateDistinctFunction;
import coco.flink.bp.template.Templates;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.Arrays;

/**
 * @author coco
 */
public class BestPractice01_DistinctWithTemplate {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60_000);
        env.setParallelism(1);

        DataStream<UserAction> source = env
                .addSource(CollectionSource.of(Arrays.asList(
                        new UserAction(1, "2021-01-01 10:00:00", "click"),
                        new UserAction(1, "2021-01-01 10:00:20", "click"),
                        new UserAction(2, "2021-01-01 10:01:15", "click"),
                        new UserAction(3, "2021-01-01 10:01:20", "click"),
                        new UserAction(3, "2021-01-01 10:02:19", "click"),
                        new UserAction(4, "2021-01-01 10:02:20", "click")
                ), 1000), TypeInformation.of(UserAction.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<UserAction>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((event, timestamp) -> event.eventTimestamp));

        SingleOutputStreamOperator<UserAction> resStream = Templates.distinct(source.keyBy(new KeySelector<UserAction, Long>() {
            @Override
            public Long getKey(UserAction value) throws Exception {
                return value.reqId;
            }
        }), Time.seconds(60));

        resStream.print("res");
        resStream.getSideOutput(TemplateDistinctFunction.droppedTag).print("dropped");

        env.execute("distinct job");
    }
}
