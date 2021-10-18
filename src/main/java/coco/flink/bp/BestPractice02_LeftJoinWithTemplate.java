package coco.flink.bp;

import coco.flink.bp.entity.UserAction;
import coco.flink.bp.source.CollectionSource;
import coco.flink.bp.template.TemplateLeftJoinFunction;
import coco.flink.bp.template.Templates;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.Arrays;

/**
 * @author coco
 */
public class BestPractice02_LeftJoinWithTemplate {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60_000);
        env.setParallelism(1);

        DataStream<UserAction> source = env
                .addSource(CollectionSource.of(Arrays.asList(
                        new UserAction(1, "2021-01-01 10:00:00", "view"),
                        new UserAction(1, "2021-01-01 10:00:20", "view"),
                        new UserAction(1, "2021-01-01 10:00:25", "click"),
                        new UserAction(2, "2021-01-01 10:01:15", "click"),
                        new UserAction(1, "2021-01-01 10:00:14", "click"),
                        new UserAction(3, "2021-01-01 10:02:15", "view"),
                        new UserAction(4, "2021-01-01 10:02:20", "view")
                ), 1000), TypeInformation.of(UserAction.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<UserAction>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((event, timestamp) -> event.eventTimestamp));

        SingleOutputStreamOperator<Tuple2<UserAction, UserAction>> resStream = Templates.leftJoin(source
                .filter(val -> "view".equals(val.type))
                .keyBy(new KeySelector<UserAction, Long>() {
                    @Override
                    public Long getKey(UserAction value) throws Exception {
                        return value.reqId;
                    }
                }), source
                .filter(val -> "click".equals(val.type))
                .keyBy(new KeySelector<UserAction, Long>() {
                    @Override
                    public Long getKey(UserAction value) throws Exception {
                        return value.reqId;
                    }
                }), Time.seconds(60), null);

        resStream.print("res");
        resStream.getSideOutput(TemplateLeftJoinFunction.latenessTag).print("lateness");
        resStream.getSideOutput(TemplateLeftJoinFunction.joinFailedTag).print("joinFailed");

        env.execute("left join job");
    }
}
