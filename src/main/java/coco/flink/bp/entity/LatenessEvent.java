package coco.flink.bp.entity;

import com.alibaba.fastjson.JSON;

/**
 * @author coco
 */
public class LatenessEvent {
    private final Object event;
    private final long eventTime;
    private final long currWatermark;

    public LatenessEvent(Object event, long eventTime, long currWatermark) {
        this.event = event;
        this.eventTime = eventTime;
        this.currWatermark = currWatermark;
    }

    public Object getEvent() {
        return event;
    }

    public long getEventTime() {
        return eventTime;
    }

    public long getCurrWatermark() {
        return currWatermark;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
