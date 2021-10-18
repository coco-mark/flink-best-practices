package coco.flink.bp.entity;

import com.alibaba.fastjson.JSON;

/**
 * @author coco
 */
public class DroppedEvent {

    public final Object event;

    public DroppedEvent(Object event) {
        this.event = event;
    }
    public Object getEvent() {
        return event;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(event);
    }
}
