package coco.flink.bp.entity;

import com.alibaba.fastjson.JSON;

/**
 * @author coco
 */
public class JoinFailedEvent {
    private final Object event;
    private final boolean isLeft;

    public JoinFailedEvent(Object event, boolean isLeft) {
        this.event = event;
        this.isLeft = isLeft;
    }

    public Object getEvent() {
        return event;
    }

    public boolean isLeft() {
        return isLeft;
    }
    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
