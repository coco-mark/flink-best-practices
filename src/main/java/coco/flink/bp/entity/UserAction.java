package coco.flink.bp.entity;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.time.DateUtils;

import java.io.Serializable;
import java.text.ParseException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author coco
 */
public class UserAction implements Serializable {
    public long reqId;
    public long eventTimestamp;
    public String eventTime;
    public String type;
    public int id;

    final static AtomicInteger ids = new AtomicInteger(0);

    public UserAction(long reqId, String eventTime, String type) {
        this.reqId = reqId;
        this.eventTime = eventTime;
        try {
            this.eventTimestamp = DateUtils.parseDate(eventTime, "yyyy-MM-dd HH:mm:ss").getTime();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        this.type = type;
        this.id = ids.incrementAndGet();
    }

    public long getReqId() {
        return reqId;
    }

    public void setReqId(long reqId) {
        this.reqId = reqId;
    }

    public long getEventTimestamp() {
        return eventTimestamp;
    }

    public void setEventTimestamp(long eventTimestamp) {
        this.eventTimestamp = eventTimestamp;
    }

    public String getEventTime() {
        return eventTime;
    }

    public void setEventTime(String eventTime) {
        this.eventTime = eventTime;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
