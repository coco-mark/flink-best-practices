package coco.flink.bp.template;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.time.Time;

import java.text.ParseException;

/**
 * @author coco
 */
public class Windows {

    public static long windowBeg(long curr, long windowSize, long stepSize) {
        return curr / stepSize * stepSize;
    }

    public static long windowEnd(long curr, long windowSize, long stepSize) {
        return curr / stepSize * stepSize + windowSize;
    }

    public static void main(String[] args) throws ParseException {
        System.out.printf("(%s, %s)\n",
                DateFormatUtils.format(windowBeg(DateUtils.parseDate("2021-01-01 10:03:00", "yyyy-MM-dd HH:mm:ss").getTime(), Time.minutes(10).toMilliseconds(), Time.minutes(2).toMilliseconds()), "yyyy-MM-dd HH:mm:ss"),
                DateFormatUtils.format(windowEnd(DateUtils.parseDate("2021-01-01 10:03:00", "yyyy-MM-dd HH:mm:ss").getTime(), Time.minutes(10).toMilliseconds(), Time.minutes(2).toMilliseconds()), "yyyy-MM-dd HH:mm:ss")
        );
        System.out.printf("(%s, %s)\n",
                DateFormatUtils.format(windowBeg(DateUtils.parseDate("2021-01-01 10:12:00", "yyyy-MM-dd HH:mm:ss").getTime(), Time.minutes(10).toMilliseconds(), Time.minutes(1).toMilliseconds()), "yyyy-MM-dd HH:mm:ss"),
                DateFormatUtils.format(windowEnd(DateUtils.parseDate("2021-01-01 10:12:00", "yyyy-MM-dd HH:mm:ss").getTime(), Time.minutes(10).toMilliseconds(), Time.minutes(1).toMilliseconds()), "yyyy-MM-dd HH:mm:ss")
        );
        System.out.printf("(%s, %s)\n",
                DateFormatUtils.format(windowBeg(DateUtils.parseDate("2021-01-01 10:45:00", "yyyy-MM-dd HH:mm:ss").getTime(), Time.minutes(40).toMilliseconds(), Time.minutes(40).toMilliseconds()), "yyyy-MM-dd HH:mm:ss"),
                DateFormatUtils.format(windowEnd(DateUtils.parseDate("2021-01-01 10:45:00", "yyyy-MM-dd HH:mm:ss").getTime(), Time.minutes(40).toMilliseconds(), Time.minutes(40).toMilliseconds()), "yyyy-MM-dd HH:mm:ss")
        );
    }
}
