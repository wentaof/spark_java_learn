package flume.mysql_source;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @author fengwentao@changjing.ai
 * @date 2022/6/24 15:28
 * @Version 1.0.0
 * @Description TODO
 */
public class SQLSource extends AbstractSource implements Configurable, PollableSource {
    //打印日志
    private static final Logger LOG = LoggerFactory.getLogger(SQLSource.class);
    //定义 sqlHelper
    private SQLSourceHelper sqlSourceHelper;

    public long getBackOffSleepIncrement() {
        return 0;
    }

    public long getMaxBackOffSleepInterval() {
        return 0;
    }

    public void configure(Context context) {
        try {
            //初始化
            sqlSourceHelper = new SQLSourceHelper(context);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    public Status process() throws EventDeliveryException {
        try {
            //查询数据表
            List<List<Object>> result =
                    sqlSourceHelper.executeQuery();
            //存放 event 的集合
            List<Event> events = new ArrayList<Event>();
            //存放 event 头集合
            HashMap<String, String> header = new HashMap<String, String>();
            //如果有返回数据，则将数据封装为 event
            if (!result.isEmpty()) {
                List<String> allRows =
                        sqlSourceHelper.getAllRows(result);
                Event event = null;
                for (String row : allRows) {
                    event = new SimpleEvent();
                    event.setBody(row.getBytes());
                    event.setHeaders(header);
                    events.add(event);
                }
                //将 event 写入 channel

                this.getChannelProcessor().processEventBatch(events);
                //更新数据表中的 offset 信息
                sqlSourceHelper.updateOffset2DB(result.size());
            }
            //等待时长
            Thread.sleep(sqlSourceHelper.getRunQueryDelay());
            return Status.READY;
        } catch (InterruptedException e) {
            LOG.error("Error procesing row", e);
            return Status.BACKOFF;
        }
    }

    public synchronized void stop() {
        LOG.info("Stopping sql source {} ...", getName());
        try {
            //关闭资源
            sqlSourceHelper.close();
        } finally {
            super.stop();
        }
    }
}