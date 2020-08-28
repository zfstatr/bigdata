package org.example.sink;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Mysink extends AbstractSink implements Configurable {
    //获取logger对象
    private Logger logger = LoggerFactory.getLogger(Mysink.class);

    //定义全局的前后缀
    private String prefix;
    private String subfix;

    @Override
    public void configure(Context context) {
        //读取配置信息给前后缀
        prefix = context.getString("prefix");
        subfix = context.getString("subfix","zf");
    }

    /**
     * 1.获取channel
     * 2.从channel读取数据
     * 3.发送数据
     * @return
     * @throws EventDeliveryException
     */
    @Override
    public Status process() throws EventDeliveryException {
        //1.定义返回值
        Status status = null;

        //2.获取channel
        Channel channel = getChannel();

        //3.从channel读取事务
        Transaction transaction = channel.getTransaction();


        //4.开启事务
        transaction.begin();
        //读取 Channel 中的数据，直到读取到事件结束循环
        Event event;

        while (true) {
            event = channel.take();
            if (event != null) {
                break;
            }
        }
        try {
            //6.处理事件
            String body = new String(event.getBody());
            logger.info(prefix + body + subfix);

            //7.提交事务
            transaction.commit();

            status = Status.READY;
        } catch (ChannelException e) {
            e.printStackTrace();

            //8.提交失败
            transaction.rollback();

            status = Status.BACKOFF;
        }finally {
            //11.关闭事务
            if (transaction != null){
                transaction.close();
            }
        }

        return status;
    }


}
