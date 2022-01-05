package com.jx.kafak.consumer;

import kafka.admin.TopicCommand;


/**
 * @Author jx
 * @Date 2021/12/31
 * 直接调用 TopicCommand 类中的 main() 函数来直接管理主题
 */
public class CreateTopicDemo {
    public static void main(String[] args) {
        // createTopic();
        describeTopic();
    }

    public static void createTopic() {
        String[] options = new String[] {
                "--bootstrap-server","192.168.220.10:9092",
                "--create",
                "--topic","--topic-create-api",
                "--partitions","1",
                "--replication-factor","1"
        };
        TopicCommand.main(options);
    }

    public static void describeTopic() {
        String[] options = new String[]  {
             "--bootstrap-server","192.168.220.10:9092",
             "--describe",
             "--topic","topic-create",
        };
        TopicCommand.main(options);
    }
}
