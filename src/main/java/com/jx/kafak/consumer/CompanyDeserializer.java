package com.jx.kafak.consumer;
/*


import com.jx.kafak.producer.Company;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

*/

import com.jx.kafak.producer.Company;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * @Author jx
 * @Date 2021/12/28
 *//*

public class CompanyDeserializer implements Deserializer<Company> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public void close() {

    }

    @Override
    public Company deserialize(String topic, byte[] data) {
        System.out.println("data====>" + data.toString());
        if (data == null) {
            return null;
        }
//        if (data.length < 8) {
//            throw new SerializationException("Size of data received " +
//                    "by DemoDeserializer is shorter than expected!");
//        }
        ByteBuffer byteBuffer = ByteBuffer.wrap(data);
        int nameLen, addressLen;
        String name, address;

        nameLen = byteBuffer.getInt();
        byte[] nameBytes = new byte[nameLen];
        byteBuffer.get(nameBytes);
        addressLen= byteBuffer.getInt();
        byte[] addressBytes = new byte[addressLen];
        byteBuffer.get(addressBytes);

        name = new String(nameBytes, StandardCharsets.UTF_8);
        address = new String(addressBytes,StandardCharsets.UTF_8);

        return new Company(name,address);
    }
}
*/
public class CompanyDeserializer implements Deserializer<Company> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {}

    @Override
    public Company deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        if (data.length < 8) {
            throw new SerializationException("Size of data received " +
                    "by DemoDeserializer is shorter than expected!");
        }
        ByteBuffer buffer = ByteBuffer.wrap(data);
        int nameLen, addressLen;
        String name, address;

        nameLen = buffer.getInt();
        byte[] nameBytes = new byte[nameLen];
        buffer.get(nameBytes);
        addressLen = buffer.getInt();
        byte[] addressBytes = new byte[addressLen];
        buffer.get(addressBytes);

        try {
            name = new String(nameBytes, "UTF-8");
            address = new String(addressBytes, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new SerializationException("Error occur when deserializing!");
        }

        return new Company(name,address);
    }

    @Override
    public void close() {}
}