package com.jx.kafak.producer;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;


/**
 * @Author jx
 * @Date 2021/12/24
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Company implements Serializable {
    private String name;
    private String address;
}
