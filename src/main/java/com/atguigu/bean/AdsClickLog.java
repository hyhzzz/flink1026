package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author CoderHyh
 * @create 2022-03-21 17:34
 */
@NoArgsConstructor
@AllArgsConstructor
@Data
public class AdsClickLog {
    private long userId;
    private long adsId;
    private String province;
    private String city;
    private Long timestamp;
}