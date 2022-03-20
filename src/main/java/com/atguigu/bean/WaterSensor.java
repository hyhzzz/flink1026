package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author CoderHyh
 * @create 2022-03-18 16:43
 * id:传感器编号
 * ts:时间戳
 * vc:水位
 */
@Data //生成get/set
@NoArgsConstructor //生成无参构造
@AllArgsConstructor //生成全参构造
public class WaterSensor {
    private String id;
    private Long ts;
    private Integer vc;
}
