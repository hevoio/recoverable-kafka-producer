package com.hevodata.bigqueue;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class BigQueueConsumerConfig {
    private long sleepTimeInSecs = 10;
}
