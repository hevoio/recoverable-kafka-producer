package com.hevodata.bigqueue;

import com.leansoft.bigqueue.BigArrayImpl;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.nio.file.Path;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class BigQueuePoolConfiguration<T> {
    private String name;
    private int noOfQueues;
    private Path baseDir;
    @Builder.Default
    private BigQueueConsumerConfig bigQueueConsumerConfig = new BigQueueConsumerConfig();
    private BaseBigQueueConsumer<T> bigQueueConsumer;
    private BigQueueSerDe<T> bigQueueSerDe;
    int diskSpaceThresholdGBs = 10;
    @Builder.Default
    int pageSize = BigArrayImpl.MINIMUM_DATA_PAGE_SIZE;
}
