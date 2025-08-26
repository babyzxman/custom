package com.gable.templar.custom;

import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;

public class NewThreadExecutor {

    public static TaskExecutor threadExecutor;

    static {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(60);         // keep 20 threads alive
        executor.setMaxPoolSize(70);          // allow burst up to 40
        executor.setQueueCapacity(10);       // buffer before creating more threads
        executor.setKeepAliveSeconds(60);     // non-core threads die after idle
        executor.setThreadNamePrefix("MyCustomThreadPool-");
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(30);
        executor.initialize();
        threadExecutor = executor;
    }
}
