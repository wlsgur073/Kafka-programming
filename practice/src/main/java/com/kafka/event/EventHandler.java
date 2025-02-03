package com.kafka.event;

import java.util.concurrent.ExecutionException;

public interface EventHandler {

    // producer에서 send()를 호출할 때 get()으로 동기 방식으로 처리했을 때 필요한 예외처리.
    void onMessage(MessageEvent messageEvent) throws InterruptedException, ExecutionException;
}
