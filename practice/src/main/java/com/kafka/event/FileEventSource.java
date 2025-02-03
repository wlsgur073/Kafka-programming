package com.kafka.event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.ExecutionException;

public class FileEventSource implements Runnable{

    private static final Logger logger = LoggerFactory.getLogger(FileEventSource.class);

    private boolean keepRunning = true;
    private long updateInterval;

    private File file;
    private long filePointer = 0;

    private EventHandler eventHandler;

    public FileEventSource(long updateInterval, File file, EventHandler eventHandler) {
        this.updateInterval = updateInterval;
        this.file = file;
        this.eventHandler = eventHandler;
    }

    @Override
    public void run() {
        try {
            while (this.keepRunning) {
                // 파일에서 메시지를 읽어옴
                // 메시지를 읽어온 후 이벤트 핸들러로 메시지를 전달
                // 메시지를 전달한 후 파일에서 읽은 메시지를 삭제
                // 파일에서 읽은 메시지가 없다면 1초 대기
                Thread.sleep(this.updateInterval);

                // read file size // 파일의 크기가 증가하면 수정된 것을 의미함.
                long len = this.file.length();
                // 파일의 크기가 줄어들었을 때
                if (len < this.filePointer) {
                    logger.info("file was reset. filePointer is longer than file size. Resetting..");
                    this.filePointer = len;
                } else if (len > this.filePointer) { // 파일에 변화가 생김
                    readAppendAndSend();
                } else {
                    logger.info("no change in file size. filePointer: {}, file size: {}", this.filePointer, len);
                }
            }
        } catch (InterruptedException e) {
            logger.error("FileEventSource.InterruptedException: {}", e.getMessage());
        } catch (IOException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private void readAppendAndSend() throws IOException, ExecutionException, InterruptedException {
        RandomAccessFile raf = new RandomAccessFile(this.file, "r"); // 자바에서 파일 포인터를 자유롭게 옮길 수 있는 클래스.
        raf.seek(this.filePointer); // 파일 포인터를 이동시킴 // 처음에는 0부터 읽을 거임.

        String line;
        while ((line = raf.readLine()) != null) {
            sendMessage(line);
        }
        // file이 변경되면 file의 filePointer를 현재 file의 마지막으로 재설정함.
        this.filePointer = raf.getFilePointer();
    }

    private void sendMessage(String line) throws ExecutionException, InterruptedException {
        String delimiter = ",";
        String[] tokens = line.split(delimiter);
        String key = tokens[0];
        StringBuffer value = new StringBuffer();

        String joinStr = String.join(delimiter, tokens);
        value.append(joinStr);

        MessageEvent messageEvent = new MessageEvent(key, value.toString());
        this.eventHandler.onMessage(messageEvent);
    }
}
