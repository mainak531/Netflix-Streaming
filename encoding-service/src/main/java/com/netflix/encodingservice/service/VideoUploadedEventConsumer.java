package com.netflix.encodingservice.service;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.netflix.encodingservice.event.VideoUploadedEvent;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
@RequiredArgsConstructor
public class VideoUploadedEventConsumer {
    
    private final EncodingService encodingService;

    @KafkaListener(topics = "video.uploaded", groupId = "encoding-service-group")
    public void consumeVideoUploadEvent(VideoUploadedEvent event) {
        log.info("Consumed VideoUploadedEvent for movie: {} file: {}", event.getMovieId(), event.getOriginalFileName());

        try {
            encodingService.encodeVideo(event);
        } catch (Exception e) {
            log.error("Failed to process encoding for movie: {} - {}", event.getMovieId(), e.getMessage());
        }
    }
}
