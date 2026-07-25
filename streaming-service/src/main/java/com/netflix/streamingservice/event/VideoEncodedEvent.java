package com.netflix.streamingservice.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

// Event published to Kafka when a video is encoded. Topic: video.enco  ded

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class VideoEncodedEvent {
    private String movieId;
    private String hlsUrl;             // Master playlist url for streaming
    private String masterPlaylistKey;  // S3 key of master.m3u8
    private boolean success;
    private String errorMessage;
}

