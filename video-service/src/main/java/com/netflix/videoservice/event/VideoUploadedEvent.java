package com.netflix.videoservice.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

// Event published to Kafka when a video is uploaded to S3. Topic: video.uploaded

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class VideoUploadedEvent {
    private String movieId;
    private String videoKey;
    private String bucketName;
    private String originalFileName;
    private long fileSizeBytes;

}
