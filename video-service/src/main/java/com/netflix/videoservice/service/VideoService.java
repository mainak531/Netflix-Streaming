package com.netflix.videoservice.service;

import java.io.IOException;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import com.netflix.videoservice.event.VideoUploadedEvent;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

@Service
@Slf4j
@RequiredArgsConstructor
public class VideoService {
    
    private final S3Client s3Client;
    private final KafkaTemplate<String, VideoUploadedEvent> kafkaTemplate;

    @Value("${aws.s3.bucket-name}")
    private String bucketName;

    private static final String VIDEO_UPLOADED_TOPIC = "video.uploaded";

    // Upload video to S3 and publish VideoUploadedEvent to Kafka
    public String uploadVideo(String movieId, MultipartFile file) throws IOException {
        log.info("Starting video upload for movie: {} file: {}", movieId, file.getOriginalFilename());

        // generate unique S3 key for raw video. Format: raw/movieId/uuid_filename
        String videoKey = "raw/" + movieId + "/" + UUID.randomUUID() + "_" + file.getOriginalFilename();

        // build the request to upload the video to S3
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
            .bucket(bucketName)
            .key(videoKey)
            .contentType(file.getContentType())
            .contentLength(file.getSize())
            .build();
        
        // upload the video to S3
        s3Client.putObject(putObjectRequest, RequestBody.fromInputStream(file.getInputStream(), file.getSize()));
        
        log.info("Video uploaded to S3 successfully! Key: {}", videoKey);

        // publish event to Kafka, Encoding service will consume this and start ffmpeg processing
        VideoUploadedEvent videoUploadedEvent = VideoUploadedEvent.builder()
            .movieId(movieId)
            .videoKey(videoKey)
            .bucketName(bucketName)
            .originalFileName(file.getOriginalFilename())
            .fileSizeBytes(file.getSize())
            .build();

        kafkaTemplate.send(VIDEO_UPLOADED_TOPIC, movieId, videoUploadedEvent);

        log.info("VideoUploadedEvent published for movie: {}", movieId);

        return videoKey;
    }
}
