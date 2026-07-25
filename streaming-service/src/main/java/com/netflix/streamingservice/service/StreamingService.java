package com.netflix.streamingservice.service;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import com.netflix.streamingservice.dto.StreamingResponse;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;

@Service
@RequiredArgsConstructor
@Slf4j
public class StreamingService {
    
    private final S3Client s3Client;
    private final S3Presigner s3Presigner;
    private final RedisTemplate<String, String> redisTemplate;

    @Value("${aws.s3.bucket-name}")
    private String buckerName;

    @Value("${aws.s3.presigned-url-expiry}")
    private long presignedUrlExpiry; // 60 minutes

    // Redis key for caching streaming URLs
    private final static String STREAMING_URL_CACHE_PREFIX = "streaming:url:";

    /*
        Get streaming URL for a movie
        1. Check redis cache for existing presigned URL
        2. If cached - return immediately
        3. If not cached - generate new presigned URL from S3
        4. Cache the new URL in Redis
        5. Return streaming URL

        Why presigned URL?
        - S3 is private locker room - videos are not publicaly accessible
        - Presigned URL gives temporaty access (X minutes)
        - Prevents unauthorized video downloads
    */
    public StreamingResponse getStreamingUrl(String movieId, String playlistKey) {
        log.info("Getting streaming URL for movie: {}", movieId);

        String cacheKey = STREAMING_URL_CACHE_PREFIX + movieId;

        // Check Redis cache first
        String cachedUrl = redisTemplate.opsForValue().get(cacheKey);
        if (cachedUrl != null) {
            log.info("Returning cached streaming URL for movie: {}", movieId);
            return StreamingResponse.builder()
                    .movieId(movieId)
                    .streamingUrl(cachedUrl)
                    .quality("1080p, 720p, 480p, 360p, 240p, 144p")
                    .expiredInMinutes(presignedUrlExpiry)
                    .build();
        }

        // Generate presigned URL from S3
        log.info("Generating new presigned URl for movie: {}", movieId);
        String presignedUrl = generatePresignedUrl(playlistKey);

        // Cache in redis for 55 minutes (5 minutes less than actual expiry to avoid edge cases)
        redisTemplate.opsForValue().set(cacheKey, presignedUrl, 55, TimeUnit.MINUTES);
        
        log.info("Streaming URL generated and cached for movie: {}", movieId);

        return StreamingResponse.builder()
                .movieId(movieId)
                .streamingUrl(presignedUrl)
                .quality("1080p, 720p, 480p, 360p, 240p, 144p")
                .expiredInMinutes(presignedUrlExpiry)
                .build(); 
    }   
    
    // This is the key menthod that makes everything secure
    public String getSignedPlaylist(String movieId, String playlistPath) {
        // Get base path for this playlist
        String basePath = playlistPath.substring(0, playlistPath.lastIndexOf('/') + 1);

        // Read m3u8 content from S3
        String m3u8Content = readFromS3(playlistPath);

        // Rewrite each line that is a segment or playlist reference
        String signedContent = rewriteM3u8SignedUrls(m3u8Content, basePath);

        return signedContent;
    }

    private String rewriteM3u8SignedUrls(String m3u8Content, String basePath) {
        StringBuilder rewritten = new StringBuilder();

        for (String line : m3u8Content.split("\n")) {
            String trimmed = line.trim();

            // Skip empty lines and comments
            if (trimmed.isEmpty() || trimmed.startsWith("#")) {
                rewritten.append(line).append("\n");
                continue;
            }

            // This is a segment or playlist reference. Build full S3 key and sign it
            String fullKey = basePath + trimmed;
            String signedUrl = generatePresignedUrl(fullKey);

            rewritten.append(signedUrl).append("\n");
        }

        return rewritten.toString();
    }

    // Read file content from S3
    private String readFromS3(String s3Key) {
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(buckerName)
                .key(s3Key)
                .build();

        ResponseInputStream<GetObjectResponse> response = s3Client.getObject(request);

        return new BufferedReader(new InputStreamReader(response))
                .lines()
                .collect(Collectors.joining("\n"));
    }
    
    // Generate a presigned URL for S3 object, URL expired after configured time
    private String generatePresignedUrl(String key) {
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(buckerName)
                .key(key)
                .build();
        
        GetObjectPresignRequest presignRequest = GetObjectPresignRequest.builder()
                .signatureDuration(Duration.ofMinutes(presignedUrlExpiry))
                .getObjectRequest(getObjectRequest)
                .build();
        
        return s3Presigner.presignGetObject(presignRequest)
                .url()
                .toString();
    }

    // Invalidate cache streaming URL. Called when video is re-encoded or updated
    public void invalidateCache(String movieId) {
        String cacheKey = STREAMING_URL_CACHE_PREFIX + movieId;
        redisTemplate.delete(cacheKey);
        log.info("Streaming URL cache invalidate for movie: {}", movieId);
    }
}
