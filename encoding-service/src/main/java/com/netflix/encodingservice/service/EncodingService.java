package com.netflix.encodingservice.service;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.netflix.encodingservice.event.VideoEncodedEvent;
import com.netflix.encodingservice.event.VideoUploadedEvent;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

@Service
@Slf4j
@RequiredArgsConstructor
public class EncodingService {
    
    private final S3Client s3Client;
    private final KafkaTemplate<String, VideoEncodedEvent> kafkaTemplate;

    @Value("${aws.s3.bucket-name}")
    private String bucketName;

    @Value("${ffmpeg.path}")
    private String ffmpegPath;

    @Value("${encoding.base-path}")
    private String basePath;

    private static final String VIDEO_ENCODED_TOPIC = "video.encoded";

    // Video qualities to encode. Format: resolution, bitrate, height
    private static final List<int[]> VIDEO_QUALITIES = Arrays.asList(
        new int[]{1920, 5000, 1080}, // 1080p - 5000k bitrate
        new int[]{1280, 2800, 720},  // 720p  - 2800k bitrate
        new int[]{854, 1200, 480},   // 480p  - 1200k bitrate
        new int[]{640, 800, 360},    // 360p  - 800k bitrate
        new int[]{426, 400, 240},    // 240p  - 400k bitrate
        new int[]{256, 150, 144}     // 144p  - 150k bitrate
    );

    /*
        main encoding pipeline.
        Steps:
        1. Dowload raw video from S3
        2. Encode to multiple qualities using FFmpeg
        3. Generate HLS playlist (.m3u8) for each quality
        4. Create master playlist
        5. Upload all encoded files back to S3
        6. Publish VideoEncodedEvent to Kafka
    */
    public void encodeVideo(VideoUploadedEvent event) {
        log.info("Starting encoding platform for movie: {}", event.getMovieId());

        // Create a unique path for movie
        String jobPath = basePath + "/" + event.getMovieId();

        try {
            // Create temp directories
            Files.createDirectories(Paths.get(jobPath));
            Files.createDirectories(Paths.get(jobPath + "/encoded"));

            // Download raw video from S3
            String localVideoPath = jobPath + "/raw_video.mp4";
            downloadFromS3(event.getVideoKey(), localVideoPath);
            log.info("Raw video downloaded to {}", localVideoPath);

            // Encode to multiple qualities and generate HLS
            for (int[] quality : VIDEO_QUALITIES) {
                int width = quality[0];
                int bitrate = quality[1];
                int height = quality[2];

                String qualityDir = jobPath + "/encoded/" + height + "p";
                Files.createDirectories(Paths.get(qualityDir));

                encodeToHLS(localVideoPath, qualityDir, width, height, bitrate);
                log.info("Encoded {}p successfully", height);
            }

            // Generate master playlist
            String masterPlayslistPath = jobPath + "/encoded/master.m3u8";
            generateMasterPlaylist(masterPlayslistPath);
            log.info("Master playlist generated");

            // Upload all resources files to S3
            String encodedPrefix = "encoded/" + event.getMovieId() + "/";
            uploadEncodedFilesToS3(jobPath + "/encoded", encodedPrefix);
            log.info("All encoded files uploaded to S3");
            
            // Publish VideoEncodedEvent to Kafka
            String masterPlaylistKey = encodedPrefix + "master.m3u8";
            String hlsUrl = "https://" + bucketName + ".s3.amazonaws.com/" + masterPlaylistKey;

            VideoEncodedEvent encodedEvent = VideoEncodedEvent.builder()
                .movieId(event.getMovieId())
                .hlsUrl(hlsUrl)
                .masterPlaylistKey(masterPlaylistKey)
                .success(true)
                .errorMessage(null)
                .build();
            
            kafkaTemplate.send(VIDEO_ENCODED_TOPIC, event.getMovieId(), encodedEvent);
            log.info("VideoEncodedEvent published for movie: {}", event.getMovieId());
        } catch (Exception e) {
            log.error("encoding failed for movie: {} - {}", event.getMovieId(), e.getMessage());

            // Publish failure event to Kafka
            VideoEncodedEvent encodeFailureEvent = VideoEncodedEvent.builder()
                .movieId(event.getMovieId())
                .hlsUrl(null)
                .masterPlaylistKey(null)
                .success(false)
                .errorMessage(e.getMessage())
                .build();
            
            kafkaTemplate.send(VIDEO_ENCODED_TOPIC, event.getMovieId(), encodeFailureEvent);
        } finally {
            // Clean up temp files
            cleanupTempFiles(jobPath);
        }
    }

    // Download file from S3 to local path
    private void downloadFromS3(String s3Key, String localPath) {
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
            .bucket(bucketName)
            .key(s3Key)
            .build();
        
        s3Client.getObject(getObjectRequest, Paths.get(localPath));
    }

    // Encode video to HLS format using FFmpeg
    private void encodeToHLS(String inputPath, String outputDir, int width, int height, int bitrate) throws IOException, InterruptedException {
        String playlistPath = outputDir + "/playlist.m3u8";
        String segmentPattern = outputDir + "/segment_%03d.ts";

        // FFmpeg command for HLS encoding
        List<String> command = Arrays.asList(
            ffmpegPath,                                   // path to ffmpeg executable
            "-i", inputPath,                              // -i : input file
            "-vf", "scale=w=" + width + ":h=" + height,   // -vf : video filter, scale : resize video to target resolution
            "-c:v", "libx264",                            // -c:v : video codec, libx264 is widely used for HLS encoding and is the encoder for H.264, codec performs Compression + decompression
            "-b:v", bitrate + "k",                        // -b:v : bitrate for video stream, e.g. 5000k (kilobits) per second -> 5000k for 5 Mbps
            "-c:a", "aac",                                // -c:a : audio codec, AAC is common for HLS and is standard format for streaming
            "-b:a", "128k",                               // -b:a : audio bitrate, 128k is common for good audio quality in streaming, 128k for 128 kbps
            "-hls_time", "10",                            // -hls_time : segment duration in seconds, 10 means each .ts segment will be around 10 seconds long
            "-hls_list_size", "0",                        // -hls_list-size : number of segments in playlist, 0 means include all segments in the playlist
            "-hls_segment_filename", segmentPattern,      // -hls_segment_filename : pattern for naming segment files, e.g. segment_%03d.ts will create segment_001.ts, segment_002.ts, etc.
            "-f", "hls",                                  // -f : output format, hls specifies that we want to create HLS output
            playlistPath                                  // output playlist file path, e.g. /path/to/encoded/720p/playlist.m3u8
        );

        ProcessBuilder processBuilder = new ProcessBuilder(command);
        processBuilder.redirectErrorStream(true);
        processBuilder.inheritIO();
        Process process = processBuilder.start();
        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new RuntimeException("FFmpeg encoding failed with exit code: " + exitCode);
        }
    }

    // Generate master playlist that references all quality playlists. This is the file that video player downloads first
    private void generateMasterPlaylist(String masterPlaylistPath) throws IOException {
        StringBuilder master = new StringBuilder();
        master.append("#EXTM3U\n");
        master.append("#EXT-X-VERSION:3\n");

        int[][] qualities = VIDEO_QUALITIES.toArray(new int[0][]);

        for (int[] quality : qualities) {
            int width = quality[0];
            int bitrate = quality[1];
            int height = quality[2];

            master.append("#EXT-X-STREAM-INF:BANDWIDTH=")
                    .append(bitrate * 1000) // Convert kbps to bps
                    .append(",RESOLUTION=").append(width).append("x").append(height)
                    .append(", CODECS=\"avc1.42e01e,mp4a.40.2\"\n"); // Codecs for H.264 video and AAC audio
            master.append(height).append("p/playlist.m3u8\n\n");
        }

        Files.writeString(Paths.get(masterPlaylistPath), master.toString());
    }

    // Upload all encoded files in the directory to S3 with the given prefix
    private void uploadEncodedFilesToS3(String localDir, String s3Prefix) throws IOException {
        File directory = new File(localDir);
        uploadDirectoryToS3(directory, localDir, s3Prefix);
    }
    // Recursively upload files in the directory to S3, maintaining the directory structure
    private void uploadDirectoryToS3(File dir, String baseDir, String s3Prefix) throws IOException {
        for (File file : dir.listFiles()) {
            if (file.isDirectory()) {
                uploadDirectoryToS3(file, baseDir, s3Prefix);
            } else {
                String relativePath = file.getAbsolutePath()
                        .substring(baseDir.length() + 1)
                        .replace("\\", "/");
                
                String s3Key = s3Prefix + relativePath;

                String conentType = file.getName().endsWith(".m3u8") ? "application/x-mpegURL" : "video/MP2T";

                PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(s3Key)
                    .contentType(conentType)
                    .build();
                
                s3Client.putObject(putObjectRequest, RequestBody.fromFile(file));
            }
        }
    }

    // Clean up temporary files created during encoding
    private void cleanupTempFiles(String jobPath) {
        try {
            Path dirPath = Paths.get(jobPath);
            if (Files.exists(dirPath)) {
                Files.walk(dirPath)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
                
                log.info("Temp files cleaned up for job: {}", jobPath);
            }
        } catch (Exception e) {
            log.warn("Failed to clean up temp files: {}", e.getMessage());
        }
    }
}
