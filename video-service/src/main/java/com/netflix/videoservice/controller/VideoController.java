package com.netflix.videoservice.controller;

import java.io.IOException;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.netflix.videoservice.service.VideoService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RestController
@RequestMapping("/api/v1/videos")
@Slf4j
@RequiredArgsConstructor
@CrossOrigin(origins = "*")
public class VideoController {
    
    private final VideoService videoService;

    // Upload video file for a movie
    @PostMapping("/upload/{movieId}")
    public ResponseEntity<String> uploadVide(@PathVariable String movieId, @RequestParam("file") MultipartFile file) throws IOException {
        
        log.info("Video upload request for movie: {} file size: {}MB", movieId, file.getSize() / (1024 * 1024));

        if (file.isEmpty()) {
            return ResponseEntity.badRequest().body("File is empty");
        }
        String videoKey = videoService.uploadVideo(movieId, file);

        return ResponseEntity.ok(
            "Video uploaded successfully! Key: " + videoKey +
            " - Encoding started automatically via Kafka"
        );
    }
}
