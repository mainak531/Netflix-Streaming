package com.netflix.streamingservice.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class StreamingResponse {
    private String movieId;
    private String streamingUrl;    // Presigned HLS master playlist URL
    private String quality;         // Available qualities
    private long expiredInMinutes;  // URL expiry time
}
