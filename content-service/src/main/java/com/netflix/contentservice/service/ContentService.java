package com.netflix.contentservice.service;

import java.util.List;
import java.util.stream.Collectors;

import org.springframework.stereotype.Service;

import com.netflix.contentservice.dto.MovieRequest;
import com.netflix.contentservice.dto.MovieResponse;
import com.netflix.contentservice.model.Genre;
import com.netflix.contentservice.model.Movie;
import com.netflix.contentservice.model.VideoStatus;
import com.netflix.contentservice.repository.MovieRepository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
@RequiredArgsConstructor
public class ContentService {
    
    private final MovieRepository movieRepository;

    public MovieResponse addMovie(MovieRequest request) {
        log.info("Adding new movie: {}", request.getTitle());

        Movie movie = Movie.builder()
                .title(request.getTitle())
                .description(request.getDescription())
                .genere(request.getGenre())
                .director(request.getDirector())
                .cast(request.getCast())
                .releaseYear(request.getReleaseYear())
                .rating(request.getRating())
                .thumbnailUrl(request.getThumbnailUrl())
                .durationMinutes(request.getDurationMinutes())
                .videoStatus(VideoStatus.PENDING)
                .build();

        Movie savedMovie = movieRepository.save(movie);
        log.info("Movie added with id: {}", savedMovie.getId());

        return mapToResponse(savedMovie);
    }

    public List<MovieResponse> getAllMovies() {
        return movieRepository.findAll()
                .stream()
                .map(this::mapToResponse)
                .collect(Collectors.toList());
    }

    public MovieResponse getMovieById(String movieId) {
        Movie movie = movieRepository.findById(movieId)
                .orElseThrow(() -> new RuntimeException("Movie not found with id: " + movieId));
        
        return mapToResponse(movie);
    }

    public List<MovieResponse> getMoviesByGenre(Genre genre) {
        return movieRepository.findByGenere(genre)
                .stream()
                .map(this::mapToResponse)
                .collect(Collectors.toList());
    }

    public List<MovieResponse> searchMovies(String title) {
        return movieRepository.findByTitleContainingIgnoreCase(title)
                .stream()
                .map(this::mapToResponse)
                .collect(Collectors.toList());
    }

    public void updateVideoKey(String movieId, String videoKey) {
        log.info("Updating videokey for movie: {}", movieId);

        Movie movie = movieRepository.findById(movieId)
                .orElseThrow(() -> new RuntimeException("Movie not found with id: " + movieId));

        movie.setVideoKey(videoKey);
        movie.setVideoStatus(VideoStatus.UPLOADED);
        movieRepository.save(movie);
    }

    public void updateHlsUrl(String movieId, String hlsUrl) {
        log.info("Updating HLS URL for movie: {}", movieId);

        Movie movie = movieRepository.findById(movieId)
                .orElseThrow(() -> new RuntimeException("Movie not found with id: " + movieId));

        movie.setHlsUrl(hlsUrl);
        movie.setVideoStatus(VideoStatus.READY);
        movieRepository.save(movie);

        log.info("Movie {} is now ready for streaming", movieId);
    }

    public void updateVideoStatus(String movieId, VideoStatus videoStatus) {
        Movie movie = movieRepository.findById(movieId)
                .orElseThrow(() -> new RuntimeException("Movie not found: " + movieId));
        movie.setVideoStatus(videoStatus);
        movieRepository.save(movie);
    }

    private MovieResponse mapToResponse(Movie movie) {
        return MovieResponse.builder()
                .id(movie.getId())
                .title(movie.getTitle())
                .description(movie.getDescription())
                .genre(movie.getGenere())
                .director(movie.getDirector())
                .cast(movie.getCast())
                .releaseYear(movie.getReleaseYear())
                .rating(movie.getRating())
                .thumbnailUrl(movie.getThumbnailUrl())
                .durationMinutes(movie.getDurationMinutes())
                .videoStatus(movie.getVideoStatus())
                .videoKey(movie.getVideoKey())
                .hlsUrl(movie.getHlsUrl())
                .createdAt(movie.getCreatedAt())
                .build();
    }
}
