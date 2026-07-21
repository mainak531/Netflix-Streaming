package com.netflix.contentservice.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.netflix.contentservice.model.Genre;
import com.netflix.contentservice.model.Movie;
import java.util.List;


public interface MovieRepository extends JpaRepository<Movie, String> {
    List<Movie> findByGenere(Genre genere);
    List<Movie> findByTitleContainingIgnoreCase(String title);
}
