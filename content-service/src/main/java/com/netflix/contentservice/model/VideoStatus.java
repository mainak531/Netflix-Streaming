package com.netflix.contentservice.model;

// Pending -> Uploaded -> Encoding -> Encoded -> Ready or Failed

public enum VideoStatus {
    PENDING,  // movie added but not uploaded
    UPLOADED, // raw video uploaded to S3
    ENCODING, // ffmpeg is encoding the video
    ENCODED,  // encoding complete
    READY,    // HSL playlist ready
    FAILED    // encoding failed
}
