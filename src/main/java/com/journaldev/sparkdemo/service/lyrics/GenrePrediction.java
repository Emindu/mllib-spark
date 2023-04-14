package com.journaldev.sparkdemo.service.lyrics;

public class GenrePrediction {

    private String genre;
    private Double metalProbability;
    private Double popProbability;
    private String probability;

    public void setGenre(String genre) {
        this.genre = genre;
    }

    public void setMetalProbability(Double metalProbability) {
        this.metalProbability = metalProbability;
    }

    public void setPopProbability(Double popProbability) {
        this.popProbability = popProbability;
    }

    public String getProbability() {
        return probability;
    }

    public void setProbability(String probability) {
        this.probability = probability;
    }

    public GenrePrediction(String genre, Double metalProbability, Double popProbability) {
        this.genre = genre;
        this.metalProbability = metalProbability;
        this.popProbability = popProbability;
    }

    public GenrePrediction(String genre) {
        this.genre = genre;
    }

    public String getGenre() {
        return genre;
    }

    public Double getMetalProbability() {
        return metalProbability;
    }

    public Double getPopProbability() {
        return popProbability;
    }
}
