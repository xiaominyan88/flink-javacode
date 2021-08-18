package com.hyit.entity;

public class MessageEntity {

    private String measurePointId;
    private String measureTag;
    private String sampleTime;
    private double value;

    public MessageEntity(){}

    public MessageEntity(String measurePointId,String measureTag,double value,String sampleTime){
        this.measurePointId = measurePointId;
        this.measureTag = measureTag;
        this.value = value;
        this.sampleTime = sampleTime;
    }

    public String getMeasurePointId() {
        return measurePointId;
    }

    public void setMeasurePointId(String measurePointId) {
        this.measurePointId = measurePointId;
    }

    public String getMeasureTag() {
        return measureTag;
    }

    public void setMeasureTag(String measureTag) {
        this.measureTag = measureTag;
    }

    public String getSampleTime() {
        return sampleTime;
    }

    public void setSampleTime(String sampleTime) {
        this.sampleTime = sampleTime;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

}
