package com.mytech.model;

import lombok.Data;

import java.time.Instant;
import java.util.Map;

@Data
public class ManufacturingResult {

    private String eventType;
    private String barcode;

    private Integer productCode;
    private Integer productSeq; // keep optional if not always present

    private Integer stationCode;
    private Integer stationChannelNo;

    private Integer result;

    // NEW: dynamic test results
    private Map<String, Double> testResults;

    private String operator;

    private Instant startTime;
    private Instant endTime;
}
