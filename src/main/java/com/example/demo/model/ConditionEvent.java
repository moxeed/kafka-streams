package com.example.demo.model;

public class ConditionEvent {
    public int atomId;
    public int conditionId;
    public String operation;
    public int atomValue;
    public int rlcValue;

    public boolean isOk() {
        return switch (operation) {
            case ">" -> rlcValue > atomValue;
            default -> rlcValue <= atomValue;
        };
    }
}
