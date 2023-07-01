package com.example.demo.model;

import java.util.HashMap;

public class Condition {
    public HashMap<Integer, Boolean> atoms;

    public boolean isPassed() {
        for (var atom : atoms.values()) {
            if (!atom)
                return false;
        }

        return true;
    }

    public Condition() {
        atoms = new HashMap<>();
    }
}
