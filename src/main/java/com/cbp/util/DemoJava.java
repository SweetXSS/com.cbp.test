package com.cbp.util;
import java.io.IOException;
import java.util.ArrayList;

public class DemoJava {
    public static void main(String[] args) {
        String str = "2001520160112001520160";
        System.out.println(Long.valueOf(Integer.valueOf(str)));
    }

    public static ArrayList<String> getcolname() throws IOException {
        ArrayList<String> cols = new ArrayList<String>();
        cols.add("se");
        cols.add("je");
        cols.add("name");
        cols.add("age");
        return cols;
    }
}
