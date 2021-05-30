package main;

import java.util.concurrent.TimeUnit;

public class TimeHelper {
    public static String getTIME(long time) {

        // This method uses this formula :minutes =
        // (milliseconds / 1000) / 60;
        long minutes
                = TimeUnit.MILLISECONDS.toMinutes(time);

        // This method uses this formula seconds =
        // (milliseconds / 1000);
        long seconds
                = (TimeUnit.MILLISECONDS.toSeconds(time)
                % 60);

        return (minutes + ":" + seconds);
    }
}
