package org.wso2.carbon.siddhihive.core.utils;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;


public class CronExpressionCreator {

    private int months = 0;
    private int days = 0;
    private int hours = 0;
    private int minutes = 0;
    private int seconds = 0;
    private final String DEFAULT_DAY_OF_WEEK = "?";
    private final String DEFAULT_YEAR = "*";

    private List<Integer> params;
    private List<String> cronList;

    public String getCronExpression(long durationInMillis) {
        populateData(durationInMillis);
        cronList = getCronList();
        return cronList.get(4) + " " + cronList.get(3) + " " + cronList.get(2) + " " + cronList.get(1) + " " + cronList.get(0) + " " + DEFAULT_DAY_OF_WEEK + " " + DEFAULT_YEAR;

    }

    private List<String> getCronList() {
        List<String> result = new ArrayList<String>();
        int i;
        int month = params.get(0);
        if (month == 0) {
            result.add("*");
        } else {
            result.add("*/" + month);
        }
        for (i = 1; i < params.size(); i++) {
            if (params.get(i) == 0) {
                String exp = "*";
                for (int j = i - 1; j >= 0; j--) {
                    if (params.get(j) != 0) {
                        Date date = new Date();
                        if (i == 1)
                            exp = String.valueOf(date.getDate());
                        if (i == 2)
                            exp = String.valueOf(date.getHours());
                        if (i == 3)
                            exp = String.valueOf(date.getMinutes());
                        if (i == 4)
                            exp = String.valueOf(date.getSeconds());

                    }
                }
                result.add(exp);
            } else {

                result.add("*/" + params.get(i));

            }
        }
        return result;
    }


    /*
    returns date in MM:dd:hh:mm:ss
     */
    private void populateData(long millis) {
        params = new ArrayList<Integer>();
        if (millis < 0) {
            throw new IllegalArgumentException("Duration must be greater than zero!");
        }
        days = (int) TimeUnit.MILLISECONDS.toDays(millis);
        millis -= TimeUnit.DAYS.toMillis(days);
        if (days > 31) {
            months = (days / 31);
            days = days % 31;
        }
        params.add(months);
        params.add(days);
        hours = (int) TimeUnit.MILLISECONDS.toHours(millis);
        params.add(hours);
        millis -= TimeUnit.HOURS.toMillis(hours);
        minutes = (int) TimeUnit.MILLISECONDS.toMinutes(millis);
        params.add(minutes);
        millis -= TimeUnit.MINUTES.toMillis(minutes);
        seconds = (int) TimeUnit.MILLISECONDS.toSeconds(millis);
        params.add(seconds);

    }
}
