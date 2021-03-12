package org.bonitasoft.migrate.tool;


public class Toolbox {
    /**
     * @param time
     * @return
     */
    public static String getHumanTime(long time) {
      long hour = time / 1000 / 60 / 60;
      time -= hour * 1000 * 60 * 60;

      long minute = time / 1000 / 60;
      time -= minute * 1000 * 60;

      long second = time / 1000;
      time -= second * 1000;
      // don't display second and ms if we are more than 1 hour
      return (hour > 0 ? hour + " h " : "") + minute + " mn " + (hour < 1 ? second + " s " + time + " ms" : "");

    }
}
