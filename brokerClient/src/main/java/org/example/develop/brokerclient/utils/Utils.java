package org.example.develop.brokerclient.utils;

public class Utils
{
    public static String generateMsg (int maxChars)
    {
        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.";
        int minChars = maxChars * 99 / 100;
        int numChars = minChars + (int) (Math.random() * (1 + maxChars - minChars));

        StringBuilder sb = new StringBuilder();
        for (int n = 0; n < numChars; n++)
        {
            sb.append(chars.charAt((int) (Math.random() * chars.length())));
        }
        return sb.toString();
    }

    public static String getUser (String credentials)
    {
        return credentials.split(":")[0];
    }

    public static String getPassword (String credentials)
    {
        return credentials.split(":")[1];
    }
}
