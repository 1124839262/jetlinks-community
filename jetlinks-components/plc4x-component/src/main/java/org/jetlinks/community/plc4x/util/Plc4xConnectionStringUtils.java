package org.jetlinks.community.plc4x.util;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Utilities for PLC4X connection strings.
 */
public final class Plc4xConnectionStringUtils {

    private static final String USERNAME_PARAM = "username";
    private static final String PASSWORD_PARAM = "password";

    private Plc4xConnectionStringUtils() {
    }

    public static String withAuthentication(String baseUrl, String username, String password) {
        if (isBlank(username)) {
            return baseUrl;
        }
        if (baseUrl == null) {
            return null;
        }

        String cleanUrl = removeAuthenticationParameters(baseUrl);
        String separator = cleanUrl.contains("?") ? "&" : "?";
        StringBuilder result = new StringBuilder(cleanUrl)
                .append(separator)
                .append(USERNAME_PARAM)
                .append("=")
                .append(encodeQueryValue(username.trim()));

        if (!isBlank(password)) {
            result.append("&")
                  .append(PASSWORD_PARAM)
                  .append("=")
                  .append(encodeQueryValue(password.trim()));
        }

        return result.toString();
    }

    private static String removeAuthenticationParameters(String baseUrl) {
        int queryIndex = baseUrl.indexOf('?');
        if (queryIndex < 0) {
            return baseUrl;
        }

        String prefix = baseUrl.substring(0, queryIndex);
        String query = baseUrl.substring(queryIndex + 1);
        List<String> retainedParameters = new ArrayList<>();

        for (String parameter : query.split("&", -1)) {
            if (parameter.isEmpty() || isAuthenticationParameter(parameter)) {
                continue;
            }
            retainedParameters.add(parameter);
        }

        if (retainedParameters.isEmpty()) {
            return prefix;
        }
        return prefix + "?" + String.join("&", retainedParameters);
    }

    private static boolean isAuthenticationParameter(String parameter) {
        int equalsIndex = parameter.indexOf('=');
        String key = equalsIndex < 0 ? parameter : parameter.substring(0, equalsIndex);
        String normalizedKey = key.toLowerCase(Locale.ROOT);
        return USERNAME_PARAM.equals(normalizedKey) || PASSWORD_PARAM.equals(normalizedKey);
    }

    private static String encodeQueryValue(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
