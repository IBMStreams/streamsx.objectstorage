/*******************************************************************************
* Copyright (C) 2017,2020, International Business Machines Corporation
* All Rights Reserved
*******************************************************************************/

package com.ibm.streamsx.objectstorage;

import java.text.MessageFormat;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

/**
 * fetch translated strings from the correct properties file
 * according to the current local
 */
public class Messages
{
    private final static Map<String, MessageFormat> formatCache = Collections.synchronizedMap(new HashMap<>());
	
    private static final String BUNDLE_NAME = "com.ibm.streamsx.objectstorage.messages.messages"; 

    // load the bundle based on the current locale
    private static final ResourceBundle RESOURCE_BUNDLE = ResourceBundle.getBundle(BUNDLE_NAME);
    private static final ResourceBundle FALLBACK_RESOURCE_BUNDLE = ResourceBundle.getBundle (BUNDLE_NAME, new Locale ("en", "US"));

    private Messages()
    {
    }

    /**
     * Returns an unformatted message string from the resource bundle or the fallback locale (en_US)
     * when the key is not available in the system default locale.
     * @param key the key of the message string
     * @return the unformatted message
     */
    public static String getString(String key) {
        try {
            return getRawMsg (key);
        } catch (MissingResourceException e) {
            return '!' + key + '!';
        }
    }

    /**
     * Returns a formatted message string from the resource bundle or the fallback locale (en_US)
     * when the key is not available in the system default locale.
     * @param key the key of the message string
     * @param args the arguments for formatting the message pattern
     * @return the message formatted with the arguments
     */
    public static String getString (String key, Object... args) {
        try {
            String msg = getRawMsg (key);
            if (args == null) return msg;
            return format (msg, args);
        } catch (MissingResourceException e) {
            return '!' + key + '!';
        }
    }



    private static String getRawMsg (String key) throws MissingResourceException {
        try {
            return RESOURCE_BUNDLE.getString(key);
        } catch (MissingResourceException e) {
            return FALLBACK_RESOURCE_BUNDLE.getString(key);
        }
    }

    /**
     * Formats a pattern with parameters without throwing an exception when the objects
     * cannot be formatted for the given format string.
     * 
     * Exceptions are printed to stderr with stack trace.
     *
     * @param pattern   the pattern
     * @param arguments the parameters
     * @return the formatted String
     */
    public static String format (String pattern, Object... arguments) {
        MessageFormat fmt = formatCache.get (pattern);
        if (fmt == null) {
            fmt = new MessageFormat (pattern);
            formatCache.put (pattern, fmt);
        }
        try {
            return fmt.format (arguments, new StringBuffer(), null).toString();
        } catch (Exception e) {
            e.printStackTrace();
            return "Failed to format trace message. See stdouterr";
        }
    }

    /**
     * Prints out all messages with generic parameters PARAM_0 ... PARAM_9
     * If you want to run this main with a specific locale, launch it with VM arguments
     * -Duser.language=ab -Duser.country=CD
     * 
     * @param args
     */
    public static void main (String[] args) {
        Enumeration<String> keyEnum = RESOURCE_BUNDLE.getKeys();
        Map<String, String> messageMap = new HashMap<>();
        int maxKeyLen = 0;
        Object[] parameters = new Object[] {"PARAM_0", "PARAM_1", "PARAM_2", "PARAM_3", "PARAM_4", "PARAM_5", "PARAM_6", "PARAM_7", "PARAM_8", "PARAM_9"}; 
        while (keyEnum.hasMoreElements()) {
            String key = keyEnum.nextElement();
            if (key.length() > maxKeyLen) maxKeyLen = key.length();
            String message = getString (key, parameters);
            messageMap.put (key, message);
        }
        // sort according message (CDIST numbers)
        List<Entry<String, String>> keysMessages = new LinkedList<>(messageMap.entrySet());
        Collections.sort (keysMessages, new Comparator<Entry<String, String>>() {
            @Override
            public int compare (Entry<String, String> o1, Entry<String, String> o2) {
                return o1.getValue().compareTo(o2.getValue());
            }
        });
        int n = 0;
        for (Entry <String, String> msg: keysMessages) {
            String k = msg.getKey();
            while (k.length() < maxKeyLen) k += ' ';
            String ns = "" + ++n;
            while (ns.length() < 3) ns += ' ';
            System.out.println (format("{0} {1} {2}", ns, k, msg.getValue()));
        }
    }
}
