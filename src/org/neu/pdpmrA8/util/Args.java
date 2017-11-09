package org.neu.pdpmrA8.util;

import java.util.Arrays;

/**
 * Simple command line argument processing
 *
 * @author Jan Vitek
 */
public class Args {
    String[] args;

    public Args(String[] args) {
        this.args = args;
    }

    public boolean getFlag(String flag) {
        boolean found = false;
        for (int i = 0; i < args.length; i++)
            if (found = args[i].equals(flag)) {
                args[i] = args[0];
                args = Arrays.copyOfRange(args, 1, args.length);
                break;
            }
        return found;
    }

    public String getOption(String opt) {
        opt += "=";
        for (int i = 0; i < args.length; i++)
            if (args[i].startsWith(opt)) {
                String value = args[i].replaceAll(opt, "");
                args[i] = args[0];
                args = Arrays.copyOfRange(args, 1, args.length);
                return value;
            }
        return null;
    }

    int length() {
        return args.length;
    }

    String[] get() {
        return args;
    }

    @Override
    public String toString() {
        String res = "";
        for (String s : args)
            res += s + " ";
        return res;
    }
}
