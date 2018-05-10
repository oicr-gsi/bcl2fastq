package ca.on.oicr.pde.deciders.data;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang.StringUtils;

/**
 *
 * @author mlaszloffy
 */
public class BasesMask {

    private final Integer readOneIncludeLength;
    private final Integer readOneIgnoreLength;

    private final Integer indexOneIncludeLength;
    private final Integer indexOneIgnoreLength;

    private final Integer indexTwoIncludeLength;
    private final Integer indexTwoIgnoreLength;

    private final Integer readTwoIncludeLength;
    private final Integer readTwoIgnoreLength;

    public BasesMask(Integer readOneIncludeLength, Integer readOneIgnoreLength, Integer indexOneIncludeLength, Integer indexOneIgnoreLength, Integer indexTwoIncludeLength, Integer indexTwoIgnoreLength, Integer readTwoIncludeLength, Integer readTwoIgnoreLength) {
        this.readOneIncludeLength = readOneIncludeLength;
        this.readOneIgnoreLength = readOneIgnoreLength;
        this.indexOneIncludeLength = indexOneIncludeLength;
        this.indexOneIgnoreLength = indexOneIgnoreLength;
        this.indexTwoIncludeLength = indexTwoIncludeLength;
        this.indexTwoIgnoreLength = indexTwoIgnoreLength;
        this.readTwoIncludeLength = readTwoIncludeLength;
        this.readTwoIgnoreLength = readTwoIgnoreLength;
    }

    public BasesMask(String readOneIncludeLength, String readOneIgnoreLength, String indexOneIncludeLength, String indexOneIgnoreLength, String indexTwoIncludeLength, String indexTwoIgnoreLength, String readTwoIncludeLength, String readTwoIgnoreLength) {
        this.readOneIncludeLength = getInt(readOneIncludeLength);
        this.readOneIgnoreLength = getInt(readOneIgnoreLength);
        this.indexOneIncludeLength = getInt(indexOneIncludeLength);
        this.indexOneIgnoreLength = getInt(indexOneIgnoreLength);
        this.indexTwoIncludeLength = getInt(indexTwoIncludeLength);
        this.indexTwoIgnoreLength = getInt(indexTwoIgnoreLength);
        this.readTwoIncludeLength = getInt(readTwoIncludeLength);
        this.readTwoIgnoreLength = getInt(readTwoIgnoreLength);
    }

    public Integer getReadOneIncludeLength() {
        return readOneIncludeLength;
    }

    public Integer getReadOneIgnoreLength() {
        return readOneIgnoreLength;
    }

    public Integer getIndexOneIncludeLength() {
        return indexOneIncludeLength;
    }

    public Integer getIndexOneIgnoreLength() {
        return indexOneIgnoreLength;
    }

    public Integer getIndexTwoIncludeLength() {
        return indexTwoIncludeLength;
    }

    public Integer getIndexTwoIgnoreLength() {
        return indexTwoIgnoreLength;
    }

    public Integer getReadTwoIncludeLength() {
        return readTwoIncludeLength;
    }

    public Integer getReadTwoIgnoreLength() {
        return readTwoIgnoreLength;
    }

    private String getVal(String prefix, Integer length) {
        if (length != null && length != Integer.MAX_VALUE) {
            return prefix + length.toString();
        } else if (length != null && length == Integer.MAX_VALUE) {
            return prefix + "*";
        } else if (length != null && length == 0) {
            return "";
        } else {
            return "";
        }
    }

    private Integer getInt(String length) {
        if (length == null) {
            return null;
        } else if ("*".equals(length)) {
            return Integer.MAX_VALUE;
        } else if (StringUtils.isNumeric(length)) {
            Integer i = Integer.parseInt(length);
            if (i == 0) {
                return null;
            } else {
                return Integer.parseInt(length);
            }
        } else {
            throw new IllegalArgumentException("Input value [" + length + "] is not supported");
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getVal("y", readOneIncludeLength));
        sb.append(getVal("n", readOneIgnoreLength));
        sb.append(",");
        sb.append(getVal("i", indexOneIncludeLength));
        sb.append(getVal("n", indexOneIgnoreLength));
        sb.append(",");
        if (indexTwoIncludeLength != null || indexTwoIgnoreLength != null) {
            sb.append(getVal("i", indexTwoIncludeLength));
            sb.append(getVal("n", indexTwoIgnoreLength));
            sb.append(",");
        }
        sb.append(getVal("y", readTwoIncludeLength));
        sb.append(getVal("n", readTwoIgnoreLength));
        return sb.toString();
    }

    public static BasesMask fromString(String basesMaskString) {
        Pattern p;

        //dual barcode patterns
        p = Pattern.compile("y(\\*|\\d+)n(\\*|\\d+),i(\\*|\\d+)n(\\*|\\d+),i(\\*|\\d+)n(\\*|\\d+),y(\\*|\\d+)n(\\*|\\d+)", Pattern.CASE_INSENSITIVE);
        if (p.matcher(basesMaskString).matches()) {
            Matcher m = p.matcher(basesMaskString);
            m.find();
            return new BasesMask(m.group(1), m.group(2), m.group(3), m.group(4), m.group(5), m.group(6), m.group(7), m.group(8));
        }

        p = Pattern.compile("y(\\*|\\d+),i(\\*|\\d+)n(\\*|\\d+),i(\\*|\\d+)n(\\*|\\d+),y(\\*|\\d+)", Pattern.CASE_INSENSITIVE);
        if (p.matcher(basesMaskString).matches()) {
            Matcher m = p.matcher(basesMaskString);
            m.find();
            return new BasesMask(m.group(1), null, m.group(2), m.group(3), m.group(4), m.group(5), m.group(6), null);
        }

        p = Pattern.compile("y(\\*|\\d+),i(\\*|\\d+),i(\\*|\\d+),y(\\*|\\d+)", Pattern.CASE_INSENSITIVE);
        if (p.matcher(basesMaskString).matches()) {
            Matcher m = p.matcher(basesMaskString);
            m.find();
            return new BasesMask(m.group(1), null, m.group(2), null, m.group(3), null, m.group(4), null);
        }

        //single barcode patterns
        p = Pattern.compile("y(\\*|\\d+)n(\\*|\\d+),i(\\*|\\d+)n(\\*|\\d+),y(\\*|\\d+)n(\\*|\\d+)", Pattern.CASE_INSENSITIVE);
        if (p.matcher(basesMaskString).matches()) {
            Matcher m = p.matcher(basesMaskString);
            m.find();
            return new BasesMask(m.group(1), m.group(2), m.group(3), m.group(4), null, null, m.group(5), m.group(6));
        }

        p = Pattern.compile("y(\\*|\\d+),i(\\*|\\d+)n(\\*|\\d+),y(\\*|\\d+)", Pattern.CASE_INSENSITIVE);
        if (p.matcher(basesMaskString).matches()) {
            Matcher m = p.matcher(basesMaskString);
            m.find();
            return new BasesMask(m.group(1), null, m.group(2), m.group(3), null, null, m.group(4), null);
        }

        p = Pattern.compile("y(\\*|\\d+),i(\\*|\\d+),y(\\*|\\d+)", Pattern.CASE_INSENSITIVE);
        if (p.matcher(basesMaskString).matches()) {
            Matcher m = p.matcher(basesMaskString);
            m.find();
            return new BasesMask(m.group(1), null, m.group(2), null, null, null, m.group(3), null);
        }

        throw new IllegalArgumentException("Unsupported bases mask string [" + basesMaskString + "]");
    }

}
