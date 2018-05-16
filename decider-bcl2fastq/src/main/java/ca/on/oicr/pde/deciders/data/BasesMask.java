package ca.on.oicr.pde.deciders.data;

import java.util.Objects;
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

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 83 * hash + Objects.hashCode(this.readOneIncludeLength);
        hash = 83 * hash + Objects.hashCode(this.readOneIgnoreLength);
        hash = 83 * hash + Objects.hashCode(this.indexOneIncludeLength);
        hash = 83 * hash + Objects.hashCode(this.indexOneIgnoreLength);
        hash = 83 * hash + Objects.hashCode(this.indexTwoIncludeLength);
        hash = 83 * hash + Objects.hashCode(this.indexTwoIgnoreLength);
        hash = 83 * hash + Objects.hashCode(this.readTwoIncludeLength);
        hash = 83 * hash + Objects.hashCode(this.readTwoIgnoreLength);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final BasesMask other = (BasesMask) obj;
        if (!Objects.equals(this.readOneIncludeLength, other.readOneIncludeLength)) {
            return false;
        }
        if (!Objects.equals(this.readOneIgnoreLength, other.readOneIgnoreLength)) {
            return false;
        }
        if (!Objects.equals(this.indexOneIncludeLength, other.indexOneIncludeLength)) {
            return false;
        }
        if (!Objects.equals(this.indexOneIgnoreLength, other.indexOneIgnoreLength)) {
            return false;
        }
        if (!Objects.equals(this.indexTwoIncludeLength, other.indexTwoIncludeLength)) {
            return false;
        }
        if (!Objects.equals(this.indexTwoIgnoreLength, other.indexTwoIgnoreLength)) {
            return false;
        }
        if (!Objects.equals(this.readTwoIncludeLength, other.readTwoIncludeLength)) {
            return false;
        }
        if (!Objects.equals(this.readTwoIgnoreLength, other.readTwoIgnoreLength)) {
            return false;
        }
        return true;
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

        p = Pattern.compile("y(\\*|\\d+),i(\\*|\\d+),n(\\*|\\d+),y(\\*|\\d+)", Pattern.CASE_INSENSITIVE);
        if (p.matcher(basesMaskString).matches()) {
            Matcher m = p.matcher(basesMaskString);
            m.find();
            return new BasesMask(m.group(1), null, m.group(2), null, null, m.group(3), m.group(4), null);
        }

        p = Pattern.compile("y(\\*|\\d+),n(\\*|\\d+),i(\\*|\\d+),y(\\*|\\d+)", Pattern.CASE_INSENSITIVE);
        if (p.matcher(basesMaskString).matches()) {
            Matcher m = p.matcher(basesMaskString);
            m.find();
            return new BasesMask(m.group(1), null, null, m.group(2), m.group(3), null, m.group(4), null);
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

    public static class BasesMaskBuilder {

        private Integer readOneIncludeLength = Integer.MAX_VALUE;
        private Integer readOneIgnoreLength = null;
        private Integer indexOneIncludeLength = Integer.MAX_VALUE;
        private Integer indexOneIgnoreLength = Integer.MAX_VALUE;
        private Integer indexTwoIncludeLength = null;
        private Integer indexTwoIgnoreLength = null;
        private Integer readTwoIncludeLength = Integer.MAX_VALUE;
        private Integer readTwoIgnoreLength = null;

        public BasesMaskBuilder() {
        }

        @Override
        public String toString() {
            return "BasesMaskBuilder{" + "readOneIncludeLength=" + readOneIncludeLength + ", readOneIgnoreLength=" + readOneIgnoreLength + ", indexOneIncludeLength=" + indexOneIncludeLength + ", indexOneIgnoreLength=" + indexOneIgnoreLength + ", indexTwoIncludeLength=" + indexTwoIncludeLength + ", indexTwoIgnoreLength=" + indexTwoIgnoreLength + ", readTwoIncludeLength=" + readTwoIncludeLength + ", readTwoIgnoreLength=" + readTwoIgnoreLength + '}';
        }

        public BasesMaskBuilder setReadOneIncludeLength(Integer readOneIncludeLength) {
            this.readOneIncludeLength = readOneIncludeLength;
            return this;
        }

        public BasesMaskBuilder setReadOneIgnoreLength(Integer readOneIgnoreLength) {
            this.readOneIgnoreLength = readOneIgnoreLength;
            return this;
        }

        public BasesMaskBuilder setIndexOneIncludeLength(Integer indexOneIncludeLength) {
            this.indexOneIncludeLength = indexOneIncludeLength;
            return this;
        }

        public BasesMaskBuilder setIndexOneIgnoreLength(Integer indexOneIgnoreLength) {
            this.indexOneIgnoreLength = indexOneIgnoreLength;
            return this;
        }

        public BasesMaskBuilder setIndexTwoIncludeLength(Integer indexTwoIncludeLength) {
            this.indexTwoIncludeLength = indexTwoIncludeLength;
            return this;
        }

        public BasesMaskBuilder setIndexTwoIgnoreLength(Integer indexTwoIgnoreLength) {
            this.indexTwoIgnoreLength = indexTwoIgnoreLength;
            return this;
        }

        public BasesMaskBuilder setReadTwoIncludeLength(Integer readTwoIncludeLength) {
            this.readTwoIncludeLength = readTwoIncludeLength;
            return this;
        }

        public BasesMaskBuilder setReadTwoIgnoreLength(Integer readTwoIgnoreLength) {
            this.readTwoIgnoreLength = readTwoIgnoreLength;
            return this;
        }

        public BasesMask createBasesMask() {
            return new BasesMask(readOneIncludeLength, readOneIgnoreLength, indexOneIncludeLength, indexOneIgnoreLength, indexTwoIncludeLength, indexTwoIgnoreLength, readTwoIncludeLength, readTwoIgnoreLength);
        }

    }

}
