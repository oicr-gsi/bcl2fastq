package ca.on.oicr.pde.deciders.data;

import java.text.MessageFormat;

/**
 *
 * @author mlaszloffy
 */
public class ValidationResult {

    private final Boolean isValid;
    private final String reason;

    private ValidationResult(boolean isValid, String reason) {
        this.isValid = isValid;
        this.reason = reason;
    }

    public boolean isValid() {
        return this.isValid;
    }

    public String getReason() {
        return this.reason;
    }

    public static ValidationResult invalid(String reason) {
        return new ValidationResult(false, reason);
    }

    public static ValidationResult invalid(String message, String... args) {
        return new ValidationResult(false, MessageFormat.format(message, (Object[]) args));
    }

    public static ValidationResult valid(String reason) {
        return new ValidationResult(true, reason);
    }

    public static ValidationResult valid(String message, String... args) {
        return new ValidationResult(true, MessageFormat.format(message, (Object[]) args));
    }

    public static ValidationResult valid() {
        return new ValidationResult(true, "");
    }

}
