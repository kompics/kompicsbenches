package se.kth.benchmarks.kompicsjava.bench.chameneos;

public enum ChameneosColour {
    RED, YELLOW, BLUE, FADED;

    public static ChameneosColour forId(int i) {
        switch (i % 3) {
        case 0:
            return RED;
        case 1:
            return YELLOW;
        case 2:
            return BLUE;
        default:
            throw new RuntimeException("Unreachable!");
        }
    }

    public ChameneosColour complement(ChameneosColour other) {
        switch (this) {
        case RED:
            switch (other) {
            case RED:
                return RED;
            case YELLOW:
                return BLUE;
            case BLUE:
                return YELLOW;
            case FADED:
                return FADED;
            }
            break;
        case YELLOW:
            switch (other) {
            case RED:
                return BLUE;
            case YELLOW:
                return YELLOW;
            case BLUE:
                return RED;
            case FADED:
                return FADED;
            }
            break;
        case BLUE:
            switch (other) {
            case RED:
                return YELLOW;
            case YELLOW:
                return RED;
            case BLUE:
                return BLUE;
            case FADED:
                return FADED;
            }
            break;
        case FADED:
            return FADED;

        }
        throw new IllegalArgumentException("Unknown color: " + this);
    }
}
