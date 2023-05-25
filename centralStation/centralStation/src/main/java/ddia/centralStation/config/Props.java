package ddia.centralStation.config;

import java.util.Optional;

public class Props {
    public static final String DATA_PATH = Optional.ofNullable(System.getenv("data-path")).orElse("data");
}
