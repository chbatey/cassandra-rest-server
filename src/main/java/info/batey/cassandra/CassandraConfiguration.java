package info.batey.cassandra;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@ConfigurationProperties(prefix = "cassandra")
public class CassandraConfiguration {
    private String[] contactPoints;

    public String[] getContactPoints() {
        return contactPoints;
    }

    public void setContactPoints(String[] contactPoints) {
        this.contactPoints = contactPoints;
    }
}
