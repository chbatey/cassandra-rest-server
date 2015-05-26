package info.batey.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.WebRequest;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.StreamSupport.stream;

@RestController
public class RestResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(RestResource.class);

    @Autowired
    private CassandraConfiguration cassandraConfiguration;

    private Cluster cluster;
    private Session session;

    @PostConstruct
    public void cassandra() {
        cluster = Cluster.builder().addContactPoints(cassandraConfiguration.getContactPoints()).build();
        session = cluster.connect();
    }

    // would look nicer with the QueryBuilder API but not supported yet
    @RequestMapping(value = "/{keyspace}/{table}", produces = "application/json")
    public String getTable(@PathVariable String keyspace, @PathVariable String table, WebRequest webRequest) {
        Set<String> parameterNames = webRequest.getParameterMap().keySet();
        StringBuilder sb = new StringBuilder("select JSON * from ").append(keyspace).append(".").append(table);
        if (!parameterNames.isEmpty()) sb.append(" where ");

        List<String> whereClause = stream(parameterNames.spliterator(), false)
                .map(paramName -> paramName + " = ?")
                .collect(Collectors.toList());
        String[] paramValues = stream(parameterNames.spliterator(), false).map(webRequest::getParameter).toArray(String[]::new);

        String query = sb.toString() + String.join(" AND ", whereClause);
        LOGGER.info("Executing Query {} with parameters {}", query, paramValues);
        List<String> all = session.execute(query, paramValues).all()
                .stream().map(row -> row.getString("[json]"))
                .collect(Collectors.toList());
        return "[" +String.join(",", all) + "]";
    }
}
