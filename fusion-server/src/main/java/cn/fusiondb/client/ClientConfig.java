package cn.fusiondb.client;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import io.airlift.units.Duration;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

import static com.facebook.presto.client.KerberosUtil.defaultCredentialCachePath;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.nullToEmpty;
import static java.util.Collections.emptyMap;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * Created by xiliu on 2020/12/9
 */
public class ClientConfig {
    private static final Splitter NAME_VALUE_SPLITTER = Splitter.on('=').limit(2);
    private static final CharMatcher PRINTABLE_ASCII = CharMatcher.inRange((char) 0x21, (char) 0x7E); // spaces are not allowed

    public String server = "localhost:8080";

    public String krb5RemoteServiceName;

    public String krb5ConfigPath = "/etc/krb5.conf";

    public String krb5KeytabPath = "/etc/krb5.keytab";

    public String krb5CredentialCachePath = defaultCredentialCachePath().orElse(null);

    public String krb5Principal;

    public boolean krb5DisableRemoteServiceHostnameCanonicalization;

    public String keystorePath;

    public String keystorePassword;

    public String truststorePath;

    public String truststorePassword;

    public String accessToken;

    public String user = System.getProperty("user.name");

    public boolean password;

    public String source = "mysql-client@5.7";

    public String clientInfo;

    public String clientTags = "";

    public String catalog;

    public String schema;

    public String file;

    public boolean debug;

    public String logLevelsFile;

    public String execute;

    public ClientConfig.OutputFormat outputFormat = OutputFormat.MYSQL;

    public final List<ClientConfig.ClientResourceEstimate> resourceEstimates = new ArrayList<>();

    public final List<ClientConfig.ClientSessionProperty> sessionProperties = new ArrayList<>();

    public final List<ClientConfig.ClientExtraCredential> extraCredentials = new ArrayList<>();

    public HostAndPort socksProxy;

    public HostAndPort httpProxy;

    public Duration clientRequestTimeout = new Duration(2, MINUTES);

    public boolean ignoreErrors;

    public enum OutputFormat
    {
        MYSQL,
        ALIGNED,
        VERTICAL,
        CSV,
        TSV,
        CSV_HEADER,
        TSV_HEADER,
        NULL
    }

    public ClientSession toClientSession(String server, String user, String catalog, String  schema)
    {
        return new ClientSession(
                parseServer(server),
                user,
                source,
                Optional.empty(),
                parseClientTags(clientTags),
                clientInfo,
                catalog,
                schema,
                TimeZone.getDefault().getID(),
                Locale.getDefault(),
                toResourceEstimates(resourceEstimates),
                toProperties(sessionProperties),
                emptyMap(),
                emptyMap(),
                toExtraCredentials(extraCredentials),
                null,
                clientRequestTimeout);
    }

    public ClientSession toClientSession(String catalog, String  schema)
    {
        return new ClientSession(
                parseServer(server),
                user,
                source,
                Optional.empty(),
                parseClientTags(clientTags),
                clientInfo,
                catalog,
                schema,
                TimeZone.getDefault().getID(),
                Locale.getDefault(),
                toResourceEstimates(resourceEstimates),
                toProperties(sessionProperties),
                emptyMap(),
                emptyMap(),
                toExtraCredentials(extraCredentials),
                null,
                clientRequestTimeout);
    }

    public static URI parseServer(String server)
    {
        server = server.toLowerCase(ENGLISH);
        if (server.startsWith("http://") || server.startsWith("https://")) {
            return URI.create(server);
        }

        HostAndPort host = HostAndPort.fromString(server);
        try {
            return new URI("http", null, host.getHost(), host.getPortOrDefault(80), null, null, null);
        }
        catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static Set<String> parseClientTags(String clientTagsString)
    {
        Splitter splitter = Splitter.on(',').trimResults().omitEmptyStrings();
        return ImmutableSet.copyOf(splitter.split(nullToEmpty(clientTagsString)));
    }

    public static Map<String, String> toProperties(List<ClientConfig.ClientSessionProperty> sessionProperties)
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        for (ClientConfig.ClientSessionProperty sessionProperty : sessionProperties) {
            String name = sessionProperty.getName();
            if (sessionProperty.getCatalog().isPresent()) {
                name = sessionProperty.getCatalog().get() + "." + name;
            }
            builder.put(name, sessionProperty.getValue());
        }
        return builder.build();
    }

    public static Map<String, String> toResourceEstimates(List<ClientConfig.ClientResourceEstimate> estimates)
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        for (ClientConfig.ClientResourceEstimate estimate : estimates) {
            builder.put(estimate.getResource(), estimate.getEstimate());
        }
        return builder.build();
    }

    public static Map<String, String> toExtraCredentials(List<ClientConfig.ClientExtraCredential> extraCredentials)
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        for (ClientConfig.ClientExtraCredential credential : extraCredentials) {
            builder.put(credential.getName(), credential.getValue());
        }
        return builder.build();
    }

    public static final class ClientResourceEstimate
    {
        private final String resource;
        private final String estimate;

        public ClientResourceEstimate(String resourceEstimate)
        {
            List<String> nameValue = NAME_VALUE_SPLITTER.splitToList(resourceEstimate);
            checkArgument(nameValue.size() == 2, "Resource estimate: %s", resourceEstimate);

            this.resource = nameValue.get(0);
            this.estimate = nameValue.get(1);
            checkArgument(!resource.isEmpty(), "Resource name is empty");
            checkArgument(!estimate.isEmpty(), "Resource estimate is empty");
            checkArgument(PRINTABLE_ASCII.matchesAllOf(resource), "Resource contains spaces or is not US_ASCII: %s", resource);
            checkArgument(resource.indexOf('=') < 0, "Resource must not contain '=': %s", resource);
            checkArgument(PRINTABLE_ASCII.matchesAllOf(estimate), "Resource estimate contains spaces or is not US_ASCII: %s", resource);
        }

        @VisibleForTesting
        public ClientResourceEstimate(String resource, String estimate)
        {
            this.resource = requireNonNull(resource, "resource is null");
            this.estimate = estimate;
        }

        public String getResource()
        {
            return resource;
        }

        public String getEstimate()
        {
            return estimate;
        }

        @Override
        public String toString()
        {
            return resource + '=' + estimate;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ClientConfig.ClientResourceEstimate other = (ClientConfig.ClientResourceEstimate) o;
            return Objects.equals(resource, other.resource) &&
                    Objects.equals(estimate, other.estimate);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(resource, estimate);
        }
    }

    public static final class ClientSessionProperty
    {
        private static final Splitter NAME_SPLITTER = Splitter.on('.');
        private final Optional<String> catalog;
        private final String name;
        private final String value;

        public ClientSessionProperty(String property)
        {
            List<String> nameValue = NAME_VALUE_SPLITTER.splitToList(property);
            checkArgument(nameValue.size() == 2, "Session property: %s", property);

            List<String> nameParts = NAME_SPLITTER.splitToList(nameValue.get(0));
            checkArgument(nameParts.size() == 1 || nameParts.size() == 2, "Invalid session property: %s", property);
            if (nameParts.size() == 1) {
                catalog = Optional.empty();
                name = nameParts.get(0);
            }
            else {
                catalog = Optional.of(nameParts.get(0));
                name = nameParts.get(1);
            }
            value = nameValue.get(1);

            verifyProperty(catalog, name, value);
        }

        public ClientSessionProperty(Optional<String> catalog, String name, String value)
        {
            this.catalog = requireNonNull(catalog, "catalog is null");
            this.name = requireNonNull(name, "name is null");
            this.value = requireNonNull(value, "value is null");

            verifyProperty(catalog, name, value);
        }

        private static void verifyProperty(Optional<String> catalog, String name, String value)
        {
            checkArgument(!catalog.isPresent() || !catalog.get().isEmpty(), "Invalid session property: %s.%s:%s", catalog, name, value);
            checkArgument(!name.isEmpty(), "Session property name is empty");
            checkArgument(catalog.orElse("").indexOf('=') < 0, "Session property catalog must not contain '=': %s", name);
            checkArgument(PRINTABLE_ASCII.matchesAllOf(catalog.orElse("")), "Session property catalog contains spaces or is not US_ASCII: %s", name);
            checkArgument(name.indexOf('=') < 0, "Session property name must not contain '=': %s", name);
            checkArgument(PRINTABLE_ASCII.matchesAllOf(name), "Session property name contains spaces or is not US_ASCII: %s", name);
            checkArgument(PRINTABLE_ASCII.matchesAllOf(value), "Session property value contains spaces or is not US_ASCII: %s", value);
        }

        public Optional<String> getCatalog()
        {
            return catalog;
        }

        public String getName()
        {
            return name;
        }

        public String getValue()
        {
            return value;
        }

        @Override
        public String toString()
        {
            return (catalog.isPresent() ? catalog.get() + '.' : "") + name + '=' + value;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(catalog, name, value);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            ClientConfig.ClientSessionProperty other = (ClientConfig.ClientSessionProperty) obj;
            return Objects.equals(this.catalog, other.catalog) &&
                    Objects.equals(this.name, other.name) &&
                    Objects.equals(this.value, other.value);
        }
    }

    public static final class ClientExtraCredential
    {
        private final String name;
        private final String value;

        public ClientExtraCredential(String extraCredential)
        {
            List<String> nameValue = NAME_VALUE_SPLITTER.splitToList(extraCredential);
            checkArgument(nameValue.size() == 2, "Extra credential: %s", extraCredential);

            this.name = nameValue.get(0);
            this.value = nameValue.get(1);
            checkArgument(!name.isEmpty(), "Credential name is empty");
            checkArgument(!value.isEmpty(), "Credential value is empty");
            checkArgument(PRINTABLE_ASCII.matchesAllOf(name), "Credential name contains spaces or is not US_ASCII: %s", name);
            checkArgument(name.indexOf('=') < 0, "Credential name must not contain '=': %s", name);
            checkArgument(PRINTABLE_ASCII.matchesAllOf(value), "Credential value contains space or is not US_ASCII: %s", name);
        }

        public ClientExtraCredential(String name, String value)
        {
            this.name = requireNonNull(name, "name is null");
            this.value = value;
        }

        public String getName()
        {
            return name;
        }

        public String getValue()
        {
            return value;
        }

        @Override
        public String toString()
        {
            return name + '=' + value;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ClientConfig.ClientExtraCredential other = (ClientConfig.ClientExtraCredential) o;
            return Objects.equals(name, other.name) && Objects.equals(value, other.value);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(name, value);
        }
    }
}
